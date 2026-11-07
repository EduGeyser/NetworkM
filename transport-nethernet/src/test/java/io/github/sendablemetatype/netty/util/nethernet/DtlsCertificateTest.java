package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.sec.ECPrivateKey;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The one DTLS certificate an endpoint presents: created once, or supplied and checked as upstream checks it. */
class DtlsCertificateTest {

    private static X509Bundle bundle(CertificateBuilder.Algorithm algorithm) throws Exception {
        return new CertificateBuilder().subject("CN=NetherNet Host").algorithm(algorithm)
                .setIsCertificateAuthority(true).buildSelfSigned();
    }

    private static X509Certificate parse(String pem) throws Exception {
        return (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void publishesTheSha256FingerprintOfASuppliedCertificate() throws Exception {
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp256);
        DtlsCertificate certificate = DtlsCertificate.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM());

        byte[] der = parse(bundle.getCertificatePEM()).getEncoded();
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(der), certificate.sha256());
        assertEquals(bundle.getCertificatePEM(), certificate.dtls().certificatePem());
        assertFalse(certificate.toString().contains("PRIVATE"));
    }

    @Test
    void takesRsaSignedWithSha256Too() throws Exception {
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.rsa2048);
        assertEquals(32, DtlsCertificate.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM()).sha256().length);
    }

    @Test
    void refusesACertificateSignedWithAnotherDigest() throws Exception {
        // libwebrtc would announce a sha-384 fingerprint for it, which no client built from the profile expects
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp384);
        GeneralSecurityException refused = assertThrows(GeneralSecurityException.class,
                () -> DtlsCertificate.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM()));
        assertTrue(refused.getMessage().contains("SHA-256"), refused.getMessage());
    }

    @Test
    void refusesAKeyThatIsNotTheCertificates() throws Exception {
        X509Bundle certificate = bundle(CertificateBuilder.Algorithm.ecp256);
        X509Bundle other = bundle(CertificateBuilder.Algorithm.ecp256);
        assertThrows(GeneralSecurityException.class,
                () -> DtlsCertificate.of(certificate.getCertificatePEM(), other.getPrivateKeyPEM()));
    }

    @Test
    void refusesAKeyThatIsNotPkcs8() throws Exception {
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp256);
        String sec1 = bundle.getPrivateKeyPEM().replace("PRIVATE KEY", "EC PRIVATE KEY");
        assertThrows(GeneralSecurityException.class, () -> DtlsCertificate.of(bundle.getCertificatePEM(), sec1));
    }

    @Test
    void createsACertificateOnFirstUseAndKeepsIt(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("keys/dtls-certificate.pem");

        DtlsCertificate created = DtlsCertificate.loadOrCreate(file);
        String written = Files.readString(file);
        DtlsCertificate reloaded = DtlsCertificate.loadOrCreate(file);

        assertArrayEquals(created.sha256(), reloaded.sha256(), "a new certificate would fail every published profile");
        assertEquals(created.dtls(), reloaded.dtls());
        assertEquals(written, Files.readString(file), "an existing certificate is never rewritten");
        try (var files = Files.list(file.getParent())) {
            assertEquals(List.of(file), files.toList(), "no temporary file is left behind");
        }

        X509Certificate certificate = parse(created.dtls().certificatePem());
        assertEquals("SHA256withECDSA", certificate.getSigAlgName());
        assertEquals(256, ((ECPublicKey) certificate.getPublicKey()).getParams().getCurve().getField().getFieldSize());
        assertEquals("CN=WebRTC", certificate.getSubjectX500Principal().getName());
        assertEquals(Instant.parse("9999-12-31T23:59:59Z"), certificate.getNotAfter().toInstant());
        if (Files.getFileStore(dir).supportsFileAttributeView(PosixFileAttributeView.class)) {
            assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
        } else {
            List<AclEntry> acl = Files.getFileAttributeView(file, AclFileAttributeView.class).getAcl();
            assertEquals(1, acl.size(), () -> "only the owner may read the key: " + acl);
        }
    }

    @Test
    void theFingerprintIsTheOneLibwebrtcComputes(@TempDir Path dir) throws Exception {
        // libwebrtc hashes the certificate's DER with its signature's digest (rtc_base/ssl_fingerprint.cc)
        DtlsCertificate certificate = DtlsCertificate.loadOrCreate(dir.resolve("dtls-certificate.pem"));
        X509Certificate parsed = parse(certificate.dtls().certificatePem());

        assertEquals("SHA256withECDSA", parsed.getSigAlgName());
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(parsed.getEncoded());
        assertArrayEquals(digest, certificate.sha256());
        assertEquals("DtlsCertificate[sha-256 " + HexFormat.ofDelimiter(":").withUpperCase().formatHex(digest) + "]",
                certificate.toString());
    }

    @Test
    void concurrentCreatorsAllEndUpWithTheWinnersCertificate(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("dtls-certificate.pem");
        int racers = 8;
        CyclicBarrier start = new CyclicBarrier(racers);
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        try {
            Callable<String> racer = () -> {
                start.await();
                return HexFormat.of().formatHex(DtlsCertificate.loadOrCreate(file).sha256());
            };
            List<Future<String>> results = pool.invokeAll(IntStream.range(0, racers).mapToObj(i -> racer).toList());
            Set<String> fingerprints = results.stream().map(result -> {
                try {
                    return result.get();
                } catch (Exception e) {
                    throw new AssertionError("a racer failed instead of loading the winner's certificate", e);
                }
            }).collect(Collectors.toSet());

            assertEquals(Set.of(HexFormat.of().formatHex(DtlsCertificate.loadOrCreate(file).sha256())), fingerprints);
        } finally {
            pool.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"another-key", "damaged-key", "no-key"})
    void refusesATamperedFileAndKeepsIt(String tampering, @TempDir Path dir) throws Exception {
        Path file = dir.resolve("dtls-certificate.pem");
        DtlsCertificate.loadOrCreate(file);
        String written = Files.readString(file);
        String certificate = written.substring(0, written.indexOf("-----BEGIN PRIVATE KEY-----"));
        String key = written.substring(certificate.length());
        String tampered = switch (tampering) {
            case "another-key" -> certificate + otherKey();
            case "damaged-key" -> certificate + key.replaceFirst("(?m)^(\\S{8})\\S{8}", "$1AAAAAAAA");
            case "no-key" -> certificate;
            default -> throw new AssertionError(tampering);
        };
        Files.writeString(file, tampered);

        assertThrows(GeneralSecurityException.class, () -> DtlsCertificate.loadOrCreate(file));
        assertEquals(tampered, Files.readString(file), "a failed load keeps the operator's file");
    }

    @Test
    void keepsAPairInTwoFilesWhereUpstreamsProviderHostKeepsIt(@TempDir Path dir) throws Exception {
        Path state = dir.resolve("provider-state");
        Path certificateFile = state.resolve("host-cert.pem");
        Path keyFile = state.resolve("host-key.pem");

        DtlsCertificate created = DtlsCertificate.loadOrCreate(certificateFile, keyFile);
        String certificate = Files.readString(certificateFile);
        String key = Files.readString(keyFile);
        DtlsCertificate reloaded = DtlsCertificate.loadOrCreate(certificateFile, keyFile);

        assertArrayEquals(created.sha256(), reloaded.sha256(), "a new certificate would fail every published profile");
        assertEquals(certificate, Files.readString(certificateFile), "an existing certificate is never rewritten");
        assertEquals(key, Files.readString(keyFile), "an existing key is never rewritten");
        assertEquals(new DtlsIdentity(certificate, key), created.dtls());
        try (var files = Files.list(state)) {
            assertEquals(Set.of(certificateFile, keyFile), files.collect(Collectors.toSet()), "no temporary file is left");
        }
        // Upstream's NativeHostIdentity reads the key as one PKCS#8 block with BouncyCastle, the certificate with the JDK
        try (PEMParser parser = new PEMParser(new StringReader(key))) {
            assertInstanceOf(PrivateKeyInfo.class, parser.readObject());
            assertNull(parser.readObject());
        }
        assertEquals("SHA256withECDSA", parse(certificate).getSigAlgName());
        if (Files.getFileStore(dir).supportsFileAttributeView(PosixFileAttributeView.class)) {
            assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(keyFile)));
        } else {
            List<AclEntry> acl = Files.getFileAttributeView(keyFile, AclFileAttributeView.class).getAcl();
            assertEquals(1, acl.size(), () -> "only the owner may read the key: " + acl);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aNewKeyCarriesItsPublicPoint(boolean twoFiles, @TempDir Path dir) throws Exception {
        DtlsCertificate certificate = twoFiles
                ? DtlsCertificate.loadOrCreate(dir.resolve("host-cert.pem"), dir.resolve("host-key.pem"))
                : DtlsCertificate.loadOrCreate(dir.resolve("dtls-certificate.pem"));

        PrivateKeyInfo key;
        try (PEMParser parser = new PEMParser(new StringReader(certificate.dtls().privateKeyPem()))) {
            key = (PrivateKeyInfo) parser.readObject();
        }
        ECPrivateKey sec1 = ECPrivateKey.getInstance(key.parsePrivateKey());
        byte[] point = SubjectPublicKeyInfo.getInstance(parse(certificate.dtls().certificatePem()).getPublicKey()
                .getEncoded()).getPublicKeyData().getOctets();
        assertNotNull(sec1.getPublicKey(), "the JDK's encoding leaves the point out");
        assertArrayEquals(point, sec1.getPublicKey().getOctets());
    }

    @Test
    void loadsAPairUpstreamCreatedAsItIs(@TempDir Path dir) throws Exception {
        // What upstream's ProviderHostIdentity writes: a certificate and the JDK's PKCS#8 key, without the point
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp256);
        Path certificateFile = Files.writeString(dir.resolve("host-cert.pem"), bundle.getCertificatePEM());
        Path keyFile = Files.writeString(dir.resolve("host-key.pem"), bundle.getPrivateKeyPEM());

        DtlsCertificate loaded = DtlsCertificate.loadOrCreate(certificateFile, keyFile);

        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(parse(bundle.getCertificatePEM()).getEncoded()),
                loaded.sha256());
        assertEquals(bundle.getCertificatePEM(), Files.readString(certificateFile));
        assertEquals(bundle.getPrivateKeyPEM(), Files.readString(keyFile));
    }

    @Test
    void makesTheCertificateForAKeyThatHasNone(@TempDir Path dir) throws Exception {
        // As a crash between the two writes, or another creator between them, leaves it
        String key = otherKey();
        Path keyFile = Files.writeString(dir.resolve("host-key.pem"), key);
        Path certificateFile = dir.resolve("host-cert.pem");

        DtlsCertificate created = DtlsCertificate.loadOrCreate(certificateFile, keyFile);

        assertEquals(key, Files.readString(keyFile), "the key stays");
        assertEquals(Files.readString(certificateFile), created.dtls().certificatePem());
        // of() checked that the key signs for the certificate's public key
        assertArrayEquals(created.sha256(), DtlsCertificate.load(certificateFile, keyFile).sha256());
    }

    @Test
    void refusesACertificateWithoutItsKey(@TempDir Path dir) throws Exception {
        Path certificateFile = dir.resolve("host-cert.pem");
        Path keyFile = dir.resolve("host-key.pem");
        DtlsCertificate.loadOrCreate(certificateFile, keyFile);
        String certificate = Files.readString(certificateFile);
        Files.delete(keyFile);

        IOException refused = assertThrows(IOException.class, () -> DtlsCertificate.loadOrCreate(certificateFile, keyFile));
        assertTrue(refused.getMessage().contains("Restore the key"), refused.getMessage());
        assertEquals(certificate, Files.readString(certificateFile));
        assertFalse(Files.exists(keyFile), "a new key would not match the certificate clients hold");
    }

    @Test
    void concurrentCreatorsOfAPairAllEndUpWithTheWinnersCertificate(@TempDir Path dir) throws Exception {
        Path certificateFile = dir.resolve("host-cert.pem");
        Path keyFile = dir.resolve("host-key.pem");
        int racers = 8;
        CyclicBarrier start = new CyclicBarrier(racers);
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        try {
            Callable<String> racer = () -> {
                start.await();
                return HexFormat.of().formatHex(DtlsCertificate.loadOrCreate(certificateFile, keyFile).sha256());
            };
            Set<String> fingerprints = pool.invokeAll(IntStream.range(0, racers).mapToObj(i -> racer).toList())
                    .stream().map(result -> {
                        try {
                            return result.get();
                        } catch (Exception e) {
                            throw new AssertionError("a racer failed instead of loading the winner's pair", e);
                        }
                    }).collect(Collectors.toSet());

            assertEquals(Set.of(HexFormat.of().formatHex(DtlsCertificate.load(certificateFile, keyFile).sha256())),
                    fingerprints);
        } finally {
            pool.shutdownNow();
        }
    }

    private static String otherKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(generator.generateKeyPair().getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
    }
}

/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HexFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One DTLS certificate and private key that every peer of an endpoint presents, so the endpoint can publish the
 * certificate's fingerprint before a client connects. NXS stateless clients build the host's answer from the
 * published fingerprint, so the engine cannot make a certificate per peer for them.
 * <p>
 * libwebrtc takes the fingerprint's digest from the certificate's signature algorithm
 * ({@code rtc_base/ssl_fingerprint.cc}). Only certificates signed with SHA-256 are accepted, so the SHA-256
 * fingerprint is the one the peers announce. A server creates its certificate once with {@link #loadOrCreate}, or
 * supplies one as PEM with {@link #load} or {@link #of}.
 */
public final class DtlsCertificate {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(DtlsCertificate.class);
    /** The most characters a PEM certificate or key may have. */
    static final int MAX_PEM_LENGTH = 65536;
    private static final Pattern PRIVATE_KEY =
            Pattern.compile("-----BEGIN PRIVATE KEY-----([A-Za-z0-9+/=\\s]+)-----END PRIVATE KEY-----");
    /** The signature algorithms allowed, by OID: those whose digest is SHA-256. */
    private static final Map<String, String> SHA256_SIGNATURES = Map.of(
            "1.2.840.10045.4.3.2", "SHA256withECDSA",
            "1.2.840.113549.1.1.11", "SHA256withRSA");
    /**
     * The common name libwebrtc gives the certificates it generates ({@code rtc_base/rtc_certificate_generator.cc}).
     * The certificate travels in clear during the handshake, so it names nothing about the server.
     */
    private static final X500Name SUBJECT = new X500Name("CN=WebRTC");
    /**
     * RFC 5280's value for a certificate without a well-defined expiration date (section 4.1.2.5). Clients pin the
     * certificate by fingerprint and it is never replaced, and libwebrtc checks only the fingerprint of a peer's
     * certificate, not its validity ({@code OpenSSLStreamAdapter::VerifyPeerCertificate}).
     */
    private static final Date NOT_AFTER = Date.from(Instant.parse("9999-12-31T23:59:59Z"));
    private static final HexFormat SDP_HEX = HexFormat.ofDelimiter(":").withUpperCase();

    private final DtlsIdentity dtls;
    private final byte[] sha256;

    private DtlsCertificate(DtlsIdentity dtls, byte[] sha256) {
        this.dtls = dtls;
        this.sha256 = sha256;
    }

    /**
     * Loads the certificate and key from one PEM file, creating them there when the file does not exist. The file
     * is never replaced once it exists, because clients pin the certificate's fingerprint. Keep it next to the
     * server identity key ({@link ServerIdentity#loadOrCreate}): both are the server's long-lived keys.
     * <p>
     * A new certificate has an ECDSA P-256 key and is signed with SHA256withECDSA, as libwebrtc generates its own,
     * but never expires. The file holds a {@code CERTIFICATE} block and a PKCS#8 {@code PRIVATE KEY} block and is
     * handled as the server identity key is: an existing file may be a symbolic link, and creating writes it
     * owner-only, refuses a symbolic link at the path, and, when another process created it at the same time,
     * loads that one.
     *
     * @param file the PEM file to load or create
     * @return the certificate
     * @throws IOException              if the file cannot be read or created, or is larger than 128 KiB
     * @throws GeneralSecurityException if the file holds no usable certificate and key, see {@link #of}
     */
    public static DtlsCertificate loadOrCreate(Path file) throws IOException, GeneralSecurityException {
        Path path = file.toAbsolutePath();
        KeyFile.Origin origin = KeyFile.ensure(path, "the DTLS certificate", DtlsCertificate::generate);
        if (Files.size(path) > 2L * MAX_PEM_LENGTH) {
            throw new IOException("Oversized DTLS certificate file " + path);
        }
        DtlsCertificate certificate = combined(Files.readString(path, StandardCharsets.US_ASCII), path.toString());
        log.info("{} the NetherNet DTLS certificate {} ({})", origin.verb(), path, certificate.fingerprint());
        return certificate;
    }

    /**
     * Reads a certificate and key the server supplies as PEM files.
     *
     * @param certificate an X.509 certificate signed with SHA-256
     * @param privateKey  the certificate's private key, PKCS#8 ({@code BEGIN PRIVATE KEY})
     * @throws IOException              if a file cannot be read or is larger than 64 KiB
     * @throws GeneralSecurityException if the certificate or key cannot be used, see {@link #of}
     */
    public static DtlsCertificate load(Path certificate, Path privateKey) throws IOException, GeneralSecurityException {
        if (Files.size(certificate) > MAX_PEM_LENGTH || Files.size(privateKey) > MAX_PEM_LENGTH) {
            throw new IOException("Oversized PEM DTLS certificate or key");
        }
        return of(Files.readString(certificate, StandardCharsets.US_ASCII),
                Files.readString(privateKey, StandardCharsets.US_ASCII));
    }

    /**
     * Checks a certificate and key given as PEM text.
     *
     * @param certificatePem an X.509 certificate signed with SHA-256, ECDSA or RSA
     * @param privateKeyPem  the certificate's private key, PKCS#8 ({@code BEGIN PRIVATE KEY})
     * @throws GeneralSecurityException if the certificate or key cannot be read, the certificate is not signed with
     *                                  SHA-256, or the key is not the certificate's
     */
    public static DtlsCertificate of(String certificatePem, String privateKeyPem) throws GeneralSecurityException {
        if (certificatePem.length() > MAX_PEM_LENGTH || privateKeyPem.length() > MAX_PEM_LENGTH) {
            throw new GeneralSecurityException("Oversized PEM DTLS certificate or key");
        }
        X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(certificatePem.getBytes(StandardCharsets.US_ASCII)));
        String signature = SHA256_SIGNATURES.get(certificate.getSigAlgOID());
        if (signature == null) {
            throw new GeneralSecurityException("The DTLS certificate is signed with " + certificate.getSigAlgName()
                    + "; it must be signed with SHA256withECDSA or SHA256withRSA, as libwebrtc takes the fingerprint's "
                    + "digest from the signature and the published fingerprint is SHA-256");
        }
        PrivateKey key = privateKey(privateKeyPem, certificate.getPublicKey().getAlgorithm());
        requirePair(certificate, key, signature);
        byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
        return new DtlsCertificate(new DtlsIdentity(certificatePem, privateKeyPem), sha256);
    }

    /** @return the certificate and key, as every peer of the endpoint presents them */
    public DtlsIdentity dtls() {
        return dtls;
    }

    /** @return the SHA-256 digest of the certificate's DER encoding, which its fingerprint shows */
    public byte[] sha256() {
        return sha256.clone();
    }

    @Override
    public String toString() {
        return "DtlsCertificate[" + fingerprint() + "]";
    }

    /** The fingerprint as SDP writes it. */
    private String fingerprint() {
        return "sha-256 " + SDP_HEX.formatHex(sha256);
    }

    /** Splits the file {@link #loadOrCreate} keeps into its certificate and key. */
    private static DtlsCertificate combined(String text, String source) throws GeneralSecurityException {
        String certificate = null;
        String privateKey = null;
        try (PemReader reader = new PemReader(new StringReader(text))) {
            for (PemObject block; (block = reader.readPemObject()) != null; ) {
                switch (block.getType()) {
                    case "CERTIFICATE" -> {
                        if (certificate != null) {
                            throw new GeneralSecurityException("More than one certificate in " + source);
                        }
                        certificate = pem(block.getType(), block.getContent());
                    }
                    case "PRIVATE KEY" -> {
                        if (privateKey != null) {
                            throw new GeneralSecurityException("More than one private key in " + source);
                        }
                        privateKey = pem(block.getType(), block.getContent());
                    }
                    default -> throw new GeneralSecurityException("Expected a certificate and a PKCS#8 private key in "
                            + source + ", found " + block.getType());
                }
            }
        } catch (IOException | RuntimeException malformed) {
            throw new GeneralSecurityException("Cannot read the PEM in " + source, malformed);
        }
        if (certificate == null || privateKey == null) {
            throw new GeneralSecurityException("Expected a certificate and a PKCS#8 private key in " + source);
        }
        return of(certificate, privateKey);
    }

    private static String generate() throws IOException, GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair pair = generator.generateKeyPair();
        // A day back, as libwebrtc does for its own, for peers whose clock is behind
        Date notBefore = Date.from(Instant.now().minus(Duration.ofDays(1)));
        BigInteger serial = new BigInteger(64, new SecureRandom()).add(BigInteger.ONE);
        byte[] certificate;
        try {
            certificate = new JcaX509v3CertificateBuilder(SUBJECT, serial, notBefore, NOT_AFTER, SUBJECT,
                    pair.getPublic())
                    .build(new JcaContentSignerBuilder("SHA256withECDSA").build(pair.getPrivate()))
                    .getEncoded();
        } catch (OperatorCreationException e) {
            throw new GeneralSecurityException("Cannot sign the DTLS certificate", e);
        }
        byte[] key = pair.getPrivate().getEncoded();
        try {
            return pem("CERTIFICATE", certificate) + pem("PRIVATE KEY", key);
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private static String pem(String type, byte[] der) {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }

    private static PrivateKey privateKey(String pem, String algorithm) throws GeneralSecurityException {
        Matcher block = PRIVATE_KEY.matcher(pem);
        if (!block.find()) {
            throw new GeneralSecurityException("A PKCS#8 PEM private key (BEGIN PRIVATE KEY) is required");
        }
        byte[] der;
        try {
            der = Base64.getMimeDecoder().decode(block.group(1));
        } catch (IllegalArgumentException e) {
            throw new GeneralSecurityException("Unreadable PEM private key", e);
        }
        try {
            return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
        } finally {
            Arrays.fill(der, (byte) 0);
        }
    }

    /** Signs a random challenge with the key and checks it with the certificate. */
    private static void requirePair(X509Certificate certificate, PrivateKey key, String algorithm)
            throws GeneralSecurityException {
        byte[] challenge = new byte[32];
        new SecureRandom().nextBytes(challenge);
        Signature signer = Signature.getInstance(algorithm);
        signer.initSign(key);
        signer.update(challenge);
        byte[] signed = signer.sign();
        signer.initVerify(certificate.getPublicKey());
        signer.update(challenge);
        if (!signer.verify(signed)) {
            throw new GeneralSecurityException("The DTLS private key does not belong to the certificate");
        }
    }
}

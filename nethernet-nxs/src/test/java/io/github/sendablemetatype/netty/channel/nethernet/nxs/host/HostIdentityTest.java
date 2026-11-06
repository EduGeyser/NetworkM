package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The DTLS identity a host supplies, checked as upstream's {@code NativeHostIdentity} checks it. */
class HostIdentityTest {

    private static X509Bundle bundle(CertificateBuilder.Algorithm algorithm) throws Exception {
        return new CertificateBuilder().subject("CN=NetherNet Host").algorithm(algorithm)
                .setIsCertificateAuthority(true).buildSelfSigned();
    }

    @Test
    void publishesTheSha256FingerprintOfTheCertificate() throws Exception {
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp256);
        HostIdentity identity = HostIdentity.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM());

        byte[] der = CertificateFactory.getInstance("X.509").generateCertificate(
                new ByteArrayInputStream(bundle.getCertificatePEM().getBytes(StandardCharsets.US_ASCII))).getEncoded();
        assertEquals(DtlsFingerprint.of(MessageDigest.getInstance("SHA-256").digest(der)), identity.fingerprint());
        assertEquals(bundle.getCertificatePEM(), identity.dtls().certificatePem());
        assertFalse(identity.toString().contains("PRIVATE"));
    }

    @Test
    void takesRsaSignedWithSha256Too() throws Exception {
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.rsa2048);
        assertTrue(HostIdentity.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM()).fingerprint().value()
                .startsWith("sha-256 "));
    }

    @Test
    void refusesACertificateSignedWithAnotherDigest() throws Exception {
        // libwebrtc would announce a sha-384 fingerprint for it, which no client built from the profile expects
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp384);
        GeneralSecurityException refused = assertThrows(GeneralSecurityException.class,
                () -> HostIdentity.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM()));
        assertTrue(refused.getMessage().contains("SHA-256"), refused.getMessage());
    }

    @Test
    void refusesAKeyThatIsNotTheCertificates() throws Exception {
        X509Bundle certificate = bundle(CertificateBuilder.Algorithm.ecp256);
        X509Bundle other = bundle(CertificateBuilder.Algorithm.ecp256);
        assertThrows(GeneralSecurityException.class,
                () -> HostIdentity.of(certificate.getCertificatePEM(), other.getPrivateKeyPEM()));
    }

    @Test
    void refusesAKeyThatIsNotPkcs8() throws Exception {
        X509Bundle bundle = bundle(CertificateBuilder.Algorithm.ecp256);
        String sec1 = bundle.getPrivateKeyPEM().replace("PRIVATE KEY", "EC PRIVATE KEY");
        assertThrows(GeneralSecurityException.class, () -> HostIdentity.of(bundle.getCertificatePEM(), sec1));
    }
}

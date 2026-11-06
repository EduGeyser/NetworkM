package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.netty.handler.ssl.SslContext;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TlsContextsTest {
    private static X509Bundle certificate;

    @TempDir
    Path dir;

    @BeforeAll
    static void certificate() throws Exception {
        X509Bundle authority = new CertificateBuilder()
                .subject("CN=NetherNet Test CA")
                .setIsCertificateAuthority(true)
                .ecp256()
                .buildSelfSigned();
        certificate = new CertificateBuilder()
                .subject("CN=NetherNet Test Server")
                .addSanIpAddress("127.0.0.1")
                .notBefore(Instant.now().minus(1, ChronoUnit.HOURS))
                .notAfter(Instant.now().plus(1, ChronoUnit.DAYS))
                .ecp256()
                .buildIssuedBy(authority);
    }

    @Test
    void readsAPemPair() throws Exception {
        Path chain = Files.writeString(dir.resolve("chain.pem"), certificate.getCertificatePathPEM());
        Path key = Files.writeString(dir.resolve("key.pem"), certificate.getPrivateKeyPEM());

        SslContext context = TlsContexts.fromPem(chain, key, null);

        assertTrue(context.isServer());
    }

    @Test
    void namesTheMissingFile() throws Exception {
        Path chain = Files.writeString(dir.resolve("chain.pem"), certificate.getCertificatePathPEM());
        Path key = dir.resolve("missing-key.pem");

        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> TlsContexts.fromPem(chain, key, null));

        assertTrue(failure.getMessage().contains("private key " + key + " does not exist"), failure.getMessage());
    }

    @Test
    void namesBothFilesWhenTheyDoNotFormAPair() throws Exception {
        Path chain = Files.writeString(dir.resolve("chain.pem"), certificate.getCertificatePathPEM());
        Path key = Files.writeString(dir.resolve("key.pem"), "not a key");

        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> TlsContexts.fromPem(chain, key, null));

        assertTrue(failure.getMessage().contains(chain.toString()) && failure.getMessage().contains(key.toString()),
                failure.getMessage());
    }

    @Test
    void readsAPkcs12Keystore() throws Exception {
        Path keystore = certificate.toTempKeyStoreFile("secret".toCharArray()).toPath();
        try {
            assertTrue(TlsContexts.fromPkcs12(keystore, "secret").isServer());
            IllegalArgumentException failure =
                    assertThrows(IllegalArgumentException.class, () -> TlsContexts.fromPkcs12(keystore, "wrong"));
            assertTrue(failure.getMessage().contains(keystore.toString()), failure.getMessage());
        } finally {
            Files.deleteIfExists(keystore);
        }
    }
}

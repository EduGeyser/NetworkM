package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import javax.net.ssl.SSLContext;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Certificates made for each test run, so none can expire in the repository: a test CA and server
 * certificates it issues. Clients trust the CA, never everything.
 */
final class TestCertificates {
    private TestCertificates() {
    }

    static X509Bundle authority() throws Exception {
        return new CertificateBuilder()
                .subject("CN=NetherNet Test CA")
                .setIsCertificateAuthority(true)
                .notBefore(Instant.now().minus(1, ChronoUnit.HOURS))
                .notAfter(Instant.now().plus(1, ChronoUnit.DAYS))
                .ecp256()
                .buildSelfSigned();
    }

    /** A server certificate for 127.0.0.1 and the given names. */
    static X509Bundle server(X509Bundle authority, String... names) throws Exception {
        CertificateBuilder builder = new CertificateBuilder()
                .subject("CN=NetherNet Test Server")
                .addSanIpAddress("127.0.0.1")
                .addExtendedKeyUsageServerAuth()
                .notBefore(Instant.now().minus(1, ChronoUnit.HOURS))
                .notAfter(Instant.now().plus(1, ChronoUnit.DAYS))
                .ecp256();
        for (String name : names) {
            builder.addSanDnsName(name);
        }
        return builder.buildIssuedBy(authority);
    }

    static SslContext serverContext(X509Bundle server) throws Exception {
        return SslContextBuilder.forServer(server.getKeyPair().getPrivate(), server.getCertificatePath()).build();
    }

    /** A client context that trusts the authority, for raw socket tests. */
    static SSLContext trusting(X509Bundle authority) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, authority.toTrustManagerFactory().getTrustManagers(), null);
        return context;
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.Objects;

/**
 * A DTLS certificate and its private key, both PEM encoded. The key is never
 * printed by {@link #toString()}.
 *
 * @param certificatePem the certificate
 * @param privateKeyPem the private key
 */
public record DtlsIdentity(String certificatePem, String privateKeyPem) {

    public DtlsIdentity {
        Objects.requireNonNull(certificatePem, "certificatePem");
        Objects.requireNonNull(privateKeyPem, "privateKeyPem");
    }

    @Override
    public String toString() {
        return "DtlsIdentity[certificatePem=" + certificatePem.length() + " chars, privateKeyPem=<redacted>]";
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import java.util.Map;

/**
 * Receives the secrets from which the endpoint's native prefilter derives the ICE password of a ticket, so that a
 * first STUN request no installed key signed is dropped before it reaches Java. The password of token {@code u} is
 * the unpadded standard base64 of the first 24 bytes of {@code HMAC-SHA256(secret, context || u)}, with the secret
 * chosen by the token's {@code NXS1<keyId>} prefix. webrtc-java's {@code IceUdpEndpoint.setDerivedPasswords} has
 * this shape.
 */
@FunctionalInterface
public interface PrefilterKeySink {
    /**
     * Replaces the installed secrets. Called on the validator's lock, now and after every key change; must not
     * block. The arrays are wiped once this returns, so the sink copies what it keeps.
     *
     * @param context              The HMAC input that precedes the token
     * @param secretsByTokenPrefix Each installed epoch's secret by {@code NXS1<keyId>}; empty to drop every ticket
     */
    void install(byte[] context, Map<String, byte[]> secretsByTokenPrefix);
}

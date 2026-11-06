package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import java.security.GeneralSecurityException;

/**
 * Adds the endpoint's server identity assertion ({@code a=identity}) to an answer. Clients since 26.40 refuse an
 * answer without one, and the NXS specification does not say who adds it to a host-built assisted answer (gap G1),
 * so the endpoint signs every assisted player answer with the identity it uses for all its join sources.
 */
@FunctionalInterface
public interface AnswerSigner {
    /**
     * @param answerSdp The answer without an identity line
     * @return The same answer with the endpoint's {@code a=identity} line
     */
    String sign(String answerSdp) throws GeneralSecurityException;
}

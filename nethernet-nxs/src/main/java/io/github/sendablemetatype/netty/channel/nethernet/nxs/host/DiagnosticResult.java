package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * What the host saw of one connectivity check it admitted (diagnostic-v1). It is the host's own observation: a PONG
 * sent does not show that the prober received it, which only the prober's report can.
 *
 * @param attemptId      The attempt's ID, which the prober's report carries too
 * @param keyId          The admission key the permit was issued under
 * @param target         The endpoint and candidate revision the permit checks; port 0 for an assisted check
 * @param offerDigestHex SHA-256 of the prober's offer, as the prober reports it
 * @param outcome        How the check ended
 * @param selectedLocal  The selected pair's local side once both data channels opened, else null
 * @param selectedRemote The selected pair's remote side once both data channels opened, else null
 * @param completedAt    When the check's peer was gone, epoch milliseconds
 */
public record DiagnosticResult(String attemptId, String keyId, DiagnosticHostPolicy.Endpoint target,
                               String offerDigestHex, Outcome outcome, InetSocketAddress selectedLocal,
                               InetSocketAddress selectedRemote, int sentFrames, int sentBytes, int receivedFrames,
                               int receivedBytes, long completedAt) {
    public DiagnosticResult {
        Objects.requireNonNull(attemptId, "attemptId");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(outcome, "outcome");
    }

    /** How a check ended. */
    public enum Outcome {
        /** The host answered the PING and the peer closed cleanly while the permit still held. */
        PONG_SENT,
        /** The permit expired, or the policy, key, endpoint or provider authority behind it was withdrawn. */
        WITHDRAWN,
        /** The PONG went out, but the permit no longer held once the peer was gone. */
        OBSERVATION_INVALIDATED,
        /** The data channels did not open and exchange the frames within the handshake limit. */
        HANDSHAKE_TIMEOUT,
        /** The connection or a data channel failed before the PONG. */
        TRANSPORT_FAILED,
        /** The engine could not create the peer or answer the offer, or answered with another identity. */
        NEGOTIATION_FAILED,
        /** The assisted answer could not be built or handed back. */
        ANSWER_FAILED,
        /** The prober opened other data channels, or sent text, more than one frame or a frame off the reliable one. */
        INVALID_PROTOCOL,
        /** The selected pair is not on this host's port, or not between the endpoints the permit allows. */
        INVALID_PATH,
        /** The prober's frame is not the PING of this attempt. */
        INVALID_PING,
        /** The host stopped serving diagnostics. */
        HOST_CLOSED,
        /** The peer's teardown failed. */
        CLEANUP_FAILED
    }

    /** Whether the host answered the PING and saw the check end cleanly. */
    public boolean success() {
        return outcome == Outcome.PONG_SENT;
    }
}

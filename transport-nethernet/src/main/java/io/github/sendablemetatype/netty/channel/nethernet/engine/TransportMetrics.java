package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * A connection's transport metrics. ICE and SCTP round-trip times are kept
 * apart: the SCTP one includes the peer's delayed acknowledgments, so on a
 * quiet link it reads higher than the network round trip.
 *
 * @param iceRoundTripTime the selected candidate pair's current round-trip
 *        time, empty until ICE has measured one
 * @param sctp the SCTP association's metrics, empty until the association exists
 */
public record TransportMetrics(Optional<Duration> iceRoundTripTime, Optional<Sctp> sctp) {

    /** No metrics: the peer has no connection (yet, or any more). */
    public static final TransportMetrics NONE = new TransportMetrics(Optional.empty(), Optional.empty());

    public TransportMetrics {
        Objects.requireNonNull(iceRoundTripTime, "iceRoundTripTime");
        Objects.requireNonNull(sctp, "sctp");
    }

    /**
     * An SCTP association's metrics.
     *
     * @param smoothedRoundTripTime the smoothed round-trip time
     * @param congestionWindowBytes the congestion window
     * @param unackedDataCount chunks sent and not yet acknowledged
     * @param peerReceiveWindowBytes the peer's advertised receive window
     * @param packetsSent packets sent
     * @param messagesSent messages sent
     * @param packetsRetransmitted packets retransmitted
     * @param bytesRetransmitted bytes retransmitted
     * @param packetsReceived packets received
     * @param messagesReceived messages received
     */
    public record Sctp(
            Duration smoothedRoundTripTime,
            long congestionWindowBytes,
            long unackedDataCount,
            long peerReceiveWindowBytes,
            long packetsSent,
            long messagesSent,
            long packetsRetransmitted,
            long bytesRetransmitted,
            long packetsReceived,
            long messagesReceived) {
    }
}

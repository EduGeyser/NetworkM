package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * A selected ICE candidate pair.
 *
 * @param local the local candidate's address, or null if the engine does not
 *        report it
 * @param localType the local candidate's type
 * @param remote the address the remote side sends from: for a remote relay
 *        candidate the TURN relay's address, never the related address the
 *        remote side declared. Null if the engine does not know it.
 * @param remoteType the remote candidate's type
 */
public record TransportPath(
        InetSocketAddress local,
        CandidateType localType,
        InetSocketAddress remote,
        CandidateType remoteType) {

    public TransportPath {
        Objects.requireNonNull(localType, "localType");
        Objects.requireNonNull(remoteType, "remoteType");
    }

    /** Whether either side of the pair is a TURN relay candidate. */
    public boolean relayed() {
        return localType == CandidateType.RELAY || remoteType == CandidateType.RELAY;
    }

    /** An ICE candidate type. */
    public enum CandidateType {
        /** An address of a local interface. */
        HOST,
        /** The public side of a NAT, as a STUN server saw it, or as a host announces it. */
        SERVER_REFLEXIVE,
        /** An address learned from a connectivity check, never signaled. */
        PEER_REFLEXIVE,
        /** An address on a TURN server that relays for the peer. */
        RELAY,
        /** A type the engine did not report, or a name no type has. */
        UNKNOWN;

        /**
         * Reads the type an SDP candidate line names after {@code typ} (RFC 8839 section 5.1).
         *
         * @param name the name, such as {@code srflx}
         * @return the type, or {@link #UNKNOWN} for a name no type has
         */
        public static CandidateType fromSdpName(String name) {
            return switch (name) {
                case "host" -> HOST;
                case "srflx" -> SERVER_REFLEXIVE;
                case "prflx" -> PEER_REFLEXIVE;
                case "relay" -> RELAY;
                default -> UNKNOWN;
            };
        }
    }
}

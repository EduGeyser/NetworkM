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
         * @return the name an SDP candidate line writes after {@code typ}, such as {@code srflx}
         * @throws IllegalStateException for {@link #UNKNOWN}, which has none
         */
        public String sdpName() {
            return switch (this) {
                case HOST -> "host";
                case SERVER_REFLEXIVE -> "srflx";
                case PEER_REFLEXIVE -> "prflx";
                case RELAY -> "relay";
                case UNKNOWN -> throw new IllegalStateException("No SDP name for an unknown candidate type");
            };
        }

        /**
         * The type preference RFC 8445 section 5.1.2.2 recommends, which ranks a candidate above
         * any of a less preferred type in its priority
         * ({@code type preference << 24 | local preference << 8 | (256 - component)}).
         *
         * @return the preference, 0 to 126
         * @throws IllegalStateException for {@link #UNKNOWN}, which has none
         */
        public int typePreference() {
            return switch (this) {
                case HOST -> 126;
                case PEER_REFLEXIVE -> 110;
                case SERVER_REFLEXIVE -> 100;
                case RELAY -> 0;
                case UNKNOWN -> throw new IllegalStateException("No type preference for an unknown candidate type");
            };
        }

        /**
         * Reads the type an SDP candidate line names after {@code typ} (RFC 8839 section 5.1).
         *
         * @param name the name, such as {@code srflx}
         * @return the type, or {@link #UNKNOWN} for a name no type has
         */
        public static CandidateType fromSdpName(String name) {
            for (CandidateType type : values()) {
                if (type != UNKNOWN && type.sdpName().equals(name)) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }
}

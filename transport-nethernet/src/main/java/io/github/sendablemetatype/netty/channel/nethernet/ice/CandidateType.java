package io.github.sendablemetatype.netty.channel.nethernet.ice;

import java.util.Optional;

/**
 * The type of an ICE candidate, as an SDP candidate line names it after {@code typ}.
 */
public enum CandidateType {
    /** An address of a local interface. */
    HOST("host", 1),
    /** The public side of a NAT, as a STUN server saw it, or as a host announces it. */
    SERVER_REFLEXIVE("srflx", 2),
    /** An address learned from a connectivity check, never signaled. */
    PEER_REFLEXIVE("prflx", 0),
    /** An address on a TURN server that relays for the peer. */
    RELAY("relay", 3);

    private final String sdpName;
    private final int defaultPreference;

    CandidateType(String sdpName, int defaultPreference) {
        this.sdpName = sdpName;
        this.defaultPreference = defaultPreference;
    }

    /**
     * @return The name SDP writes, such as {@code srflx}
     */
    public String sdpName() {
        return this.sdpName;
    }

    /**
     * How strongly libwebrtc prefers a candidate of this type as the {@code m=} and {@code c=}
     * address, relayed over reflexive over host ({@code Candidate::type_preference} in
     * {@code api/candidate.h}).
     */
    int defaultPreference() {
        return this.defaultPreference;
    }

    /**
     * @param sdpName The name SDP writes
     * @return The type, or empty for a name no type has
     */
    public static Optional<CandidateType> fromSdpName(String sdpName) {
        for (CandidateType type : values()) {
            if (type.sdpName.equals(sdpName)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }
}

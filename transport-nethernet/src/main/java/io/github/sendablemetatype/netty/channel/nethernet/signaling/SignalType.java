package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.util.Optional;

/** The four NetherNet signal types, each with the name Xbox RPC and LAN carry it under. */
public enum SignalType {
    /** The client's offer. */
    CONNECT_REQUEST("CONNECTREQUEST"),
    /** The server's answer. */
    CONNECT_RESPONSE("CONNECTRESPONSE"),
    /** One candidate, trickled by either side. */
    CANDIDATE_ADD("CANDIDATEADD"),
    /**
     * A failure, from either side. Vanilla reads the payload as a signed 32-bit decimal number
     * from its signaling error enumeration.
     */
    CONNECT_ERROR("CONNECTERROR");

    private final String wireName;

    SignalType(String wireName) {
        this.wireName = wireName;
    }

    /** @return the name the wire carries, such as {@code CONNECTREQUEST} */
    public String wireName() {
        return wireName;
    }

    /**
     * @param wireName a type as received, matched exactly, case included
     * @return the type with this wire name, or empty for any other text
     */
    public static Optional<SignalType> fromWireName(String wireName) {
        for (SignalType type : values()) {
            if (type.wireName.equals(wireName)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }
}

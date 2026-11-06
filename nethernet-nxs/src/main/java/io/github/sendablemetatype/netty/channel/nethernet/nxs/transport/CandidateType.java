package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

/**
 * The ICE candidate types a host profile may publish, with RFC 8445's recommended type preferences, which rank a
 * published candidate before its place in the list does.
 */
public enum CandidateType {
    HOST("host", 126),
    SRFLX("srflx", 100),
    RELAY("relay", 0);

    private final String wire;
    private final int preference;

    CandidateType(String wire, int preference) {
        this.wire = wire;
        this.preference = preference;
    }

    /** The name on the wire. */
    public String wire() {
        return wire;
    }

    /** The type preference, 0 to 126. */
    public int preference() {
        return preference;
    }
}

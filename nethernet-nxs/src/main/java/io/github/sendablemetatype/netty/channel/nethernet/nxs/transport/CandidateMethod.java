package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

/** How a host found the candidates it publishes, as the connectivity extension names it. */
public enum CandidateMethod {
    /** Configured endpoints. */
    DEFINED("defined"),
    /** Local addresses found on the host. */
    DISCOVERED("discovered"),
    /** A STUN mapping kept warm on the gameplay port. */
    WARM_STUN("warm_stun"),
    /** Gathered for one assisted join; appears only in check results. */
    PER_JOIN("per_join");

    private final String wire;

    CandidateMethod(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    /** @throws IllegalArgumentException If the value is not a known method */
    public static CandidateMethod ofWire(String value) {
        for (CandidateMethod method : values()) {
            if (method.wire.equals(value)) {
                return method;
            }
        }
        throw new IllegalArgumentException("Unknown candidate method");
    }
}

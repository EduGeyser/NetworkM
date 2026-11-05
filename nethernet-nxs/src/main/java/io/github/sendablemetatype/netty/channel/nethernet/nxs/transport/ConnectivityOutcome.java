package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

/** The result of a provider's regional connection check. */
public enum ConnectivityOutcome {
    ESTABLISHED("established"),
    NOT_ESTABLISHED("not-established"),
    /** Inconclusive; leaves the previous decision in place. */
    UNKNOWN("unknown"),
    /** The check could not run; leaves the previous decision in place. */
    UNAVAILABLE("unavailable");

    private final String wire;

    ConnectivityOutcome(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    /** @throws IllegalArgumentException If the value is not a known outcome */
    public static ConnectivityOutcome ofWire(String value) {
        for (ConnectivityOutcome outcome : values()) {
            if (outcome.wire.equals(value)) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("Unknown connectivity outcome");
    }
}

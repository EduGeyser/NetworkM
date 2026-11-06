package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.Objects;
import java.util.OptionalInt;

/**
 * A data channel's label and delivery parameters.
 *
 * @param label the label
 * @param ordered whether messages are delivered in order
 * @param maxRetransmits the retransmission limit, empty for no limit
 * @param maxPacketLifeTimeMs the lifetime limit in milliseconds, empty for no limit
 * @param negotiated whether the channel was negotiated out of band
 * @param protocol the subprotocol, empty if none
 */
public record ChannelParameters(
        String label,
        boolean ordered,
        OptionalInt maxRetransmits,
        OptionalInt maxPacketLifeTimeMs,
        boolean negotiated,
        String protocol) {

    public ChannelParameters {
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(maxRetransmits, "maxRetransmits");
        Objects.requireNonNull(maxPacketLifeTimeMs, "maxPacketLifeTimeMs");
        Objects.requireNonNull(protocol, "protocol");
    }

    /** An in-band, ordered channel that retransmits without limit. */
    public static ChannelParameters reliable(String label) {
        return new ChannelParameters(label, true, OptionalInt.empty(), OptionalInt.empty(), false, "");
    }

    /** An in-band, unordered channel that never retransmits. */
    public static ChannelParameters unreliable(String label) {
        return new ChannelParameters(label, false, OptionalInt.of(0), OptionalInt.empty(), false, "");
    }

    /** Whether every message is retransmitted until delivered. */
    public boolean reliable() {
        return maxRetransmits.isEmpty() && maxPacketLifeTimeMs.isEmpty();
    }
}

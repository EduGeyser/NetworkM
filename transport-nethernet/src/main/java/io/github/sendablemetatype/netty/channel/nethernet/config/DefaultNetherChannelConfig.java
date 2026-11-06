package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherOptions.Rule;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherOptions.Setting;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;

import java.util.Map;

/**
 * The config of one NetherNet connection, a server's child or (through
 * {@link DefaultNetherClientChannelConfig}) a client: how messages are reassembled, paced and
 * counted. It takes these options and Netty's own; any other option is refused with an
 * {@link IllegalArgumentException} naming it, and a value out of an option's range with one
 * naming the range.
 */
public class DefaultNetherChannelConfig extends DefaultChannelConfig {
    public static final int DEFAULT_MAX_REASSEMBLED_BYTES = 16 * 1024 * 1024;
    public static final int DEFAULT_SEND_PAUSE_BYTES = 2 * 1024 * 1024;
    /**
     * The highest pause level: the channel hands the engine another segment while less than the
     * pause level is queued, so the pause level and one segment of the largest size the channel
     * sends ({@link NetherNetConstants#MAX_OUTBOUND_MESSAGE_SIZE}) must fit what every engine
     * holds ({@link DataLink#SEND_QUEUE_CAPACITY}). 16 MiB less 256 KiB.
     */
    public static final int MAX_SEND_PAUSE_BYTES = DataLink.SEND_QUEUE_CAPACITY
            - NetherNetConstants.MAX_OUTBOUND_MESSAGE_SIZE;
    public static final int DEFAULT_MAX_QUEUED_BYTES = 64 * 1024 * 1024;

    /** Every NetherNet option this config handles; subclasses declare theirs here too. */
    final NetherOptions options = new NetherOptions();

    private final Setting<Integer> maxReassembledBytes = options.add(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES,
            DEFAULT_MAX_REASSEMBLED_BYTES, Rule.atLeast(1));
    private final Setting<Integer> sendPauseBytes = options.add(NetherChannelOption.NETHER_SEND_PAUSE_BYTES,
            DEFAULT_SEND_PAUSE_BYTES, Rule.between(1, MAX_SEND_PAUSE_BYTES));
    private final Setting<Integer> maxQueuedBytes = options.add(NetherChannelOption.NETHER_MAX_QUEUED_BYTES,
            DEFAULT_MAX_QUEUED_BYTES, Rule.atLeast(1));
    private final Setting<NetherChannelMetrics> metrics = options.add(NetherChannelOption.NETHER_METRICS,
            null, Rule.orNone());

    public DefaultNetherChannelConfig(Channel channel) {
        super(channel);
    }

    @Override
    protected void autoReadCleared() {
        if (channel instanceof NetherNetChannel netherNetChannel) {
            netherNetChannel.clearReadPending();
        }
    }

    @Override
    public Map<ChannelOption<?>, Object> getOptions() {
        return getOptions(super.getOptions(), options.options());
    }

    @Override
    public <T> T getOption(ChannelOption<T> option) {
        return options.handles(option) ? options.get(option) : super.getOption(option);
    }

    /**
     * Sets a NetherNet option this channel takes, or one of Netty's.
     *
     * @return true, since every other option is refused
     * @throws IllegalArgumentException if this channel does not take the option, or the value is
     *                                  out of its range
     * @throws NullPointerException     if the value is null and the option has no "none"
     */
    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        if (options.handles(option)) {
            options.set(option, value);
            return true;
        }
        if (super.setOption(option, value)) {
            return true;
        }
        throw NetherOptions.unknown(channel, option);
    }

    public int getMaxReassembledBytes() {
        return maxReassembledBytes.get();
    }

    public int getSendPauseBytes() {
        return sendPauseBytes.get();
    }

    public int getMaxQueuedBytes() {
        return maxQueuedBytes.get();
    }

    /** @return the per-connection metrics, or null for none */
    public NetherChannelMetrics getMetrics() {
        return metrics.get();
    }
}

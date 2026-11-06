package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChannel;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultNetherChannelConfig extends DefaultChannelConfig {
    public static final int DEFAULT_MAX_REASSEMBLED_BYTES = 16 * 1024 * 1024;
    public static final int DEFAULT_SEND_PAUSE_BYTES = 2 * 1024 * 1024;
    public static final int DEFAULT_MAX_QUEUED_BYTES = 64 * 1024 * 1024;

    private final Map<ChannelOption<?>, Object> options = new ConcurrentHashMap<>();

    private volatile List<IceServer> iceServers = List.of();
    private volatile CandidatePolicy candidatePolicy = CandidatePolicy.DEFAULT;
    private volatile int maxReassembledBytes = DEFAULT_MAX_REASSEMBLED_BYTES;
    private volatile int sendPauseBytes = DEFAULT_SEND_PAUSE_BYTES;
    private volatile int maxQueuedBytes = DEFAULT_MAX_QUEUED_BYTES;
    private volatile NetherChannelMetrics metrics;

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
        return this.getOptions(
                super.getOptions(),
                NetherChannelOption.NETHER_ICE_SERVERS,
                NetherChannelOption.NETHER_CANDIDATE_POLICY,
                NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES,
                NetherChannelOption.NETHER_SEND_PAUSE_BYTES,
                NetherChannelOption.NETHER_MAX_QUEUED_BYTES,
                NetherChannelOption.NETHER_METRICS
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOption(ChannelOption<T> option) {
        if (option == NetherChannelOption.NETHER_ICE_SERVERS) {
            return (T) this.iceServers;
        } else if (option == NetherChannelOption.NETHER_CANDIDATE_POLICY) {
            return (T) this.candidatePolicy;
        } else if (option == NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES) {
            return (T) Integer.valueOf(this.maxReassembledBytes);
        } else if (option == NetherChannelOption.NETHER_SEND_PAUSE_BYTES) {
            return (T) Integer.valueOf(this.sendPauseBytes);
        } else if (option == NetherChannelOption.NETHER_MAX_QUEUED_BYTES) {
            return (T) Integer.valueOf(this.maxQueuedBytes);
        } else if (option == NetherChannelOption.NETHER_METRICS) {
            return (T) this.metrics;
        } else if (options.containsKey(option)) {
            return (T) options.get(option);
        }

        return super.getOption(option);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        if (option == NetherChannelOption.NETHER_ICE_SERVERS) {
            this.iceServers = List.copyOf((List<IceServer>) value);
        } else if (option == NetherChannelOption.NETHER_CANDIDATE_POLICY) {
            this.candidatePolicy = Objects.requireNonNull((CandidatePolicy) value, "candidatePolicy");
        } else if (option == NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES) {
            this.maxReassembledBytes = positive("maxReassembledBytes", (Integer) value);
        } else if (option == NetherChannelOption.NETHER_SEND_PAUSE_BYTES) {
            this.sendPauseBytes = positive("sendPauseBytes", (Integer) value);
        } else if (option == NetherChannelOption.NETHER_MAX_QUEUED_BYTES) {
            this.maxQueuedBytes = positive("maxQueuedBytes", (Integer) value);
        } else if (option == NetherChannelOption.NETHER_METRICS) {
            this.metrics = (NetherChannelMetrics) value;
        } else if (!super.setOption(option, value)) {
            options.put(option, value);
        }
        return true;
    }

    private static int positive(String name, Integer value) {
        if (value == null || value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    public List<IceServer> getIceServers() {
        return iceServers;
    }

    public CandidatePolicy getCandidatePolicy() {
        return candidatePolicy;
    }

    public int getMaxReassembledBytes() {
        return maxReassembledBytes;
    }

    public int getSendPauseBytes() {
        return sendPauseBytes;
    }

    public int getMaxQueuedBytes() {
        return maxQueuedBytes;
    }

    public NetherChannelMetrics getMetrics() {
        return metrics;
    }
}

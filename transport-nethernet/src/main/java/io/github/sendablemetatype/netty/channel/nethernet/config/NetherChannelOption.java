package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetAnswerDecorator;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.netty.channel.ChannelOption;

import java.util.List;

public class NetherChannelOption<T> extends ChannelOption<T> {

    /**
     * Transforms every SDP answer before it is signaled, e.g. to attach the
     * server identity assertion. Null (the default) uses the built-in
     * server identity assertion.
     */
    public static final ChannelOption<NetherNetAnswerDecorator> NETHER_SERVER_ANSWER_DECORATOR =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_ANSWER_DECORATOR");

    /**
     * STUN and TURN servers every peer of a server or client channel uses, in
     * addition to the ones its signaling hands out. Empty by default.
     */
    public static final ChannelOption<List<IceServer>> NETHER_ICE_SERVERS =
            valueOf(NetherChannelOption.class, "NETHER_ICE_SERVERS");

    /**
     * Where the peers of a server or client channel gather their own
     * candidates: IPv6, local addresses and the port window. Defaults to
     * {@link CandidatePolicy#DEFAULT}.
     */
    public static final ChannelOption<CandidatePolicy> NETHER_CANDIDATE_POLICY =
            valueOf(NetherChannelOption.class, "NETHER_CANDIDATE_POLICY");

    /**
     * The largest message accepted from the remote peer, 16 MiB by default. A
     * larger one ends the connection with
     * {@link TransportDisconnectReason#SPLIT_QUEUE_TOO_LONG}. The inbound queue
     * holds up to twice this many bytes of whole messages that wait for the
     * application.
     */
    public static final ChannelOption<Integer> NETHER_MAX_REASSEMBLED_BYTES =
            valueOf(NetherChannelOption.class, "NETHER_MAX_REASSEMBLED_BYTES");

    /**
     * How many bytes may wait in a data channel's engine queue before the
     * channel stops handing segments to the engine, 2 MiB by default. Segments
     * then wait in Java and the channel turns unwritable until the engine queue
     * drained to a quarter of this. A change applies from the next pause.
     */
    public static final ChannelOption<Integer> NETHER_SEND_PAUSE_BYTES =
            valueOf(NetherChannelOption.class, "NETHER_SEND_PAUSE_BYTES");

    /**
     * How many bytes may wait in Java while the engine queue is full, 64 MiB by
     * default. Past it the connection ends with
     * {@link TransportDisconnectReason#QUEUE_TOO_LONG}.
     */
    public static final ChannelOption<Integer> NETHER_MAX_QUEUED_BYTES =
            valueOf(NetherChannelOption.class, "NETHER_MAX_QUEUED_BYTES");

    /** Per connection counters and events, or null (the default) for none. */
    public static final ChannelOption<NetherChannelMetrics> NETHER_METRICS =
            valueOf(NetherChannelOption.class, "NETHER_METRICS");

    /** A server channel's own counters and events, or null (the default) for none. */
    public static final ChannelOption<NetherServerMetrics> NETHER_SERVER_METRICS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_METRICS");

    /**
     * The timeout in milliseconds for completing the WebRTC handshake on the client before retrying.
     */
    public static final ChannelOption<Integer> NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS =
            valueOf(NetherChannelOption.class, "NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS");

    /**
     * The maximum number of handshake retries before giving up on connecting.
     */
    public static final ChannelOption<Integer> NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS =
            valueOf(NetherChannelOption.class, "NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS");

     /**
     * The timeout in seconds for completing the WebRTC handshake on the server side before automatically closing the connection.
     */
    public static final ChannelOption<Integer> NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS");

    @SuppressWarnings("deprecation")
    protected NetherChannelOption(String name) {
        super(name);
    }
}

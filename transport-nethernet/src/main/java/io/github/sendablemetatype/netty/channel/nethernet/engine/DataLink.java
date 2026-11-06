package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * One data channel of a peer.
 */
public interface DataLink {

    /**
     * How many bytes every engine takes queued on one link, as {@link #queuedBytes()} counts them,
     * before it may refuse a send: 16 MiB. The transport keeps its pause level, plus the largest
     * segment it sends, within this, so pacing never meets the engine's limit.
     *
     * <p>libwebrtc's limit is exactly this: {@code DataChannelInterface::MaxSendQueueSize()}
     * ({@code api/data_channel_interface.cc:48}), which it hands dcSCTP as
     * {@code per_stream_send_queue_limit} ({@code media/sctp/dcsctp_transport.cc:813}; WebRTC
     * main {@code 885c77558b}, 2026-11-06). dcSCTP refuses a send while the stream already holds
     * that much ({@code net/dcsctp/socket/dcsctp_socket.cc:612}), the send fails with
     * {@code RESOURCE_EXHAUSTED}, and the data channel closes
     * ({@code pc/sctp_data_channel.cc:990}).
     */
    int SEND_QUEUE_CAPACITY = 16 * 1024 * 1024;

    /** The link's label and delivery parameters, as negotiated. */
    ChannelParameters parameters();

    /**
     * Queues one message without waiting for the engine. The bytes between the
     * buffer's position and limit are copied before this method returns, so
     * the caller may reuse the buffer at once.
     *
     * <p>The completion runs exactly once: with null when the engine accepted
     * the message for transmission (not when the remote peer received it), or
     * with the cause when it refused it. It may run before this method
     * returns, and on any engine thread. It must not block.
     *
     * @param message the message
     * @param completion receives the result
     */
    void send(ByteBuffer message, Consumer<Throwable> completion);

    /**
     * Bytes passed to {@link #send} and not yet handed to the network, counted
     * from the moment {@code send} returns, before its completion runs. A
     * message the engine refused leaves the count too, though possibly only with
     * the engine's next report of what it sent; a refusal ends the connection,
     * so the transport does not pace on it. Cheap to read: no call into the
     * engine's threads. The transport paces its sends on this, so a burst of
     * sends must show here at once.
     */
    long queuedBytes();

    /**
     * Sets the level for {@link PeerListener#onQueueLow}: it fires when
     * {@link #queuedBytes()} falls to or below this level after being above
     * it. The level starts at zero.
     *
     * @param bytes the level, zero or more
     */
    void lowWatermark(long bytes);
}

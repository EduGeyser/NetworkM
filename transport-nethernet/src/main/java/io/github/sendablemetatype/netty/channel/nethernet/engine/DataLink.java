package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * One data channel of a peer.
 */
public interface DataLink {

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
     * Bytes accepted by {@link #send} and not yet handed to the network. Cheap
     * to read: no call into the engine's threads.
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

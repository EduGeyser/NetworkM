package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;

import java.util.List;
import java.util.function.Consumer;

/**
 * A source of joins for one NetherNet server endpoint: HTTP signaling, Xbox RPC signaling, LAN
 * discovery or NXS assisted joins. An endpoint takes several sources at once and can add, remove
 * or restart one while players stay connected, since a join's reply path belongs to the join, not
 * to the endpoint.
 *
 * <p>A source only carries offers and answers. Admission (identity check, throttle, capacity)
 * happens in the endpoint, the same for every source.
 */
public interface JoinSource extends AutoCloseable {

    /**
     * Starts delivering joins to the sink. Called once, by the endpoint that owns the source.
     * Never blocks: a source that has to connect somewhere does so in the background and reports
     * its progress through {@link #health()}.
     *
     * @param sink where joins go
     */
    void start(JoinSink sink);

    /** The source's current state. */
    SignalingHealth health();

    /**
     * Adds a listener for state changes. Listeners run on the source's own thread and must not
     * block.
     *
     * @param listener the listener
     */
    void addHealthListener(Consumer<SignalingHealth> listener);

    /** Removes a listener added with {@link #addHealthListener}. */
    void removeHealthListener(Consumer<SignalingHealth> listener);

    /**
     * STUN and TURN servers for peers of this source's joins: only current, unexpired ones. Read
     * when a join is admitted.
     */
    List<IceServer> iceServers();

    /**
     * Stops delivering joins and releases the source's resources. Joins already admitted keep
     * their peers; replies to them may fail from now on. Idempotent.
     */
    @Override
    void close();
}

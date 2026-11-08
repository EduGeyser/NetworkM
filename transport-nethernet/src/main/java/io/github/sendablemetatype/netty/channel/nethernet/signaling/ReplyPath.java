package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * The way back to one joining client, bound to the source and route its offer came on. Each send
 * returns its own result, so the join that sent it handles its own failure.
 *
 * <p>All methods may be called from any thread and never block.
 */
public interface ReplyPath {

    /**
     * Whether the answer goes out on its own, followed by candidate signals (Xbox RPC, LAN), or
     * once, with every gathered candidate in it (HTTP, NXS assisted joins).
     */
    boolean trickles();

    /**
     * Sends the answer. Called at most once per join.
     *
     * @param sdp the answer SDP, with the server's identity already added
     * @return completes when the source handed the answer on, or exceptionally if it could not
     */
    CompletionStage<Void> answer(String sdp);

    /**
     * Sends a local candidate, after the answer. Only for paths that {@link #trickles() trickle}.
     *
     * @param candidate the candidate line, without the {@code a=} prefix
     * @return completes when the source handed the candidate on
     * @throws UnsupportedOperationException if this path does not trickle
     */
    CompletionStage<Void> candidate(String candidate);

    /**
     * Refuses the join, or fails it after it was admitted. Called at most once, and never after a
     * successful {@link #answer} on a path that does not trickle. The endpoint reports a refusal
     * to its metrics once this completes, unless it fails with a
     * {@link JoinAlreadySettledException}: a source that settles joins on its own, as HTTP
     * signaling does at its deadline, fails a refusal that comes after that and reports its own.
     *
     * @param refusal why
     * @return completes when the source handed the refusal on; fails with a
     *         {@link JoinAlreadySettledException} when the join was settled before
     */
    CompletionStage<Void> refuse(JoinRefusal refusal);

    /**
     * Sets the handler for this join's later signals from the client: trickled candidates and
     * errors. Signals that arrived before are kept, up to a small bound, and delivered when the
     * handler is set. Called once.
     *
     * @param handler receives the signals on the source's thread; must not block
     */
    void onRemoteSignal(Consumer<RemoteSignal> handler);

    /**
     * Completes when the client can no longer receive an answer it does not have yet: the HTTP
     * request ended or passed its deadline, the answer could not be delivered, or an NXS attempt
     * expired. Never completes once the client got the answer. When it completes, the endpoint
     * stops the join and closes its child, unless the child already went active. Xbox RPC
     * completes it when the service reports that the answer could not be delivered; LAN never
     * completes it.
     */
    CompletionStage<Void> abandoned();

    /**
     * Tells the source the join's connection went active: the client got its answer and reached
     * the server. Called at most once, before {@link #finished()}; a join that fails never gets
     * it.
     */
    default void connected() {
    }

    /**
     * Tells the source the endpoint is done with this join: it was refused, or its connection
     * closed. The source drops what it keeps to route the join's signals, such as a handler by
     * connection ID or a pinned LAN peer. Called once, last; later sends may fail.
     */
    default void finished() {
    }
}

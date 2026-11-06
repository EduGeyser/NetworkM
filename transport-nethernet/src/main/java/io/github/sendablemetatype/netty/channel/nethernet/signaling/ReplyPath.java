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
     * successful {@link #answer} on a path that does not trickle.
     *
     * @param refusal why
     * @return completes when the source handed the refusal on
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
     * Completes when the client can no longer receive an answer: the HTTP request ended or
     * passed its deadline, or an NXS attempt expired. Message based paths never complete it.
     */
    CompletionStage<Void> abandoned();
}

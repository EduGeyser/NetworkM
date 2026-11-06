package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.concurrent.CompletionStage;

/**
 * One WebRTC peer connection. Belongs to exactly one channel from creation.
 *
 * <p>Whether signaling trickles candidates or sends one full description is
 * the signaling's choice, not the engine's: the engine always reports each
 * local candidate and the end of gathering, and also offers the full
 * description through {@link #gatheredLocalDescription()}.
 */
public interface Peer {

    /**
     * Answerer only: applies the remote offer, then creates and applies the
     * answer. The engine removes {@code a=identity} lines before it parses the
     * offer; the caller passes the offer as received.
     *
     * @param offer the remote offer SDP
     * @return completes with the local answer SDP, without candidates gathered
     *         after it was created
     */
    CompletionStage<String> answer(String offer);

    /**
     * Offerer only: creates and applies the offer. Data links opened before
     * this call are part of it.
     *
     * @return completes with the local offer SDP
     */
    CompletionStage<String> offer();

    /**
     * Offerer only: applies the remote answer. The engine removes
     * {@code a=identity} lines first.
     *
     * @param answer the remote answer SDP
     * @return completes once the answer has been applied
     */
    CompletionStage<Void> applyAnswer(String answer);

    /**
     * Applies a remote candidate. Safe at any time: candidates that arrive
     * before the remote description has been applied are held and applied in
     * order after it. Which candidates may reach this method at all is the
     * caller's policy. A candidate the engine refuses is logged, not fatal.
     *
     * @param candidate the candidate line, with or without the {@code a=} prefix
     * @param mid the media ID the candidate belongs to
     */
    void addRemoteCandidate(String candidate, String mid);

    /**
     * The local description with every gathered candidate and an
     * {@code a=end-of-candidates} line, once gathering is complete.
     *
     * @return completes after {@link PeerListener#onGatheringComplete()}
     */
    CompletionStage<String> gatheredLocalDescription();

    /**
     * Offerer only: opens a data link. Call before {@link #offer()}.
     *
     * @param parameters the link's label and delivery parameters
     * @return the link; usable once {@link PeerListener#onChannelOpen} fires for it
     */
    DataLink openChannel(ChannelParameters parameters);

    /**
     * Reads the connection's transport metrics.
     *
     * @return completes with the metrics, or with
     *         {@link TransportMetrics#NONE} if the peer is closed
     */
    CompletionStage<TransportMetrics> metrics();

    /**
     * Closes the peer. Idempotent and allowed from listener callbacks. After a
     * local close, {@link PeerListener#onClosed} does not fire.
     *
     * @return completes after the peer's termination (see the package
     *         documentation); the same stage on every call
     */
    CompletionStage<Void> close();
}

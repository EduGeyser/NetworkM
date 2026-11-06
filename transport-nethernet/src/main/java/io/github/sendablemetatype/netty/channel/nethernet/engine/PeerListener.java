package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.nio.ByteBuffer;

/**
 * A peer's events. Called on engine threads, serialized per peer; see the
 * {@linkplain io.github.sendablemetatype.netty.channel.nethernet.engine
 * package documentation}.
 */
public interface PeerListener {

    /**
     * A local candidate was gathered.
     *
     * @param candidate the candidate line, without the {@code a=} prefix
     * @param mid the media ID it belongs to
     */
    void onLocalCandidate(String candidate, String mid);

    /** Gathering is complete; no more local candidates follow. */
    void onGatheringComplete();

    /**
     * Answerer only: the remote peer opened a data link.
     *
     * @param link the link
     * @return true to keep the link, false to have the engine close it
     */
    boolean onRemoteChannel(DataLink link);

    /** A link is open and can carry messages. */
    void onChannelOpen(DataLink link);

    /**
     * A binary message arrived.
     *
     * @param link the link it arrived on
     * @param message the message, valid only during this call
     */
    void onMessage(DataLink link, ByteBuffer message);

    /**
     * A text message arrived: the remote peer sent a string, which WebRTC
     * marks apart from binary data. NetherNet peers send binary messages only.
     *
     * @param link the link it arrived on
     * @param message the message's bytes as they arrived, not checked to be
     *        UTF-8, valid only during this call
     */
    void onTextMessage(DataLink link, ByteBuffer message);

    /** A link's queued bytes fell to or below its low watermark. */
    void onQueueLow(DataLink link);

    /**
     * A link closed for a reason other than the peer's local close.
     *
     * @param link the link
     * @param cause why: {@link CloseCause#REMOTE_CLOSED} for an orderly close
     *        by the remote peer, {@link CloseCause#REMOTE_ABORTED} for an
     *        abort, or the transport failure that ended it
     */
    void onChannelClosed(DataLink link, CloseCause cause);

    /**
     * The engine selected a candidate pair: initially and on each later
     * change.
     *
     * @param path the selected pair
     */
    void onPathSelected(TransportPath path);

    /** The peer's overall state changed. */
    void onStateChange(PeerState state);

    /** The ICE connection state changed. */
    void onIceStateChange(IceState state);

    /**
     * The peer ended for a reason other than a local {@link Peer#close()}.
     * Fires at most once. The peer still needs a local close to release it.
     *
     * @param cause why it ended
     */
    void onClosed(CloseCause cause);
}

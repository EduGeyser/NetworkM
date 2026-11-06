package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.io.IOException;
import java.util.Objects;

/**
 * Why the engine refused a send: its data link had closed, or the remote peer was closing it,
 * when the engine tried the message. It carries the cause {@link PeerListener#onChannelClosed}
 * reports for that link, so the transport ends the connection with the same reason whichever of
 * the two it learns of first. A send refused after the peer's local close fails with another
 * exception.
 */
public final class DataLinkClosedException extends IOException {
    private final CloseCause closeCause;

    /**
     * @param closeCause why the link closed
     */
    public DataLinkClosedException(CloseCause closeCause) {
        super("The data link closed: " + closeCause);
        this.closeCause = Objects.requireNonNull(closeCause, "closeCause");
    }

    /** Why the link closed, as {@link PeerListener#onChannelClosed} reports it. */
    public CloseCause closeCause() {
        return closeCause;
    }
}

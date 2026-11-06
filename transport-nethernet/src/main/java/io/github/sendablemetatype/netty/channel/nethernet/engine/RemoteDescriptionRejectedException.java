package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.io.Serial;

/**
 * Fails a negotiation stage when the engine refused the remote description: the remote side's
 * offer or answer, not this side, is at fault. The peer then ends with
 * {@link CloseCause#REMOTE_DESCRIPTION_REJECTED}.
 */
public final class RemoteDescriptionRejectedException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    /** @param message what the engine said, without the description itself */
    public RemoteDescriptionRejectedException(String message) {
        super(message);
    }
}

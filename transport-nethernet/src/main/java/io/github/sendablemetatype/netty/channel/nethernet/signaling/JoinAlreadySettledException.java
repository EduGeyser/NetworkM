package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.util.concurrent.CompletionException;

/**
 * Fails a reply that came after its join was settled: the join already got its answer or a
 * refusal, its source gave up on it, or its client left. The reply reached nobody. Whoever
 * settled the join reported it, so a refusal that fails with this is not counted again.
 *
 * @see ReplyPath#refuse
 */
public final class JoinAlreadySettledException extends IllegalStateException {

    /** @param message what settled the join */
    public JoinAlreadySettledException(String message) {
        super(message);
    }

    /**
     * Whether a reply failed because its join was settled before it.
     *
     * @param failure what a reply's stage completed with, null when it succeeded
     * @return true when the failure is this exception, also wrapped in a {@link CompletionException}
     */
    public static boolean settledBefore(Throwable failure) {
        Throwable cause = failure instanceof CompletionException wrapped ? wrapped.getCause() : failure;
        return cause instanceof JoinAlreadySettledException;
    }
}

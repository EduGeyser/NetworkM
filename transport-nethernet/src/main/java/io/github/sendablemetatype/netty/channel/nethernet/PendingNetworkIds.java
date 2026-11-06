package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.RemoteSignal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.ReplyPath;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * The network IDs of one endpoint's pending joins from sources that take one join per network ID
 * ({@link JoinSource#oneJoinPerNetworkId()}), shared by all of them. A join holds its ID until its
 * answer is handed to its source or it ends without one, also after its source was removed: a
 * source added in its place must not admit a second join for the same client meanwhile. Safe to
 * use from any thread.
 */
final class PendingNetworkIds {
    private final ConcurrentMap<String, Hold> held = new ConcurrentHashMap<>();

    /**
     * Holds the join's network ID for it.
     *
     * @param join a join from a source that takes one join per network ID
     * @return the join with a reply path that gives the ID back, or null when another pending join
     *         holds it, in which case the join is a {@link JoinRefusal#DUPLICATE}
     */
    Join hold(Join join) {
        Hold hold = new Hold(join.remoteNetworkId(), join.reply());
        if (held.putIfAbsent(hold.networkId, hold) != null) {
            return null;
        }
        return new Join(join.connectionId(), join.remoteNetworkId(), join.offer(), join.source(), hold,
                join.addressed(), join.authenticated());
    }

    /** A held join's reply path: the source's own, plus giving the ID back. */
    private final class Hold implements ReplyPath {
        private final String networkId;
        private final ReplyPath reply;

        private Hold(String networkId, ReplyPath reply) {
            this.networkId = networkId;
            this.reply = reply;
        }

        @Override
        public boolean trickles() {
            return reply.trickles();
        }

        /**
         * Gives the ID back as the answer goes out, as upstream does: from here the client has what
         * it asked for, and an offer it sends later for the same ID is a new connect, not a
         * duplicate, even if this answer never arrives.
         */
        @Override
        public CompletionStage<Void> answer(String sdp) {
            release();
            return reply.answer(sdp);
        }

        @Override
        public CompletionStage<Void> candidate(String candidate) {
            return reply.candidate(candidate);
        }

        @Override
        public CompletionStage<Void> refuse(JoinRefusal refusal) {
            return reply.refuse(refusal);
        }

        @Override
        public void onRemoteSignal(Consumer<RemoteSignal> handler) {
            reply.onRemoteSignal(handler);
        }

        @Override
        public CompletionStage<Void> abandoned() {
            return reply.abandoned();
        }

        @Override
        public void connected() {
            reply.connected();
        }

        /** Every join ends here, with or without an answer: refused, withdrawn, abandoned or closed. */
        @Override
        public void finished() {
            release();
            reply.finished();
        }

        /**
         * Gives the ID back, on the answer ({@link #answer}) or at the end of a join that never got
         * one ({@link #finished}). This class is the one place that decides when that happens. The
         * first call frees the ID; later calls do nothing, also when another join holds the ID by
         * then.
         */
        private void release() {
            held.remove(networkId, this);
        }
    }
}

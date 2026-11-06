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
 * ({@link JoinSource#oneJoinPerNetworkId()}), shared by all of them. A join holds its ID until it
 * ends, also after its source was removed: its child lives on, and a source added in its place
 * must not admit a second join for the same client meanwhile. Safe to use from any thread.
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

        @Override
        public CompletionStage<Void> answer(String sdp) {
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
            release();
            reply.connected();
        }

        @Override
        public void finished() {
            release();
            reply.finished();
        }

        /**
         * Gives the ID back, once the join went active ({@link #connected}) or failed
         * ({@link #finished}). This class is the one place that decides when that happens. Any
         * number of times.
         */
        private void release() {
            held.remove(networkId, this);
        }
    }
}

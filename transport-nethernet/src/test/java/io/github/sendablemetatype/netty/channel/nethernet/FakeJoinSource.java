package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSink;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.RemoteSignal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.ReplyPath;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingHealth;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingState;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** A join source the test drives: it delivers the offers it is given and records every reply. */
final class FakeJoinSource implements JoinSource {
    /** A client somewhere on the internet. */
    static final InetSocketAddress CLIENT = new InetSocketAddress("198.51.100.9", 50000);

    private final boolean trickles;
    volatile JoinSink sink;
    volatile boolean closed;
    volatile List<IceServer> iceServers = List.of();
    final AtomicInteger starts = new AtomicInteger();

    /** @param trickles whether replies trickle, as Xbox and LAN do, or carry one full answer, as HTTP does */
    FakeJoinSource(boolean trickles) {
        this.trickles = trickles;
    }

    @Override
    public void start(JoinSink sink) {
        this.sink = sink;
        starts.incrementAndGet();
    }

    @Override
    public SignalingHealth health() {
        return new SignalingHealth(closed ? SignalingState.CLOSED : SignalingState.CONNECTED, 0, null, null);
    }

    @Override
    public void addHealthListener(Consumer<SignalingHealth> listener) {
    }

    @Override
    public void removeHealthListener(Consumer<SignalingHealth> listener) {
    }

    @Override
    public List<IceServer> iceServers() {
        return iceServers;
    }

    @Override
    public void close() {
        closed = true;
    }

    /** Delivers an offer from {@link #CLIENT}. */
    Reply offer(long connectionId, String offer) {
        return offer(connectionId, offer, new SourcedAddress(CLIENT, AddressSource.SIGNALING, null), null);
    }

    Reply offer(long connectionId, String offer, SourcedAddress source, Join.Addressed addressed) {
        Reply reply = new Reply(trickles);
        sink.join(new Join(connectionId, "remote-" + connectionId, offer, source, reply, addressed));
        return reply;
    }

    /** A reply path that records what the endpoint sends. */
    static final class Reply implements ReplyPath {
        private final boolean trickles;
        private final LinkedBlockingQueue<String> sent = new LinkedBlockingQueue<>();
        private final List<RemoteSignal> early = new ArrayList<>();
        private Consumer<RemoteSignal> handler;
        final CompletableFuture<Void> abandoned = new CompletableFuture<>();
        final AtomicInteger finished = new AtomicInteger();

        Reply(boolean trickles) {
            this.trickles = trickles;
        }

        @Override
        public boolean trickles() {
            return trickles;
        }

        @Override
        public CompletionStage<Void> answer(String sdp) {
            sent.add("answer " + sdp);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> candidate(String candidate) {
            if (!trickles) {
                throw new UnsupportedOperationException("This path does not trickle");
            }
            sent.add("candidate " + candidate);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> refuse(JoinRefusal refusal) {
            sent.add("refuse " + refusal);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public synchronized void onRemoteSignal(Consumer<RemoteSignal> handler) {
            this.handler = handler;
            early.forEach(handler);
            early.clear();
        }

        @Override
        public CompletionStage<Void> abandoned() {
            return abandoned;
        }

        @Override
        public void finished() {
            finished.incrementAndGet();
        }

        /** The client sends a signal for this join. */
        synchronized void signal(RemoteSignal signal) {
            if (handler != null) {
                handler.accept(signal);
            } else {
                early.add(signal);
            }
        }

        /** The next thing the endpoint sent, waiting a little for it; null if nothing came. */
        String next() throws InterruptedException {
            return sent.poll(2, TimeUnit.SECONDS);
        }

        /** Whatever was sent so far, without waiting. */
        List<String> drain() {
            List<String> drained = new ArrayList<>();
            sent.drainTo(drained);
            return drained;
        }

        /** The answer the endpoint sent next. */
        String answer() throws InterruptedException {
            String next = next();
            assertNotNull(next, "no answer was sent");
            assertEquals("answer ", next.substring(0, Math.min(7, next.length())), next);
            return next.substring("answer ".length());
        }

        /** Asserts the next thing sent is this refusal. */
        void assertRefused(JoinRefusal refusal) throws InterruptedException {
            assertEquals("refuse " + refusal, next());
        }
    }
}

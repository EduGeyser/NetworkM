package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressChangedEvent;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer.InFlightSends;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.github.sendablemetatype.netty.channel.nethernet.ActiveChild.payload;
import static org.junit.jupiter.api.Assertions.*;

/**
 * One reason, on the event loop, just before the close, only after activation
 * and only when the transport ends the connection. The channel stays active
 * until it closes, so the close passes through channelInactive.
 */
class NetherNetDisconnectReasonTest {

    @ParameterizedTest
    @CsvSource({
            "REMOTE_CLOSED, CLOSED_BY_REMOTE_PEER",
            "REMOTE_ABORTED, ABORTED_BY_REMOTE_PEER",
            "ICE_FAILED, CONNECTION_LOST",
            "TRANSPORT_FAILED, TRANSPORT_FAILED",
            "UNKNOWN, CLOSED_BY_REMOTE_PEER"})
    void theReliableChannelClosingEndsTheConnection(CloseCause cause, TransportDisconnectReason reason) {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.linkClosed(h.reliable(), cause);
            assertTrue(h.channel.isActive(), "active until the close runs on the loop");
            h.run();
            assertEquals(List.of("active", reason.name(), "inactive"), h.events);
            assertFalse(h.channel.isOpen());
            assertEquals(1, h.peer.closeCalls());
        }
    }

    @ParameterizedTest
    @CsvSource({
            "ICE_FAILED, CONNECTION_LOST",
            "TRANSPORT_FAILED, TRANSPORT_FAILED",
            "REMOTE_CLOSED, CLOSED_BY_REMOTE_PEER",
            "UNKNOWN, TRANSPORT_FAILED"})
    void thePeerEndingEndsTheConnection(CloseCause cause, TransportDisconnectReason reason) {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.closed(cause);
            h.run();
            assertEquals(List.of("active", reason.name(), "inactive"), h.events);
        }
    }

    /**
     * A client quits while a write is in flight, and the engine fails that write before it reports
     * why the reliable channel closed: libwebrtc can report them in that order.
     */
    @ParameterizedTest
    @CsvSource({"REMOTE_ABORTED, ABORTED_BY_REMOTE_PEER", "REMOTE_CLOSED, CLOSED_BY_REMOTE_PEER"})
    void aWriteFailedByTheQuitBeforeTheCloseKeepsTheQuitsReason(CloseCause cause, TransportDisconnectReason reason) {
        try (ActiveChild h = new ActiveChild()) {
            h.channel.writeAndFlush(payload(1));
            h.peer.linkClosed(h.reliable(), cause);
            h.run();
            assertEquals(List.of("active", reason.name(), "inactive"), h.events);
        }
    }

    /**
     * A quit as the captures show it: the client aborts, both data channels close (the unreliable
     * one first) and ICE disconnects, while a write is in flight on one of the links. The engine
     * fails the write before or after the link's close event; the reason is the quit's either way.
     */
    @ParameterizedTest
    @CsvSource({
            "true, FAIL_BEFORE_THE_EVENT", "true, FAIL_AFTER_THE_EVENT",
            "false, FAIL_BEFORE_THE_EVENT", "false, FAIL_AFTER_THE_EVENT"})
    void aQuitWithAWriteInFlightEndsAsAbortedByTheClientInEveryOrder(boolean reliableWrite, InFlightSends order) {
        try (ActiveChild h = new ActiveChild()) {
            h.channel.writeAndFlush(reliableWrite ? payload(1) : new NetherNetPacket(payload(1), false));
            h.peer.linkClosed(h.unreliable(), CloseCause.REMOTE_ABORTED, order);
            h.peer.linkClosed(h.reliable(), CloseCause.REMOTE_ABORTED, order);
            h.peer.iceState(IceState.DISCONNECTED);
            h.peer.state(PeerState.DISCONNECTED);
            h.run();
            assertEquals(List.of("active", "ABORTED_BY_REMOTE_PEER", "inactive"), h.events);
        }
    }

    /** The first ending wins: a peer failing after the quit, before the loop closed, changes nothing. */
    @Test
    void aPeerFailingAfterTheQuitKeepsTheQuitsReason() {
        try (ActiveChild h = new ActiveChild()) {
            h.channel.writeAndFlush(payload(1));
            h.peer.linkClosed(h.reliable(), CloseCause.REMOTE_ABORTED);
            h.peer.closed(CloseCause.ICE_FAILED);
            h.run();
            assertEquals(List.of("active", "ABORTED_BY_REMOTE_PEER", "inactive"), h.events);
        }
    }

    @Test
    void theUnreliableChannelClosingLeavesTheConnectionUp() {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.linkClosed(h.unreliable(), CloseCause.REMOTE_CLOSED);
            h.run();
            assertTrue(h.channel.isActive());
            assertEquals(List.of("active"), h.events);
        }
    }

    /** The connection carries on over the reliable channel; only the unreliable write fails. */
    @Test
    void aWriteOnTheClosedUnreliableChannelFailsOnlyThatWrite() {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.linkClosed(h.unreliable(), CloseCause.REMOTE_CLOSED);
            ChannelFuture write = h.channel.writeAndFlush(new NetherNetPacket(payload(1), false));
            h.run();
            assertTrue(write.isDone() && !write.isSuccess(), "the unreliable write must fail");
            assertTrue(h.channel.isActive());
            assertEquals(List.of("active"), h.events);
        }
    }

    @Test
    void aHandlerThatWritesOnTheReasonDoesNotFireItAgain() {
        try (ActiveChild h = new ActiveChild(child -> child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                if (event instanceof TransportDisconnectReason) {
                    ctx.channel().writeAndFlush(payload(1));
                }
                ctx.fireUserEventTriggered(event);
            }
        }))) {
            h.peer.closed(CloseCause.ICE_FAILED);
            h.run();
            assertEquals(List.of("active", "CONNECTION_LOST", "inactive"), h.events);
            assertEquals(0, h.reliable().sendCount());
        }
    }

    @Test
    void closingFromTheApplicationFiresNoReason() {
        try (ActiveChild h = new ActiveChild()) {
            h.channel.close().syncUninterruptibly();
            h.run();
            assertEquals(List.of("active", "inactive"), h.events);
        }
    }

    @Test
    void aPeerEndingBeforeActivationFiresNoReasonAndReportsTheFailure() {
        EmbeddedChannel clock = new EmbeddedChannel();
        List<NetherConnectionFailure> failures = new ArrayList<>();
        ChildFixture fixture = new ChildFixture(null);
        fixture.child.config().setOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() {
            @Override
            public void connectionFailed(NetherConnectionFailure reason) {
                failures.add(reason);
            }
        });
        List<Object> events = new ArrayList<>();
        fixture.child.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(io.netty.channel.ChannelHandlerContext ctx, Object event) {
                events.add(event);
            }
        });
        try {
            clock.eventLoop().register(fixture.child).syncUninterruptibly();
            fixture.peer.closed(CloseCause.ICE_FAILED);
            clock.runPendingTasks();
            assertFalse(fixture.child.isOpen());
            assertTrue(events.isEmpty());
            assertEquals(List.of(NetherConnectionFailure.PEER_FAILED), failures);
        } finally {
            clock.finishAndReleaseAll();
        }
    }

    @Test
    void aRefusedSendFiresTransportFailedBeforeTheWriteFails() {
        try (ActiveChild h = new ActiveChild()) {
            ChannelFuture write = h.channel.writeAndFlush(payload(1));
            write.addListener(future -> h.events.add("write failed"));
            h.reliable().reject(0, new IOException("refused"));
            h.run();
            assertEquals(List.of("active", "TRANSPORT_FAILED", "write failed", "inactive"), h.events);
        }
    }

    @Test
    void brokenFramingFiresBadPacket() {
        try (ActiveChild h = new ActiveChild()) {
            h.fixture.reliable(2, 1);
            h.fixture.reliable(0, 2);
            h.fixture.reliable(0, 3);
            h.run();
            assertEquals(List.of("active", "BAD_PACKET", "inactive"), h.events);
        }
    }

    @Test
    void nothingArrivingAfterBrokenFramingIsDelivered() {
        try (ActiveChild h = new ActiveChild()) {
            h.fixture.reliable(0, 1);
            h.fixture.reliable(0);
            h.fixture.unreliable(0, 9);
            h.run();
            assertEquals(List.of("active", "read:1", "BAD_PACKET", "inactive"), h.events);
        }
    }

    @Test
    void aFailedCopyEndsTheConnectionRatherThanSplittingTheMessage() {
        FailingAllocator allocator = new FailingAllocator();
        try (ActiveChild h = new ActiveChild(child -> child.config().setAllocator(allocator))) {
            h.fixture.reliable(2, 1);
            allocator.fail = true;
            assertThrows(IllegalStateException.class, () -> h.fixture.reliable(1, 2));
            allocator.fail = false;
            h.fixture.reliable(0, 3);
            h.run();
            assertEquals(List.of("active", "TRANSPORT_FAILED", "inactive"), h.events);
        }
    }

    @Test
    void aMessageOverTheReassemblyCapFiresSplitQueueTooLong() {
        try (ActiveChild h = new ActiveChild(child ->
                child.config().setOption(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, 2))) {
            h.fixture.reliable(1, 1, 2);
            h.fixture.reliable(0, 3);
            h.run();
            assertEquals(List.of("active", "SPLIT_QUEUE_TOO_LONG", "inactive"), h.events);
        }
    }

    @Test
    void anInboundQueueOverflowFiresQueueTooLong() {
        try (ActiveChild h = new ActiveChild(child -> {
            child.config().setAutoRead(false);
            child.config().setOption(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, 2);
        })) {
            h.fixture.reliable(0, 1, 2);
            h.fixture.reliable(0, 3, 4);
            h.fixture.reliable(0, 5);
            h.run();
            assertEquals(List.of("active", "QUEUE_TOO_LONG", "inactive"), h.events);
        }
    }

    @Test
    void terminationWaitsForThePeersTeardown() {
        try (ActiveChild h = new ActiveChild()) {
            h.fixture.engine.holdPeerClose(true);
            CompletionStage<Void> termination = h.channel.termination();
            h.channel.close().syncUninterruptibly();
            assertTrue(h.channel.closeFuture().isDone());
            assertFalse(termination.toCompletableFuture().isDone());
            h.peer.finishClose();
            assertTrue(termination.toCompletableFuture().isDone());
        }
    }

    @Test
    void theRemoteAddressFollowsEachSelectedPairOnTheEventLoop() {
        try (ActiveChild h = new ActiveChild()) {
            InetSocketAddress host = new InetSocketAddress("192.0.2.8", 32000);
            InetSocketAddress relay = new InetSocketAddress("198.51.100.4", 3478);
            h.peer.path(new TransportPath(null, TransportPath.CandidateType.HOST, host,
                    TransportPath.CandidateType.SERVER_REFLEXIVE));
            assertEquals(ChildFixture.REMOTE, h.channel.remoteAddress(), "not written from the engine thread");
            h.run();
            assertEquals(host, h.channel.remoteAddress());
            // A relayed pair reports the TURN relay, the transport peer actually connected to us.
            h.peer.path(new TransportPath(null, TransportPath.CandidateType.HOST, relay,
                    TransportPath.CandidateType.RELAY));
            h.run();
            assertEquals(relay, h.channel.remoteAddress());
        }
    }

    @Test
    void metricsSeeEngineStatesAndThePathTypes() {
        List<String> seen = new ArrayList<>();
        try (ActiveChild h = new ActiveChild(child -> child.config().setOption(NetherChannelOption.NETHER_METRICS,
                new NetherChannelMetrics() {
                    @Override
                    public void peerStateChange(PeerState state) {
                        seen.add("peer " + state);
                    }

                    @Override
                    public void iceStateChange(IceState state) {
                        seen.add("ice " + state);
                    }

                    @Override
                    public void pathSelected(TransportPath.CandidateType local, TransportPath.CandidateType remote) {
                        seen.add("path " + local + " " + remote);
                    }

                    @Override
                    public void messagesIn(int count) {
                        throw new IllegalStateException("A failing metric must not lose the message");
                    }
                }))) {
            h.peer.iceState(IceState.CONNECTED);
            h.peer.path(new TransportPath(null, TransportPath.CandidateType.HOST,
                    new InetSocketAddress("192.0.2.8", 32000), TransportPath.CandidateType.PEER_REFLEXIVE));
            h.peer.state(PeerState.CONNECTED);
            h.fixture.reliable(0, 9);
            h.run();
            assertEquals(List.of("ice CONNECTED", "peer CONNECTED", "path HOST PEER_REFLEXIVE"), seen);
            assertEquals(List.of("active", "read:9"), h.events.stream()
                    .filter(event -> !event.startsWith(RemoteAddressChangedEvent.class.getSimpleName())).toList());
        }
    }

    @Test
    void aMetricThatThrowsAnErrorDoesNotLeakTheMessage() {
        List<ByteBuf> allocated = new ArrayList<>();
        AbstractByteBufAllocator recording = new AbstractByteBufAllocator() {
            @Override
            protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
                ByteBuf buffer = Unpooled.buffer(initialCapacity, maxCapacity);
                allocated.add(buffer);
                return buffer;
            }

            @Override
            protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
                ByteBuf buffer = Unpooled.directBuffer(initialCapacity, maxCapacity);
                allocated.add(buffer);
                return buffer;
            }

            @Override
            public boolean isDirectBufferPooled() {
                return false;
            }
        };
        try (ActiveChild h = new ActiveChild(child -> {
            child.config().setAllocator(recording);
            child.config().setOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() {
                @Override
                public void messagesIn(int count) {
                    throw new AssertionError("a broken metric");
                }
            });
        })) {
            assertThrows(AssertionError.class, () -> h.fixture.reliable(0, 9));
            assertFalse(allocated.isEmpty());
            allocated.forEach(buffer -> assertEquals(0, buffer.refCnt(), "the message leaked"));
        }
    }

    /** Hands out unpooled buffers until told to fail. */
    private static final class FailingAllocator extends AbstractByteBufAllocator {
        volatile boolean fail;

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            check();
            return Unpooled.buffer(initialCapacity, maxCapacity);
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            check();
            return Unpooled.directBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }

        private void check() {
            if (fail) {
                throw new IllegalStateException("No buffer");
            }
        }
    }
}

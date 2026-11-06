package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.netty.channel.ChannelFuture;
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

    @Test
    void theUnreliableChannelClosingLeavesTheConnectionUp() {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.linkClosed(h.unreliable(), CloseCause.REMOTE_CLOSED);
            h.run();
            assertTrue(h.channel.isActive());
            assertEquals(List.of("active"), h.events);
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
            assertEquals(List.of("active", "read:9"), h.events);
        }
    }
}

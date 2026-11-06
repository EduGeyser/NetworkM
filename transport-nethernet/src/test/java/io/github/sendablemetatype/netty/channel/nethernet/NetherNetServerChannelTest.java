package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EndpointConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeIceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.RemoteDescriptionRejectedException;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.RemoteSignal;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.ANSWER;
import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.OFFER;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class NetherNetServerChannelTest {
    private final EndpointHarness harness = new EndpointHarness();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    // Binding and engines.

    @Test
    void bindingWithoutAServerIdentityFailsAndCreatesNoEngine() {
        AtomicInteger creations = new AtomicInteger();
        ChannelFuture bind = owned(() -> {
            creations.incrementAndGet();
            return new FakeEngine();
        }).option(NetherChannelOption.NETHER_SERVER_IDENTITY, null).bind(new InetSocketAddress("127.0.0.1", 0))
                .awaitUninterruptibly();

        assertInstanceOf(IllegalStateException.class, bind.cause());
        assertTrue(bind.cause().getMessage().contains("NETHER_SERVER_IDENTITY"));
        assertEquals(0, creations.get());
        assertFalse(bind.channel().isOpen(), "a failed bind closes the channel");
    }

    @Test
    void anOwnedEngineIsCreatedOnceAtBindAndClosedWithTheChannel() {
        AtomicInteger creations = new AtomicInteger();
        FakeEngine engine = new FakeEngine();
        Channel server = owned(() -> {
            creations.incrementAndGet();
            return engine;
        }).bind(new InetSocketAddress("127.0.0.1", 0)).syncUninterruptibly().channel();
        assertEquals(1, creations.get());
        assertFalse(engine.isClosed());

        server.close().syncUninterruptibly();
        assertEquals(1, engine.closeCalls());
    }

    @Test
    void aFailingEngineSupplierFailsTheBind() {
        UnsatisfiedLinkError missing = new UnsatisfiedLinkError("native library unavailable");
        ChannelFuture bind = owned(() -> {
            throw missing;
        }).bind(new InetSocketAddress("127.0.0.1", 0)).awaitUninterruptibly();
        assertSame(missing, bind.cause());
    }

    @Test
    void aSharedEngineIsNeverClosedAndServesASecondBind() throws Exception {
        for (int bind = 0; bind < 2; bind++) {
            NetherNetServerChannel server = harness.bind();
            harness.source(true).offer(bind + 1, OFFER);
            harness.child();
            server.close().syncUninterruptibly();
        }
        assertEquals(2, harness.engine.peers().size());
        assertEquals(0, harness.engine.closeCalls());
        harness.server = null;
    }

    @Test
    void bindingDoesNotWaitForTheIcePortToOpen() throws Exception {
        HeldEndpointEngine engine = new HeldEndpointEngine();
        NetherNetServerChannel server = (NetherNetServerChannel) bootstrap(EngineRef.shared(engine))
                .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        try {
            FakeJoinSource source = new FakeJoinSource(true);
            server.addSource(source, EndpointHarness.ADMIT_ALL);
            harness.sync();
            assertTrue(server.isActive());
            assertEquals(0, source.starts.get(), "sources start once the port is open");

            engine.open.complete(null);
            harness.sync();
            assertEquals(1, source.starts.get());
            source.offer(1, OFFER);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (engine.fake.peers().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertSame(engine.opened.getFirst(), engine.fake.peer(0).config().endpoint());
        } finally {
            server.close().sync();
        }
    }

    @Test
    void aTakenIcePortFailsTheBind() throws Exception {
        try (DatagramSocket taken = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0))) {
            ChannelFuture bind = harness.bootstrap.bind(new InetSocketAddress("127.0.0.1", taken.getLocalPort()))
                    .awaitUninterruptibly();
            assertInstanceOf(BindException.class, bind.cause());
            assertFalse(bind.channel().isOpen());
        }
    }

    @Test
    void anIcePortThatFailsToOpenAfterBindClosesTheEndpoint() throws Exception {
        HeldEndpointEngine engine = new HeldEndpointEngine();
        Channel server = bootstrap(EngineRef.shared(engine)).bind(new InetSocketAddress("127.0.0.1", 0)).sync()
                .channel();

        engine.open.completeExceptionally(new BindException("taken after the probe"));

        assertTrue(server.closeFuture().await(2, TimeUnit.SECONDS));
        assertEquals(0, engine.fake.closeCalls(), "a shared engine stays with its owner");
    }

    @Test
    void anIcePortThatOpensAfterTheEndpointClosedIsClosed() throws Exception {
        HeldEndpointEngine engine = new HeldEndpointEngine();
        ChannelFuture bind = bootstrap(EngineRef.shared(engine)).bind(new InetSocketAddress("127.0.0.1", 0))
                .awaitUninterruptibly();
        bind.channel().close().sync();

        engine.open.complete(null);
        harness.sync();

        assertEquals(1, engine.opened.size());
        assertTrue(engine.opened.getFirst().closed(), "the endpoint owned the port while it was opening");
    }

    @Test
    void peersShareOneIcePortAtTheBoundAddressByDefault() throws Exception {
        NetherNetServerChannel server = harness.bind();
        harness.source(true).offer(1, OFFER);
        harness.source(false).offer(2, OFFER);
        harness.negotiating();
        harness.sync();

        int port = ((InetSocketAddress) server.localAddress()).getPort();
        assertNotEquals(0, port, "port 0 picks a free port");
        for (FakePeer peer : harness.engine.peers()) {
            assertNotNull(peer.config().endpoint());
            assertEquals(port, peer.config().endpoint().port());
        }
        assertSame(harness.engine.peer(0).config().endpoint(), harness.engine.peer(1).config().endpoint());
    }

    @Test
    void portsPerSessionAreAnOption() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT, false);
        harness.bind();
        harness.source(true).offer(1, OFFER);
        assertNull(harness.negotiating().config().endpoint());
    }

    // Sources.

    @Test
    void severalSourcesFeedOneEndpointAndEachJoinRepliesOnItsOwnPath() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(ANSWER + "a=candidate:1 1 udp 2122260223 192.0.2.5 5000 typ host\r\n"
                + "a=end-of-candidates\r\n");
        NetherNetServerChannel server = (NetherNetServerChannel) harness.bootstrap.register().sync().channel();
        harness.server = server;
        FakeJoinSource xbox = new FakeJoinSource(true);
        server.addSource(xbox, EndpointHarness.ADMIT_ALL);
        assertEquals(0, xbox.starts.get(), "a source starts once the endpoint is bound");
        server.bind(new InetSocketAddress("127.0.0.1", 0)).sync();
        assertEquals(1, xbox.starts.get());
        FakeJoinSource http = harness.source(false);
        assertEquals(1, http.starts.get(), "a bound endpoint starts a source at once");
        assertEquals(List.of(xbox, http), server.sources());

        FakeJoinSource.Reply trickled = xbox.offer(1, OFFER);
        FakeJoinSource.Reply full = http.offer(2, OFFER);

        String trickledAnswer = trickled.answer();
        assertTrue(trickledAnswer.contains("a=identity:"));
        assertFalse(trickledAnswer.contains("a=candidate"), "candidates trickle after the answer");
        String fullAnswer = full.answer();
        assertTrue(fullAnswer.contains("a=identity:"));
        assertTrue(fullAnswer.contains("a=candidate:1 1 udp 2122260223 192.0.2.5 5000 typ host\r\n"));
        assertTrue(fullAnswer.endsWith("a=end-of-candidates\r\n"));
        List<String> ids = List.of(harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get(),
                harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get());
        assertTrue(ids.containsAll(List.of("1", "2")));
    }

    @Test
    void addingTheSameSourceTwiceFails() throws Exception {
        harness.bind();
        FakeJoinSource source = harness.source(true);
        assertThrows(IllegalStateException.class, () -> harness.server.addSource(source));
    }

    @Test
    void removingASourceClosesItButLeavesItsPlayersConnected() throws Exception {
        NetherNetServerChannel server = harness.bind();
        FakeJoinSource source = harness.source(true);
        source.offer(1, OFFER);
        FakePeer peer = harness.negotiating();
        peer.completeAnswer(ANSWER);
        NetherNetChildChannel child = harness.child();
        harness.activate(peer);
        assertTrue(child.isActive());

        assertTrue(server.removeSource(source));
        assertTrue(source.closed);
        assertFalse(server.removeSource(source));
        assertEquals(List.of(), server.sources());
        harness.sync();
        assertTrue(child.isActive());
        assertFalse(peer.closeCalled());

        source.offer(2, OFFER).assertRefused(JoinRefusal.CAPACITY);
        assertEquals(1, harness.engine.peers().size());
    }

    // Negotiation.

    @Test
    void theChildIsExposedAtAdmissionAndNegotiatesOnceRegistered() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_ICE_SERVERS,
                List.of(new IceServer(List.of("stun:stun.example"), null, null)));
        harness.bind();
        FakeJoinSource source = harness.source(true);
        source.iceServers = List.of(new IceServer(List.of("turn:relay.example:3478"), "user", "secret"));
        FakeJoinSource.Reply reply = source.offer(3, OFFER);

        NetherNetChildChannel child = harness.child();
        FakePeer peer = harness.negotiating();
        assertEquals(List.of(), reply.drain(), "exposed before any answer");
        assertEquals(PeerConfig.Role.ANSWERER, peer.config().role());
        assertEquals(List.of("stun:stun.example", "turn:relay.example:3478"),
                peer.config().iceServers().stream().flatMap(ice -> ice.urls().stream()).toList());
        assertEquals("3", child.attr(NetherNetChildChannel.CONNECTION_ID).get());
        assertFalse(child.isActive());

        peer.completeAnswer(ANSWER);
        peer.localCandidate("candidate:1 1 udp 1 192.0.2.1 5000 typ host", "data");
        harness.sync();
        assertTrue(reply.answer().contains("a=identity:"));
        assertEquals("candidate candidate:1 1 udp 1 192.0.2.1 5000 typ host", reply.next(),
                "local candidates follow the answer");

        harness.activate(peer);
        assertTrue(child.isActive());
    }

    @Test
    void aFailedAnswerRefusesOnceAndClosesTheChild() throws Exception {
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(4, OFFER);
        FakePeer peer = harness.negotiating();
        peer.failAnswer(new IllegalStateException("SetRemoteDescription failed"));
        peer.closed(CloseCause.NEGOTIATION_FAILED);
        harness.sync();

        reply.assertRefused(JoinRefusal.INTERNAL);
        assertEquals(List.of(), reply.drain());
        assertEquals(1, peer.closeCalls());
        assertEquals(1, reply.finished.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anOfferTheEngineRefusesIsABadOffer(boolean closedFirst) throws Exception {
        List<Integer> refused = new CopyOnWriteArrayList<>();
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_METRICS, new NetherServerMetrics() {
            @Override
            public void joinRefused(int httpStatus) {
                refused.add(httpStatus);
            }
        });
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(4, OFFER);
        FakePeer peer = harness.negotiating();
        if (closedFirst) {
            peer.closed(CloseCause.REMOTE_DESCRIPTION_REJECTED);
        }
        peer.failAnswer(new RemoteDescriptionRejectedException("SetRemoteDescription failed: no data channel"));
        if (!closedFirst) {
            harness.sync();
            peer.closed(CloseCause.REMOTE_DESCRIPTION_REJECTED);
        }
        harness.sync();

        reply.assertRefused(JoinRefusal.BAD_OFFER);
        assertEquals(List.of(), reply.drain());
        assertEquals(List.of(400), refused, "a refusal after the child exists is reported too");
        assertEquals(1, reply.finished.get());
    }

    @Test
    void anAnswerThatCannotBeSignedFailsTheJoin() throws Exception {
        harness.engine.answerWith("v=0\r\n");
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(5, OFFER);
        reply.assertRefused(JoinRefusal.INTERNAL);
        harness.sync();
        assertTrue(harness.engine.lastPeer().closeCalled());
    }

    @Test
    void aPeerTheEngineCannotCreateIsRefusedAndNeverExposed() throws Exception {
        harness.engine.failCreation(new NoClassDefFoundError("native binding unavailable"));
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(6, OFFER);
        reply.assertRefused(JoinRefusal.INTERNAL);
        assertNull(harness.children.poll(100, TimeUnit.MILLISECONDS), "a child without a peer is never exposed");
        harness.sync();
        assertEquals(1, reply.finished.get());
        assertEquals(0, harness.server.gate().sessions());
    }

    @Test
    void anAnswerTheClientNeverGotClosesTheChildButAnActiveChildStays() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(ANSWER);
        harness.bind();
        FakeJoinSource source = harness.source(false);

        FakeJoinSource.Reply lost = source.offer(1, OFFER);
        NetherNetChildChannel undelivered = harness.child();
        lost.answer();
        // The source took the answer but could not write it
        lost.abandoned.complete(null);
        harness.sync();
        assertFalse(undelivered.isOpen());

        FakeJoinSource.Reply delivered = source.offer(2, OFFER);
        NetherNetChildChannel connected = harness.child();
        delivered.answer();
        harness.activate(harness.engine.lastPeer());
        delivered.abandoned.complete(null);
        harness.sync();
        assertTrue(connected.isActive());
    }

    @Test
    void aConnectErrorFromTheClientClosesTheChild() throws Exception {
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(7, OFFER);
        FakePeer peer = harness.negotiating();
        reply.signal(new RemoteSignal.Error(2));
        harness.sync();
        assertTrue(peer.closeCalled());
        assertEquals(List.of(), reply.drain());
    }

    @Test
    void theHandshakeTimeoutClosesTheChildAndReportsIt() throws Exception {
        List<NetherConnectionFailure> failures = new ArrayList<>();
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS, 0)
                .childOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() {
                    @Override
                    public void connectionFailed(NetherConnectionFailure reason) {
                        failures.add(reason);
                    }
                });
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(1, OFFER);
        harness.child();
        harness.sync();
        assertTrue(harness.engine.lastPeer().closeCalled());
        assertEquals(List.of(NetherConnectionFailure.HANDSHAKE_TIMEOUT), failures);
        assertEquals(1, reply.finished.get());
    }

    @Test
    void closingTheEndpointClosesItsSourcesChildrenAndPort() throws Exception {
        List<String> accepted = new ArrayList<>();
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_METRICS, new NetherServerMetrics() {
            @Override
            public void connectionAccepted(String networkId, boolean verifiedIdentity) {
                accepted.add(networkId + " " + verifiedIdentity);
            }
        });
        harness.bind();
        FakeJoinSource source = harness.source(true);
        source.offer(3, OFFER);
        FakePeer peer = harness.negotiating();

        harness.server.close().sync();
        harness.sync();

        assertTrue(source.closed);
        assertTrue(peer.closeCalled());
        assertTrue(((FakeIceEndpoint) peer.config().endpoint()).closed());
        assertEquals(List.of("remote-3 false"), accepted);
        assertEquals(0, harness.engine.closeCalls(), "a shared engine stays with its owner");
        assertThrows(IllegalStateException.class, () -> harness.server.addSource(new FakeJoinSource(true)));
    }

    private ServerBootstrap owned(Supplier<? extends NetherNetEngine> engines) {
        return bootstrap(EngineRef.owned(engines));
    }

    /** An endpoint on the harness's event loop that checks no issuer. */
    private ServerBootstrap bootstrap(EngineRef engine) {
        return new ServerBootstrap()
                .group(harness.group)
                .channelFactory(() -> new NetherNetServerChannel(engine, OfferCheck::newExecutor))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, EndpointHarness.IDENTITY)
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                .childHandler(new ChannelInboundHandlerAdapter());
    }

    /** The fake engine, with shared ICE ports that open only once the test completes {@link #open}. */
    private static final class HeldEndpointEngine implements NetherNetEngine {
        final FakeEngine fake = new FakeEngine();
        final CompletableFuture<Void> open = new CompletableFuture<>();
        final List<FakeIceEndpoint> opened = new CopyOnWriteArrayList<>();

        @Override
        public Peer createPeer(PeerConfig config, PeerListener listener) {
            return fake.createPeer(config, listener);
        }

        @Override
        public CompletionStage<IceEndpoint> openEndpoint(EndpointConfig config) {
            return open.thenCompose(ignored -> fake.openEndpoint(config)).thenApply(endpoint -> {
                opened.add((FakeIceEndpoint) endpoint);
                return endpoint;
            });
        }

        @Override
        public CompletionStage<Void> close() {
            return fake.close();
        }
    }
}

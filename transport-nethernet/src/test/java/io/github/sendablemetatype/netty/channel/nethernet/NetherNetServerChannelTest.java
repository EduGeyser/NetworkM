package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetServerSignaling;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class NetherNetServerChannelTest {
    private static final String OFFER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:data\r\n"
            + "a=max-message-size:262144\r\n";

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    @AfterEach
    void tearDown() {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aFailedSignalingBindNeverCreatesAnEngine() throws Exception {
        AtomicInteger creations = new AtomicInteger();
        StubSignaling signaling = new StubSignaling(true);

        ChannelFuture future = owned(() -> {
            creations.incrementAndGet();
            return new FakeEngine();
        }, signaling).bind(new InetSocketAddress(0));

        assertThrows(Exception.class, future::sync);
        assertEquals(0, creations.get(), "no engine when the signaling bind fails");
        future.channel().close().syncUninterruptibly();
        assertTrue(signaling.closed);
    }

    @Test
    void anOwnedEngineIsCreatedOnceAfterBindAndClosedWithTheChannel() throws Exception {
        AtomicInteger creations = new AtomicInteger();
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);

        ChannelFuture future = owned(() -> {
            creations.incrementAndGet();
            assertTrue(signaling.bound, "signaling binds first");
            return engine;
        }, signaling).bind(new InetSocketAddress(0)).sync();
        assertEquals(1, creations.get());
        assertFalse(engine.isClosed());

        future.channel().close().syncUninterruptibly();
        assertEquals(1, engine.closeCalls());
        assertTrue(signaling.closed);
    }

    @Test
    void aFailingEngineSupplierFailsTheBindAndClosesSignaling() {
        StubSignaling signaling = new StubSignaling(false);
        UnsatisfiedLinkError missing = new UnsatisfiedLinkError("native library unavailable");
        ChannelFuture future = owned(() -> {
            throw missing;
        }, signaling).bind(new InetSocketAddress(0)).awaitUninterruptibly();
        assertSame(missing, future.cause());
        assertTrue(signaling.closed);
    }

    @Test
    void aSharedEngineIsNeverClosedAndServesASecondBind() throws Exception {
        FakeEngine engine = new FakeEngine();
        for (int bind = 0; bind < 2; bind++) {
            StubSignaling signaling = new StubSignaling(false);
            Channel server = shared(engine, signaling).bind(new InetSocketAddress(0)).sync().channel();
            ((NetherNetServerChannel) server).acceptConnection(bind + 1, OFFER, "peer");
            server.eventLoop().submit(() -> { }).sync();
            server.close().syncUninterruptibly();
        }
        assertEquals(2, engine.peers().size());
        assertEquals(0, engine.closeCalls());
    }

    @Test
    void anInvalidOfferLimitSignalsFailureBeforeCreatingAPeer() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        NetherNetServerChannel server = bindShared(engine, signaling);
        try {
            for (boolean failSend : new boolean[]{false, true}) {
                signaling.failSend = failSend;
                server.acceptConnection(42, "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                        + "a=max-message-size:1\r\n", "7");
                server.eventLoop().submit(() -> { }).sync();
                assertEquals(new Sent("7", "CONNECTERROR 42 Failed to establish connection"),
                        signaling.sent.poll(2, TimeUnit.SECONDS));
                assertTrue(signaling.handlers.isEmpty());
                assertTrue(engine.peers().isEmpty());
                assertTrue(server.isOpen());
            }
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void theChildIsExposedAtAcceptAndNegotiatesOnceRegistered() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        signaling.iceServers = List.of(new NetherNetServerSignaling.IceServerInfo("user", "secret",
                List.of("turn:relay.example:3478")));
        CountDownLatch registered = new CountDownLatch(1);
        AtomicReference<Channel> accepted = new AtomicReference<>();
        List<Integer> signalsAtRegistration = new ArrayList<>();
        NetherNetServerChannel server = (NetherNetServerChannel) new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine, signaling))
                .option(NetherChannelOption.NETHER_SERVER_ANSWER_DECORATOR, answer -> answer + "a=decorated\r\n")
                .option(NetherChannelOption.NETHER_ICE_SERVERS, List.of(new IceServer(List.of("stun:stun.example"), null, null)))
                .childHandler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRegistered(ChannelHandlerContext ctx) {
                        accepted.set(ctx.channel());
                        signalsAtRegistration.add(signaling.sent.size());
                        registered.countDown();
                        ctx.fireChannelRegistered();
                    }
                })
                .bind(new InetSocketAddress(0)).sync().channel();
        try {
            server.acceptConnection(3, OFFER, "remote");
            assertTrue(registered.await(2, TimeUnit.SECONDS), "exposed before any answer");
            Channel child = accepted.get();
            child.eventLoop().submit(() -> { }).sync();
            FakePeer peer = engine.lastPeer();
            assertEquals(List.of(OFFER), peer.remoteOffers(), "negotiates in its own initializer");
            assertEquals(PeerConfig.Role.ANSWERER, peer.config().role());
            assertEquals(List.of("stun:stun.example", "turn:relay.example:3478"),
                    peer.config().iceServers().stream().flatMap(ice -> ice.urls().stream()).toList());
            assertEquals("3", child.attr(NetherNetChildChannel.CONNECTION_ID).get());
            assertFalse(child.isActive());

            peer.completeAnswer("v=0\r\n");
            peer.localCandidate("candidate:1 1 udp 1 192.0.2.1 5000 typ host", "data");
            child.eventLoop().submit(() -> { }).sync();
            assertEquals(new Sent("remote", "CONNECTRESPONSE 3 v=0\r\na=decorated\r\n"), signaling.sent.poll());
            assertEquals(new Sent("remote", "CANDIDATEADD 3 candidate:1 1 udp 1 192.0.2.1 5000 typ host"),
                    signaling.sent.poll(), "local candidates follow the answer");

            FakeDataLink reliable = peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
            peer.remoteChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
            FakeDataLink extra = peer.remoteChannel(ChannelParameters.reliable("Extra"));
            assertTrue(extra.closed(), "the engine closes what the child refuses");
            peer.open(reliable);
            child.eventLoop().submit(() -> { }).sync();
            assertTrue(child.isActive());
        } finally {
            server.close().syncUninterruptibly();
        }
        assertEquals(List.of(0), signalsAtRegistration, "exposed before the answer went out");
    }

    @Test
    void signalsBeforeThePeerExistsWaitInABoundedList() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        NetherNetServerChannel server = bindShared(engine, signaling);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            server.eventLoop().execute(() -> {
                blocked.countDown();
                awaitQuietly(release);
            });
            assertTrue(blocked.await(2, TimeUnit.SECONDS));
            server.acceptConnection(9, OFFER, "remote");
            for (int i = 0; i < NetherNetServerChannel.MAX_EARLY_SIGNALS + 6; i++) {
                signaling.handlers.get(9L).onSignal("CANDIDATEADD 9 candidate:" + i);
            }
            release.countDown();
            server.eventLoop().submit(() -> { }).sync();

            List<FakePeer.RemoteCandidate> applied = engine.lastPeer().remoteCandidates();
            assertEquals(NetherNetServerChannel.MAX_EARLY_SIGNALS, applied.size());
            assertEquals(new FakePeer.RemoteCandidate("candidate:0", "data"), applied.getFirst());
            assertEquals("candidate:63", applied.getLast().candidate());

            signaling.handlers.get(9L).onSignal("CANDIDATEADD 9 candidate:late");
            assertEquals("candidate:late", engine.lastPeer().remoteCandidates().getLast().candidate());
        } finally {
            release.countDown();
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void theHandshakeTimeoutClosesTheChildAndItsPeer() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        NetherNetServerChannel server = (NetherNetServerChannel) shared(engine, signaling)
                .option(NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS, 0)
                .bind(new InetSocketAddress(0)).sync().channel();
        try {
            server.acceptConnection(1, OFFER, "1");
            awaitClosed(engine);
            assertEquals(1, engine.lastPeer().closeCalls());
            server.eventLoop().submit(() -> { }).sync();
            assertTrue(signaling.handlers.isEmpty());
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aFailedAnswerSignalsOneErrorAndClosesTheChild(boolean signalingFails) throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        signaling.failSend = signalingFails;
        NetherNetServerChannel server = bindShared(engine, signaling);
        try {
            server.acceptConnection(4, OFFER, "42");
            FakePeer peer = awaitNegotiation(engine);
            peer.failAnswer(new IllegalStateException("SetRemoteDescription failed"));
            peer.closed(CloseCause.NEGOTIATION_FAILED);
            awaitClosed(engine);
            server.eventLoop().submit(() -> { }).sync();

            assertEquals(new Sent("42", "CONNECTERROR 4 Negotiation failed"), signaling.sent.poll(2, TimeUnit.SECONDS));
            assertTrue(signaling.sent.isEmpty());
            assertEquals(1, peer.closeCalls());
            assertTrue(signaling.handlers.isEmpty());
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void aFailingDecoratorFailsTheExchange() throws Exception {
        FakeEngine engine = new FakeEngine();
        engine.answerWith("v=0\r\n");
        StubSignaling signaling = new StubSignaling(false);
        NetherNetServerChannel server = (NetherNetServerChannel) shared(engine, signaling)
                .option(NetherChannelOption.NETHER_SERVER_ANSWER_DECORATOR, answer -> {
                    throw new IllegalStateException("no key");
                })
                .bind(new InetSocketAddress(0)).sync().channel();
        try {
            server.acceptConnection(5, OFFER, "5");
            awaitClosed(engine);
            assertEquals(new Sent("5", "CONNECTERROR 5 answer decoration failed"), signaling.sent.poll(2, TimeUnit.SECONDS));
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void aPeerTheEngineCannotCreateSignalsAnErrorAndClosesTheChild() throws Exception {
        FakeEngine engine = new FakeEngine();
        engine.failCreation(new NoClassDefFoundError("native binding unavailable"));
        StubSignaling signaling = new StubSignaling(false);
        LinkedBlockingQueue<Channel> children = new LinkedBlockingQueue<>();
        NetherNetServerChannel server = (NetherNetServerChannel) shared(engine, signaling)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel child) {
                        children.add(child);
                    }
                })
                .bind(new InetSocketAddress(0)).sync().channel();
        try {
            server.acceptConnection(6, OFFER, "6");
            server.eventLoop().submit(() -> { }).sync();
            assertEquals(new Sent("6", "CONNECTERROR 6 Failed to establish connection"), signaling.sent.poll(2, TimeUnit.SECONDS));
            assertNull(children.poll(100, TimeUnit.MILLISECONDS), "a child without a peer is never exposed");
            assertTrue(signaling.handlers.isEmpty());
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void aConnectErrorFromTheClientClosesTheChild() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        NetherNetServerChannel server = bindShared(engine, signaling);
        try {
            server.acceptConnection(7, OFFER, "7");
            awaitNegotiation(engine);
            signaling.handlers.get(7L).onSignal("CONNECTERROR 7 gave up");
            awaitClosed(engine);
            assertTrue(signaling.sent.isEmpty());
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void fullIceSignalingAnswersWithTheGatheredDescriptionAndTricklesNothing() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        signaling.fullIce = true;
        NetherNetServerChannel server = (NetherNetServerChannel) shared(engine, signaling)
                .option(NetherChannelOption.NETHER_SERVER_ANSWER_DECORATOR, answer -> answer)
                .bind(new InetSocketAddress(0)).sync().channel();
        try {
            server.acceptConnection(8, OFFER, "8");
            FakePeer peer = awaitNegotiation(engine);
            peer.completeAnswer("v=0\r\n");
            peer.localCandidate("candidate:1", "data");
            peer.gatheringComplete("v=0\r\na=candidate:1\r\na=end-of-candidates\r\n");
            assertEquals(new Sent("8", "CONNECTRESPONSE 8 v=0\r\na=candidate:1\r\na=end-of-candidates\r\n"),
                    signaling.sent.poll(2, TimeUnit.SECONDS));
            assertNull(signaling.sent.poll(100, TimeUnit.MILLISECONDS));
        } finally {
            server.close().syncUninterruptibly();
        }
    }

    @Test
    void serverCloseClosesChildrenAndReportsAcceptedJoins() throws Exception {
        FakeEngine engine = new FakeEngine();
        StubSignaling signaling = new StubSignaling(false);
        List<String> accepted = new ArrayList<>();
        NetherNetServerChannel server = (NetherNetServerChannel) shared(engine, signaling)
                .option(NetherChannelOption.NETHER_SERVER_METRICS, new NetherServerMetrics() {
                    @Override
                    public void connectionAccepted(String networkId, boolean verifiedIdentity) {
                        accepted.add(networkId + " " + verifiedIdentity);
                    }
                })
                .bind(new InetSocketAddress(0)).sync().channel();
        server.acceptConnection(3, OFFER, "3");
        awaitNegotiation(engine);

        server.close().sync();

        awaitClosed(engine);
        group.next().submit(() -> { }).sync();
        assertTrue(signaling.handlers.isEmpty());
        assertEquals(List.of("3 false"), accepted);
        assertEquals(0, engine.closeCalls(), "a shared engine stays with its owner");
    }

    private ServerBootstrap owned(Supplier<? extends NetherNetEngine> engines, NetherNetServerSignaling signaling) {
        return new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engines, signaling))
                .childHandler(new ChannelInboundHandlerAdapter());
    }

    private ServerBootstrap shared(NetherNetEngine engine, NetherNetServerSignaling signaling) {
        return new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine, signaling))
                .childHandler(new ChannelInboundHandlerAdapter());
    }

    private NetherNetServerChannel bindShared(NetherNetEngine engine, NetherNetServerSignaling signaling)
            throws InterruptedException {
        return (NetherNetServerChannel) shared(engine, signaling).bind(new InetSocketAddress(0)).sync().channel();
    }

    /** Waits until the newest peer was asked to answer. */
    private static FakePeer awaitNegotiation(FakeEngine engine) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (!engine.peers().isEmpty() && !engine.lastPeer().remoteOffers().isEmpty()) {
                return engine.lastPeer();
            }
            Thread.sleep(5);
        }
        throw new AssertionError("The child never negotiated");
    }

    private static void awaitClosed(FakeEngine engine) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (!engine.peers().isEmpty() && engine.lastPeer().closeCalled()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("The child's peer was never closed");
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record Sent(String target, String data) {
    }

    private static final class StubSignaling implements NetherNetServerSignaling {
        private final boolean failBind;
        volatile boolean bound;
        volatile boolean closed;
        volatile boolean failSend;
        volatile boolean fullIce;
        volatile List<IceServerInfo> iceServers = List.of();
        final Map<Long, SignalHandler> handlers = new ConcurrentHashMap<>();
        final LinkedBlockingQueue<Sent> sent = new LinkedBlockingQueue<>();

        private StubSignaling(boolean failBind) {
            this.failBind = failBind;
        }

        @Override
        public void bind(SocketAddress localAddress) throws ConnectException {
            if (failBind) {
                throw new ConnectException("port taken");
            }
            bound = true;
        }

        @Override
        public void setNewConnectionHandler(NewConnectionHandler handler) {
        }

        @Override
        public void setAdvertisementData(PongData pongData) {
        }

        @Override
        public List<IceServerInfo> getIceServers() {
            return iceServers;
        }

        @Override
        public boolean fullIceAnswers() {
            return fullIce;
        }

        @Override
        public void sendSignal(String targetNetworkId, String data) {
            sent.add(new Sent(targetNetworkId, data));
            if (failSend) {
                throw new IllegalStateException("Signaling unavailable");
            }
        }

        @Override
        public void setSignalHandler(long connectionId, SignalHandler handler) {
            handlers.put(connectionId, handler);
        }

        @Override
        public void removeSignalHandler(long connectionId) {
            handlers.remove(connectionId);
        }

        @Override
        public String getLocalNetworkId() {
            return "0";
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}

/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.nxs.control;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ManualScheduler;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Netty carrier against a real loopback server, with its owner on virtual time: deadlines pass when the test
 * moves the clock, and write completions wait in the owner's queue behind the work already there.
 */
class ControlSocketTest {
    private static final String PROTOCOL = "transport-test";
    private static final Duration DEADLINE = Duration.ofSeconds(3);
    private static final Function<String, CompletionStage<?>> IGNORE = text -> CompletableFuture.completedFuture(null);

    private final ManualScheduler owner = new ManualScheduler();
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final List<ControlSocket> sockets = new ArrayList<>();

    @AfterEach
    void stop() {
        sockets.forEach(ControlSocket::abort);
        owner.runReady();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private static ControlSocket.Limits limits() {
        return new ControlSocket.Limits(4096, 64, 64, 32768, DEADLINE, DEADLINE, DEADLINE, DEADLINE);
    }

    private ControlSocket open(Loopback server, ControlSocket.Limits limits,
                               Function<String, ? extends CompletionStage<?>> receiver) throws Exception {
        ControlSocket socket = start(server, server.uri(), server.client, limits, receiver);
        owner.await(socket.opened());
        server.connected();
        return socket;
    }

    private ControlSocket start(Loopback server, URI endpoint, SslContext tls, ControlSocket.Limits limits,
                                Function<String, ? extends CompletionStage<?>> receiver) {
        ControlSocket socket = ControlSocket.connect(group, endpoint, PROTOCOL,
                Map.of("X-Control-Test", "opaque-auth-value"), tls, limits, owner, receiver);
        sockets.add(socket);
        return socket;
    }

    @Test
    void receivesFragmentsAndPingsWithoutLosingTheMessage() throws Exception {
        BlockingQueue<String> received = new LinkedBlockingQueue<>();
        try (var server = new Loopback(false)) {
            var socket = open(server, limits(), text -> {
                received.add(text);
                return CompletableFuture.completedFuture(null);
            });
            // Even one UTF-8 code point can straddle continuation frames, with a ping between them
            server.send(new TextWebSocketFrame(false, 0, Unpooled.wrappedBuffer(new byte[]{'o', 'n', 'e', ' ', (byte) 0xf0})));
            server.send(new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{1, 2, 3})));
            server.send(new ContinuationWebSocketFrame(false, 0,
                    Unpooled.wrappedBuffer(new byte[]{(byte) 0x9f, (byte) 0x98, (byte) 0x80})));
            server.send(new ContinuationWebSocketFrame(true, 0, " café"));
            server.send(new TextWebSocketFrame("next"));
            owner.until(() -> received.size() == 2);
            assertEquals(List.of("one 😀 café", "next"), List.copyOf(received));
            assertArrayEquals(new byte[]{1, 2, 3}, server.pongs.poll(5, TimeUnit.SECONDS));
            assertEquals("opaque-auth-value", server.header);
            owner.await(socket.sendText("outbound 😀"));
            assertEquals("outbound 😀", server.texts.poll(5, TimeUnit.SECONDS));
            assertEquals(ControlSocket.NORMAL, owner.await(socket.closeGracefully()).statusCode());
            assertEquals(0, owner.pendingTimers(), "A finished connection leaves no timer behind");
        }
    }

    @Test
    void wssChecksTheCertificateAndTheHostName() throws Exception {
        try (var server = new Loopback(true)) {
            var socket = open(server, limits(), IGNORE);
            owner.await(socket.sendText("encrypted"));
            assertEquals("encrypted", server.texts.poll(5, TimeUnit.SECONDS));
            owner.await(socket.closeGracefully());

            var wrongHost = start(server, URI.create(server.uri().toString().replace("localhost", "127.0.0.1")),
                    server.client, limits(), IGNORE);
            assertTrue(hasCause(owner.failure(wrongHost.opened()), SSLHandshakeException.class));

            var untrusted = start(server, server.uri(), SslContextBuilder.forClient()
                    .endpointIdentificationAlgorithm("HTTPS").build(), limits(), IGNORE);
            Throwable rejected = owner.failure(untrusted.opened());
            assertTrue(hasCause(rejected, SSLHandshakeException.class), rejected.toString());
        }
    }

    @Test
    void readsTheNextMessageOnlyAfterTheReceiverAcceptedTheLast() throws Exception {
        var firstAccepted = new CompletableFuture<Void>();
        List<String> received = new CopyOnWriteArrayList<>();
        try (var server = new Loopback(false)) {
            open(server, limits(), text -> {
                received.add(text);
                return text.equals("first") ? firstAccepted : CompletableFuture.completedFuture(null);
            });
            server.send(new TextWebSocketFrame("first"));
            owner.until(() -> received.size() == 1);
            server.send(new TextWebSocketFrame("second"));
            owner.quiet(150);
            assertEquals(List.of("first"), received);
            firstAccepted.complete(null);
            owner.until(() -> received.size() == 2);
            assertEquals(List.of("first", "second"), received);
        }
    }

    @Test
    void framesWaitingForABusyOwnerStopTheReading() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        try (var server = new Loopback(false)) {
            var limits = new ControlSocket.Limits(65536, 64, 64, 131072, DEADLINE, DEADLINE, DEADLINE, DEADLINE);
            open(server, limits, text -> {
                received.add(text);
                return CompletableFuture.completedFuture(null);
            });
            owner.runReady();
            Channel peer = server.socket.get(5, TimeUnit.SECONDS);
            // The owner runs nothing meanwhile; each ping makes Netty ask for another read
            int messages = 64;
            String filler = "x".repeat(60000);
            for (int i = 0; i < messages; i++) {
                peer.write(new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{1})));
                peer.write(new TextWebSocketFrame(i + ":" + filler));
            }
            peer.flush();
            // One message's worth, and what the read that passed it had already decoded
            int handedOver = settledQueue();
            assertTrue(handedOver <= 4, handedOver + " messages waited for the owner");
            owner.until(() -> received.size() == messages);
            for (int i = 0; i < messages; i++) {
                assertTrue(received.get(i).startsWith(i + ":"), "Message " + i + " in order");
            }
        }
    }

    /** How many tasks wait for the owner once the event loop stopped handing over more for a while. */
    private int settledQueue() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (owner.queued() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        int queued;
        do {
            queued = owner.queued();
            Thread.sleep(200);
        } while (owner.queued() != queued && System.currentTimeMillis() < deadline);
        return queued;
    }

    @Test
    void serializesConcurrentProducersWithoutLosingMessages() throws Exception {
        ExecutorService producers = Executors.newFixedThreadPool(8);
        try (var server = new Loopback(false)) {
            var socket = open(server, limits(), IGNORE);
            List<CompletableFuture<Void>> sent = new ArrayList<>();
            var expected = new HashSet<String>();
            for (int i = 0; i < 32; i++) {
                String message = "message-" + i;
                expected.add(message);
                sent.add(CompletableFuture.supplyAsync(() -> socket.sendText(message), producers)
                        .thenCompose(CompletionStage::toCompletableFuture));
            }
            owner.await(CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)));
            var actual = new HashSet<String>();
            for (int i = 0; i < 32; i++) {
                actual.add(server.texts.poll(5, TimeUnit.SECONDS));
            }
            assertEquals(expected, actual);
            owner.await(socket.closeGracefully());
        } finally {
            producers.shutdownNow();
        }
    }

    @Test
    void unacceptableMessagesEndTheConnectionWithTheirCloseCode() throws Exception {
        record Case(int code, List<WebSocketFrame> frames) {
        }
        for (var c : List.of(
                new Case(ControlSocket.UNSUPPORTED_DATA,
                        List.of(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{1})))),
                new Case(ControlSocket.INVALID_PAYLOAD,
                        List.of(new TextWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{'a', (byte) 0xc3, '(', 'b'})))),
                new Case(ControlSocket.TOO_BIG, List.of(new TextWebSocketFrame(false, 0, "€".repeat(1000)),
                        new ContinuationWebSocketFrame(true, 0, "€".repeat(1000)))))) {
            try (var server = new Loopback(false)) {
                var delivered = new AtomicInteger();
                var socket = open(server, limits(), text -> {
                    delivered.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
                for (WebSocketFrame frame : c.frames()) {
                    server.send(frame);
                }
                assertNotNull(owner.failure(socket.closed()));
                assertEquals(c.code(), server.closeCodes.poll(5, TimeUnit.SECONDS));
                assertEquals(0, delivered.get());
                assertEquals(0, owner.pendingTimers());
            }
        }
    }

    @Test
    void aSingleFrameOverTheLimitEndsTheConnectionAtOnce() throws Exception {
        try (var server = new Loopback(false)) {
            var delivered = new AtomicInteger();
            var socket = open(server, limits(), text -> {
                delivered.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            });
            server.send(new TextWebSocketFrame("€".repeat(2000)));
            assertInstanceOf(CorruptedWebSocketFrameException.class, owner.failure(socket.closed()));
            server.inactive.get(5, TimeUnit.SECONDS);
            assertTrue(server.closeCodes.isEmpty(), "No close frame claims a normal closure");
            assertEquals(0, delivered.get());
            assertEquals(0, owner.pendingTimers());
        }
    }

    @Test
    void aMessageInMoreFragmentsThanAllowedIsTooBig() throws Exception {
        try (var server = new Loopback(false)) {
            var limits = new ControlSocket.Limits(64, 2, 2, 128, DEADLINE, DEADLINE, DEADLINE, DEADLINE);
            var socket = open(server, limits, IGNORE);
            server.send(new TextWebSocketFrame(false, 0, Unpooled.EMPTY_BUFFER));
            server.send(new ContinuationWebSocketFrame(false, 0, Unpooled.EMPTY_BUFFER));
            server.send(new ContinuationWebSocketFrame(false, 0, Unpooled.EMPTY_BUFFER));
            assertNotNull(owner.failure(socket.closed()));
            assertEquals(ControlSocket.TOO_BIG, server.closeCodes.poll(5, TimeUnit.SECONDS));
            assertEquals(0, owner.pendingTimers());
        }
    }

    @Test
    void anIncompleteOrUnacceptedMessageTimesOut() throws Exception {
        for (boolean complete : List.of(false, true)) {
            try (var server = new Loopback(false)) {
                var limits = new ControlSocket.Limits(64, 2, 2, 128, DEADLINE, Duration.ofMillis(100), DEADLINE,
                        DEADLINE);
                var received = new AtomicInteger();
                var socket = open(server, limits, text -> {
                    received.incrementAndGet();
                    return new CompletableFuture<>();
                });
                server.send(new TextWebSocketFrame(complete, 0, Unpooled.EMPTY_BUFFER));
                if (complete) {
                    owner.until(() -> received.get() == 1);
                } else {
                    owner.until(() -> owner.pendingTimers() == 1); // The first fragment started the deadline
                }
                owner.advance(Duration.ofMillis(100));
                assertInstanceOf(TimeoutException.class, owner.failure(socket.closed()));
                assertEquals(0, owner.pendingTimers());
            }
        }
    }

    @Test
    void aMissingSubprotocolFailsTheUpgrade() throws Exception {
        try (var server = new Loopback(false, false, true)) {
            var socket = start(server, server.uri(), null, limits(), IGNORE);
            assertTrue(owner.failure(socket.opened()).getMessage().contains("subprotocol"));
            assertNotNull(owner.failure(socket.closed()));
        }
    }

    @Test
    void theUpgradeAndTheCloseEachHaveADeadline() throws Exception {
        for (boolean handshake : List.of(false, true)) {
            try (var server = new Loopback(false, true, handshake)) {
                server.replyClose = false;
                var limits = new ControlSocket.Limits(64, 10, 2, 128, Duration.ofMillis(200), DEADLINE, DEADLINE,
                        Duration.ofMillis(200));
                var socket = start(server, server.uri(), null, limits, IGNORE);
                if (handshake) {
                    owner.await(socket.opened());
                    var closing = socket.closeGracefully();
                    owner.until(() -> !server.closeCodes.isEmpty());
                    owner.advance(Duration.ofMillis(200));
                    assertInstanceOf(TimeoutException.class, owner.failure(closing));
                } else {
                    server.upgradeRequested.get(5, TimeUnit.SECONDS);
                    owner.runReady();
                    owner.advance(Duration.ofMillis(200));
                    assertInstanceOf(TimeoutException.class, owner.failure(socket.opened()));
                    assertNotNull(owner.failure(socket.closed()));
                }
                assertEquals(0, owner.pendingTimers());
            }
        }
    }

    @Test
    void sendsAreSerializedAndTheActiveSendCountsAgainstBothLimits() throws Exception {
        try (var server = new Loopback(false)) {
            var limits = new ControlSocket.Limits(20, 10, 3, 8, DEADLINE, DEADLINE, DEADLINE, DEADLINE);
            var socket = open(server, limits, IGNORE);
            // Queued before the owner runs, so each is counted while the first is still being written
            var first = socket.sendText("😀");
            var second = socket.sendText("two");
            var overBytes = socket.sendText("no");
            var third = socket.sendText("");
            var overCount = socket.sendText("");
            assertTrue(owner.failure(overBytes).getMessage().contains("capacity"));
            assertTrue(owner.failure(overCount).getMessage().contains("capacity"));
            owner.await(first);
            owner.await(second);
            owner.await(third);
            assertEquals("😀", server.texts.poll(5, TimeUnit.SECONDS));
            assertEquals("two", server.texts.poll(5, TimeUnit.SECONDS));
            assertEquals("", server.texts.poll(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aGuardThatNoLongerHoldsDropsOnlyItsOwnMessage() throws Exception {
        try (var server = new Loopback(false)) {
            var socket = open(server, limits(), IGNORE);
            var checks = new AtomicInteger();
            var withdrawn = new java.util.concurrent.atomic.AtomicBoolean(true);
            OwnershipGuard guard = () -> {
                checks.incrementAndGet();
                if (withdrawn.get()) {
                    throw new IllegalStateException("installation unavailable");
                }
            };
            var first = socket.sendText("first");
            var late = socket.sendText("late answer", guard);
            var later = socket.sendText("later");
            owner.await(first);
            assertEquals("installation unavailable", owner.failure(late).getMessage());
            assertEquals(1, checks.get(), "The guard runs once, when its message reaches the socket");
            owner.await(later);
            assertEquals("first", server.texts.poll(5, TimeUnit.SECONDS));
            assertEquals("later", server.texts.poll(5, TimeUnit.SECONDS));
            assertFalse(socket.closed().toCompletableFuture().isDone(), "The connection carries on");
            withdrawn.set(false);
            owner.await(socket.sendText("fresh", guard));
            assertEquals("fresh", server.texts.poll(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void oversizedAndMalformedOutboundMessagesNeverReachTheSocket() throws Exception {
        try (var server = new Loopback(false)) {
            var socket = open(server, limits(), IGNORE);
            assertNotNull(owner.failure(socket.sendText("😀".repeat(1025))));
            assertNotNull(owner.failure(socket.sendText("\uD800")));
            owner.await(socket.sendText("valid"));
            assertEquals("valid", server.texts.poll(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void queuedSendsDrainBeforeTheCloseAndLaterSendsFail() throws Exception {
        try (var server = new Loopback(false)) {
            var socket = open(server, limits(), IGNORE);
            var first = socket.sendText("first");
            var second = socket.sendText("second");
            var closing = socket.closeGracefully();
            var late = socket.sendText("late");
            assertNotNull(owner.failure(late));
            owner.await(first);
            owner.await(second);
            assertEquals(ControlSocket.NORMAL, owner.await(closing).statusCode());
            assertEquals(List.of("first", "second"), List.of(server.texts.poll(5, TimeUnit.SECONDS),
                    server.texts.poll(5, TimeUnit.SECONDS)));
            assertEquals(ControlSocket.NORMAL, server.closeCodes.poll(5, TimeUnit.SECONDS));
            assertTrue(server.texts.isEmpty());
        }
    }

    @Test
    void anAbortDuringTheUpgradeFailsBothStages() throws Exception {
        try (var server = new Loopback(false, true, false)) {
            var socket = start(server, server.uri(), null, limits(), IGNORE);
            server.upgradeRequested.get(5, TimeUnit.SECONDS);
            socket.abort();
            assertNotNull(owner.failure(socket.opened()));
            assertNotNull(owner.failure(socket.closed()));
            server.inactive.get(5, TimeUnit.SECONDS);
            assertEquals(0, owner.pendingTimers());
        }
    }

    @Test
    void aFailingReceiverEndsTheConnection() throws Exception {
        for (boolean throwing : List.of(false, true)) {
            try (var server = new Loopback(false)) {
                var socket = open(server, limits(), text -> {
                    if (throwing) {
                        throw new IllegalStateException("receiver failed");
                    }
                    return CompletableFuture.failedFuture(new IllegalStateException("receiver failed"));
                });
                server.send(new TextWebSocketFrame("message"));
                assertEquals("receiver failed", owner.failure(socket.closed()).getMessage());
                server.inactive.get(5, TimeUnit.SECONDS);
                assertEquals(0, owner.pendingTimers());
            }
        }
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    /** A WebSocket server on loopback that records what the client sends and sends what the test asks. */
    private static final class Loopback implements AutoCloseable {
        final BlockingQueue<String> texts = new LinkedBlockingQueue<>();
        final BlockingQueue<byte[]> pongs = new LinkedBlockingQueue<>();
        final BlockingQueue<Integer> closeCodes = new LinkedBlockingQueue<>();
        final CompletableFuture<Void> upgradeRequested = new CompletableFuture<>();
        final CompletableFuture<Void> inactive = new CompletableFuture<>();
        final CompletableFuture<Channel> socket = new CompletableFuture<>();
        final SslContext client;
        volatile boolean replyClose = true;
        volatile String header;
        private final boolean secure;
        private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        private final Channel server;

        Loopback(boolean secure) throws Exception {
            this(secure, true, true);
        }

        /**
         * @param selectProtocol Whether the server selects the requested subprotocol
         * @param handshake      Whether the server answers the upgrade at all
         */
        Loopback(boolean secure, boolean selectProtocol, boolean handshake) throws Exception {
            this.secure = secure;
            SslContext serverTls = null;
            if (secure) {
                X509Bundle root = new CertificateBuilder().subject("CN=Loopback Root").setIsCertificateAuthority(true)
                        .ecp256().buildSelfSigned();
                X509Bundle leaf = new CertificateBuilder().subject("CN=localhost").addSanDnsName("localhost")
                        .ecp256().buildIssuedBy(root);
                serverTls = SslContextBuilder.forServer(leaf.getKeyPair().getPrivate(), leaf.getCertificatePath())
                        .build();
                client = SslContextBuilder.forClient().trustManager(root.getCertificate())
                        .endpointIdentificationAlgorithm("HTTPS").build();
            } else {
                client = null;
            }
            SslContext tls = serverTls;
            server = new ServerBootstrap()
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            if (tls != null) {
                                channel.pipeline().addLast(tls.newHandler(channel.alloc()));
                            }
                            channel.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(65536),
                                    new Upgrade(selectProtocol, handshake));
                        }
                    })
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        }

        URI uri() {
            int port = ((InetSocketAddress) server.localAddress()).getPort();
            return URI.create((secure ? "wss://localhost:" : "ws://127.0.0.1:") + port + "/control");
        }

        /** Waits until the server side finished the upgrade, so frames it sends follow the 101 response. */
        void connected() throws Exception {
            socket.get(5, TimeUnit.SECONDS);
        }

        void send(WebSocketFrame frame) throws Exception {
            socket.get(5, TimeUnit.SECONDS).writeAndFlush(frame).sync();
        }

        @Override
        public void close() {
            server.close().syncUninterruptibly();
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }

        private final class Upgrade extends SimpleChannelInboundHandler<FullHttpRequest> {
            private final boolean selectProtocol, handshake;

            Upgrade(boolean selectProtocol, boolean handshake) {
                this.selectProtocol = selectProtocol;
                this.handshake = handshake;
            }

            @Override
            protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) {
                header = request.headers().get("X-Control-Test");
                upgradeRequested.complete(null);
                if (!handshake) {
                    return;
                }
                var handshaker = new WebSocketServerHandshakerFactory("ws://localhost/control",
                        selectProtocol ? PROTOCOL : null, false, 1 << 20).newHandshaker(request);
                context.pipeline().addLast(new WebSocketFrameAggregator(1 << 20), new Frames());
                context.pipeline().remove(this);
                handshaker.handshake(context.channel(), request).addListener(done -> socket.complete(context.channel()));
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) {
                inactive.complete(null);
                context.fireChannelInactive();
            }
        }

        private final class Frames extends SimpleChannelInboundHandler<WebSocketFrame> {
            @Override
            protected void channelRead0(ChannelHandlerContext context, WebSocketFrame frame) {
                switch (frame) {
                    case TextWebSocketFrame text -> texts.add(text.text());
                    case PongWebSocketFrame pong -> pongs.add(ByteBufUtil.getBytes(pong.content()));
                    case CloseWebSocketFrame close -> {
                        closeCodes.add(close.statusCode());
                        if (replyClose) {
                            context.writeAndFlush(new CloseWebSocketFrame(close.statusCode(), ""))
                                    .addListener(ChannelFutureListener.CLOSE);
                        }
                    }
                    default -> {
                    }
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) {
                inactive.complete(null);
                context.fireChannelInactive();
            }
        }
    }
}

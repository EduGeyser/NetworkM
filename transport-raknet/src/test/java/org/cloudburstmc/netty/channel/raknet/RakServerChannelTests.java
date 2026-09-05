package org.cloudburstmc.netty.channel.raknet;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.cloudburstmc.netty.channel.raknet.config.DefaultRakServerThrottle;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.channel.raknet.config.RakServerCookieMode;
import org.cloudburstmc.netty.channel.raknet.config.RakServerMetrics;
import org.cloudburstmc.netty.channel.raknet.config.RakServerThrottle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class RakServerChannelTests {
    private static final InetSocketAddress REMOTE = new InetSocketAddress("127.0.0.1", 19134);
    private static final InetSocketAddress OTHER_REMOTE = new InetSocketAddress("127.0.0.1", 19135);

    private final EventLoopGroup parentGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final EventLoopGroup childGroup = new DefaultEventLoopGroup(1);
    private final LinkedBlockingQueue<RakChildChannel> initialized = new LinkedBlockingQueue<>();
    private RakServerChannel server;

    @AfterEach
    void close() {
        if (server != null) {
            server.close().awaitUninterruptibly();
        }
        childGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
        parentGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closingReplacedChildKeepsTheNewRouteAndReservation(boolean throttled) throws Exception {
        bind(throttled ? new DefaultRakServerThrottle(1, 60_000, 1000) : null);
        RakChildChannel original = createChild(1);
        assertSame(original, initialized.poll(5, TimeUnit.SECONDS));

        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        original.eventLoop().execute(() -> {
            blocked.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException cause) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(blocked.await(5, TimeUnit.SECONDS));
        RakChildChannel replacement;
        try {
            replacement = createChild(2);
            assertNotNull(replacement, "An authenticated replacement must reuse its original connection slot");
            assertSame(replacement, server.getChildChannel(REMOTE));
            if (throttled) {
                assertNull(createChild(OTHER_REMOTE, 3));
            }
        } finally {
            release.countDown();
        }

        assertSame(replacement, initialized.poll(5, TimeUnit.SECONDS));
        original.closeFuture().syncUninterruptibly();
        original.eventLoop().submit(() -> { }).syncUninterruptibly();
        assertSame(replacement, server.getChildChannel(REMOTE));
        assertTrue(replacement.isOpen());
        if (throttled) {
            assertNull(createChild(OTHER_REMOTE, 3), "The old close must not release the replacement's slot");
            replacement.close().syncUninterruptibly();
            replacement.eventLoop().submit(() -> { }).syncUninterruptibly();
            createInitializedChild(OTHER_REMOTE, 3);
        }
    }

    @Test
    void rateLimitedReplacementKeepsTheExistingChild() throws Exception {
        bind(new DefaultRakServerThrottle(1, 60_000, 1));
        RakChildChannel original = createChild(1);
        assertSame(original, initialized.poll(5, TimeUnit.SECONDS));

        assertNull(createChild(2));
        original.eventLoop().submit(() -> { }).syncUninterruptibly();

        assertSame(original, server.getChildChannel(REMOTE));
        assertTrue(original.isOpen());
    }

    @Test
    void failedReplacementConfigurationKeepsTheExistingReservation() throws Exception {
        bind(new DefaultRakServerThrottle(1, 60_000, 1000), child -> {
            if (child.config().getGuid() == 2) {
                throw new IllegalStateException("Replacement configuration rejected");
            }
        });
        RakChildChannel original = createChild(1);
        assertSame(original, initialized.poll(5, TimeUnit.SECONDS));

        assertThrows(IllegalStateException.class, () -> createChild(2));

        assertSame(original, server.getChildChannel(REMOTE));
        assertTrue(original.isOpen());
        assertNull(createChild(OTHER_REMOTE, 3));
        original.close().syncUninterruptibly();
        original.eventLoop().submit(() -> { }).syncUninterruptibly();
        createInitializedChild(OTHER_REMOTE, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closingOriginalDuringReplacementConfigurationPreservesAccounting(boolean failReplacement) throws Exception {
        AtomicReference<RakChildChannel> original = new AtomicReference<>();
        bind(new DefaultRakServerThrottle(1, 60_000, 1000), child -> {
            if (child.config().getGuid() == 2) {
                assertTrue(original.get().close().awaitUninterruptibly(5, TimeUnit.SECONDS));
                assertTrue(original.get().eventLoop().submit(() -> { }).awaitUninterruptibly(5, TimeUnit.SECONDS));
                if (failReplacement) {
                    throw new IllegalStateException("Replacement configuration rejected");
                }
            }
        });
        original.set(createChild(1));
        assertSame(original.get(), initialized.poll(5, TimeUnit.SECONDS));

        if (failReplacement) {
            assertThrows(IllegalStateException.class, () -> createChild(2));
            assertNull(server.getChildChannel(REMOTE));
        } else {
            RakChildChannel replacement = createChild(2);
            assertNotNull(replacement);
            assertSame(replacement, initialized.poll(5, TimeUnit.SECONDS));
            assertNull(createChild(OTHER_REMOTE, 3));
            replacement.close().syncUninterruptibly();
            replacement.eventLoop().submit(() -> { }).syncUninterruptibly();
        }
        createInitializedChild(OTHER_REMOTE, 3);
    }

    @Test
    void defaultThrottleSubclassAdmissionPolicyIsPreserved() throws Exception {
        AtomicInteger accepts = new AtomicInteger();
        DefaultRakServerThrottle throttle = new DefaultRakServerThrottle(10, 60_000, 1000) {
            @Override
            public boolean accept(InetSocketAddress address) {
                accepts.incrementAndGet();
                return accepts.get() == 1 && super.accept(address);
            }
        };
        bind(throttle);
        RakChildChannel original = createChild(1);
        assertSame(original, initialized.poll(5, TimeUnit.SECONDS));

        assertNull(createChild(2));

        assertEquals(2, accepts.get());
        assertSame(original, server.getChildChannel(REMOTE));
        assertTrue(original.isOpen());
    }

    @Test
    void routeExistsBeforeChildIsPassedToBootstrap() throws Exception {
        bind(null);
        AtomicInteger routed = new AtomicInteger();
        server.eventLoop().submit(() -> server.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                if (message instanceof RakChildChannel child && server.getChildChannel(REMOTE) == child) {
                    routed.incrementAndGet();
                }
                ctx.fireChannelRead(message);
            }
        })).syncUninterruptibly();

        RakChildChannel child = createChild(1);
        assertSame(child, initialized.poll(5, TimeUnit.SECONDS));
        assertEquals(1, routed.get());
    }

    @Test
    void closingChildUsesTheThrottleThatAcceptedIt() throws Exception {
        CountingThrottle original = new CountingThrottle();
        CountingThrottle replacement = new CountingThrottle();
        bind(original);
        RakChildChannel child = createChild(1);
        assertSame(child, initialized.poll(5, TimeUnit.SECONDS));
        assertEquals(1, original.connections.get());

        server.config().setThrottle(replacement);
        child.close().syncUninterruptibly();
        child.eventLoop().submit(() -> { }).syncUninterruptibly();

        assertEquals(0, original.connections.get());
        assertEquals(0, replacement.connections.get());
    }

    @Test
    void failingOpenMetricsDoNotPreventRegistrationOrReplacement() throws Exception {
        bind(new DefaultRakServerThrottle(1, 60_000, 1000));
        server.config().setMetrics(new RakServerMetrics() {
            @Override
            public void channelOpen(InetSocketAddress address) {
                throw new IllegalStateException("Metrics unavailable");
            }
        });
        RakChildChannel original = createInitializedChild(REMOTE, 1);

        RakChildChannel replacement = createInitializedChild(REMOTE, 2);

        assertTrue(original.closeFuture().await(5, TimeUnit.SECONDS));
        assertSame(replacement, server.getChildChannel(REMOTE));
        assertTrue(replacement.isOpen());
        assertNull(createChild(OTHER_REMOTE, 3));
        replacement.close().syncUninterruptibly();
        replacement.eventLoop().submit(() -> { }).syncUninterruptibly();
        createInitializedChild(OTHER_REMOTE, 3);
    }

    @Test
    void failingCloseMetricsDoNotPreventPipelineOrReservationCleanup() throws Exception {
        bind(new DefaultRakServerThrottle(1, 60_000, 1000));
        server.config().setMetrics(new RakServerMetrics() {
            @Override
            public void channelClose(InetSocketAddress address) {
                throw new IllegalStateException("Metrics unavailable");
            }
        });
        RakChildChannel original = createInitializedChild(REMOTE, 1);
        CountDownLatch removed = new CountDownLatch(1);
        server.eventLoop().submit(() -> original.rakPipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void handlerRemoved(ChannelHandlerContext ctx) {
                removed.countDown();
            }
        })).syncUninterruptibly();

        RakChildChannel replacement = createInitializedChild(REMOTE, 2);

        assertTrue(removed.await(5, TimeUnit.SECONDS));
        assertSame(replacement, server.getChildChannel(REMOTE));
        assertNull(createChild(OTHER_REMOTE, 3));
        replacement.close().syncUninterruptibly();
        replacement.eventLoop().submit(() -> { }).syncUninterruptibly();
        createInitializedChild(OTHER_REMOTE, 3);
    }

    @Test
    void failedChildConfigurationReleasesTheThrottleReservation() {
        CountingThrottle throttle = new CountingThrottle();
        bind(throttle, child -> {
            throw new IllegalStateException("Configuration rejected");
        });

        assertThrows(IllegalStateException.class, () -> createChild(1));

        assertEquals(0, throttle.connections.get());
        assertNull(server.getChildChannel(REMOTE));
    }

    private void bind(RakServerThrottle throttle) {
        bind(throttle, null);
    }

    private void bind(RakServerThrottle throttle, Consumer<RakChannel> childConsumer) {
        server = (RakServerChannel) new ServerBootstrap()
                .group(parentGroup, childGroup)
                .channelFactory(RakChannelFactory.server(NioDatagramChannel.class, null, childConsumer))
                .option(RakChannelOption.RAK_SERVER_COOKIE_MODE, RakServerCookieMode.ACTIVE)
                .option(RakChannelOption.RAK_THROTTLE, throttle)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) {
                        initialized.add((RakChildChannel) channel);
                    }
                })
                .bind(new InetSocketAddress("127.0.0.1", 0)).syncUninterruptibly().channel();
    }

    private RakChildChannel createChild(long guid) {
        return this.createChild(REMOTE, guid);
    }

    private RakChildChannel createChild(InetSocketAddress address, long guid) {
        return server.eventLoop().submit(() -> server.createChildChannel(address, server.localAddress(), guid, 1400, 11))
                .syncUninterruptibly().getNow();
    }

    private RakChildChannel createInitializedChild(InetSocketAddress address, long guid) throws InterruptedException {
        RakChildChannel channel = createChild(address, guid);
        assertNotNull(channel);
        assertSame(channel, initialized.poll(5, TimeUnit.SECONDS));
        return channel;
    }

    private static class CountingThrottle implements RakServerThrottle {
        private final AtomicInteger connections = new AtomicInteger();

        @Override
        public boolean accept(InetSocketAddress address) {
            connections.incrementAndGet();
            return true;
        }

        @Override
        public void closed(InetSocketAddress address) {
            connections.decrementAndGet();
        }
    }
}

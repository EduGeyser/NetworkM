package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** getPing reports the ICE round trip time, sampled every 3 s once the channel is active. */
@Timeout(10)
class NetherNetChannelRttSamplerTest {
    private static final TransportMetrics MEASURED = new TransportMetrics(
            Optional.of(Duration.ofNanos(12_600_000)), Optional.empty());

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    @AfterEach
    void tearDown() {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void startsSamplingWhenTheTransportOpensAfterRegistration() throws Exception {
        ChildFixture fixture = new ChildFixture(null);
        fixture.peer.setMetrics(MEASURED);
        try {
            group.register(fixture.child).sync();
            assertEquals(0, fixture.child.getPing(), "0 while unknown");
            fixture.openLinks();

            awaitPing(fixture, 13);
            assertEquals(1, fixture.peer.metricsRequests());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void startsSamplingWhenTheTransportOpensBeforeRegistration() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        fixture.peer.setMetrics(MEASURED);
        try {
            group.register(fixture.child).sync();

            awaitPing(fixture, 13);
            assertEquals(1, fixture.peer.metricsRequests());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void aFailedSampleKeepsTheLastValue() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        fixture.peer.setMetrics(MEASURED);
        try {
            group.register(fixture.child).sync();
            awaitPing(fixture, 13);
            fixture.peer.setMetrics(TransportMetrics.NONE);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (fixture.peer.metricsRequests() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(2, fixture.peer.metricsRequests());
            assertEquals(13, fixture.child.getPing());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void aSynchronousCloseFromChannelActiveCancelsSampling() throws Exception {
        ChildFixture fixture = new ChildFixture(null);
        fixture.peer.setMetrics(MEASURED);
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                ctx.close();
            }
        });

        group.register(fixture.child).sync();
        fixture.openLinks();
        fixture.child.closeFuture().sync();

        Thread.sleep(1200);
        assertEquals(0, fixture.peer.metricsRequests());
    }

    private static void awaitPing(ChildFixture fixture, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (fixture.child.getPing() != expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(expected, fixture.child.getPing());
    }
}

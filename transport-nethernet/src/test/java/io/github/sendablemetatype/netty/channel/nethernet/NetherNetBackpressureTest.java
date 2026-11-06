package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.ActiveChild.payload;
import static org.junit.jupiter.api.Assertions.*;

/** Segments wait in Java while the engine queue is full; a backlog past its cap ends the connection. */
class NetherNetBackpressureTest {

    @Test
    void aLargeMessageIsPacedPerSegment() {
        try (ActiveChild h = new ActiveChild(child -> {
            child.setMaxOutboundMessageSize(4);
            child.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, 8);
        })) {
            ChannelFuture write = h.channel.writeAndFlush(payload(1, 2, 3, 4, 5, 6, 7, 8, 9));
            assertEquals(2, h.reliable().sendCount(), "two 4-byte segments fill the 8-byte pause level");
            assertFalse(h.channel.isWritable());
            h.reliable().acceptAll();
            h.reliable().drain(8);
            h.run();
            assertEquals(3, h.reliable().sendCount());
            assertArrayEquals(new byte[]{0, 7, 8, 9}, h.reliable().sentBytes().get(2));
            assertTrue(h.channel.isWritable());
            h.reliable().acceptAll();
            h.run();
            assertTrue(write.isSuccess());
        }
    }

    @Test
    void thePauseTurnsTheChannelUnwritableUntilTheQueueDrains() {
        try (ActiveChild h = new ActiveChild(child ->
                child.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, 100))) {
            assertTrue(h.channel.isWritable());
            h.reliable().setQueuedBytes(100);
            h.channel.writeAndFlush(payload(1));
            assertFalse(h.channel.isWritable());
            h.reliable().drain(70);
            h.run();
            assertFalse(h.channel.isWritable(), "still above a quarter of the pause level");
            h.reliable().drain(5);
            h.run();
            assertTrue(h.channel.isWritable());
            assertEquals(1, h.reliable().sendCount());
        }
    }

    @Test
    void aPauseLevelChangedAfterTheLinksBoundStillResumes() {
        try (ActiveChild h = new ActiveChild()) {
            h.channel.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, 100);
            h.reliable().setQueuedBytes(100);
            h.channel.writeAndFlush(payload(1));
            assertFalse(h.channel.isWritable());
            h.reliable().drain(100);
            h.run();
            assertTrue(h.channel.isWritable());
            assertEquals(1, h.reliable().sendCount());
        }
    }

    @Test
    void aBacklogPastItsCapEndsTheConnectionWithQueueTooLong() {
        try (ActiveChild h = new ActiveChild(child -> {
            child.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, 100);
            child.config().setOption(NetherChannelOption.NETHER_MAX_QUEUED_BYTES, 1000);
        })) {
            h.reliable().setQueuedBytes(100);
            // Netty counts 96 bytes of overhead per waiting message: 850 + 96 stays within the cap.
            ChannelFuture within = h.channel.writeAndFlush(Unpooled.buffer().writeZero(850));
            assertTrue(h.channel.isOpen());
            h.events.add("written");
            ChannelFuture over = h.channel.writeAndFlush(payload(1));
            h.run();
            assertFalse(h.channel.isOpen());
            assertEquals(List.of("active", "written",
                    TransportDisconnectReason.QUEUE_TOO_LONG.name(), "inactive"), h.events);
            assertFalse(within.isSuccess());
            assertFalse(over.isSuccess());
            assertEquals(0, h.reliable().sendCount());
        }
    }

    @Test
    @Timeout(30)
    void concurrentEngineDrainsDoNotStrandPendingWrites() throws Exception {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ChildFixture fixture = new ChildFixture(null);
        fixture.engine.autoAcceptSends(true);
        fixture.child.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, 4096);
        FakeDataLink link = fixture.openLinks().reliable;
        Thread engine = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                link.drain(link.queuedBytes());
                Thread.onSpinWait();
            }
        }, "test-engine-drain");
        engine.setDaemon(true);
        byte[] atPauseLevel = new byte[4096];
        try {
            group.register(fixture.child).sync();
            engine.start();
            for (int attempt = 0; attempt < 2000; attempt++) {
                fixture.child.write(Unpooled.wrappedBuffer(atPauseLevel));
                var tail = fixture.child.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{1}));
                assertTrue(tail.await(3, TimeUnit.SECONDS), "a drained engine queue must resume queued writes");
                assertTrue(tail.isSuccess(), () -> String.valueOf(tail.cause()));
            }
        } finally {
            fixture.child.close().syncUninterruptibly();
            engine.interrupt();
            engine.join(1000);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}

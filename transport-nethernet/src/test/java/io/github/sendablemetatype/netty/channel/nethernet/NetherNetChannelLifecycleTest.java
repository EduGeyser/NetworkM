package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Inbound queueing, read demand, registration order and close cleanup. */
@Timeout(10)
class NetherNetChannelLifecycleTest {

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    @AfterEach
    void tearDown() {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aTextSegmentIsReadAsGameData() {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.text(h.reliable(), (byte) 1, (byte) 5);
            h.fixture.reliable(0, 6);
            h.run();
            assertEquals(List.of("active", "read:5,6"), h.events);
        }
    }

    @Test
    void rejectedInboundTaskIsRetriedForQuietConnection() throws Exception {
        InboundRecoveryLoop loop = new InboundRecoveryLoop();
        ChildFixture fixture = new ChildFixture(null).openLinks();
        LinkedBlockingQueue<Integer> received = captureInboundValues(fixture);
        try {
            loop.register(fixture.child).sync();
            loop.submit(() -> { }).sync();
            deliverFromRejectedThread(fixture, 1);
            assertEquals(1, received.poll(3, TimeUnit.SECONDS));
            assertTrue(fixture.child.isOpen());
            assertTrue(fixture.child.isActive());
        } finally {
            fixture.child.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void freshInboundEventBypassesRejectedRecovery(boolean manualRead) throws Exception {
        InboundRecoveryLoop loop = new InboundRecoveryLoop();
        ChildFixture fixture = new ChildFixture(null).openLinks();
        LinkedBlockingQueue<Integer> received = captureInboundValues(fixture);
        try {
            loop.register(fixture.child).sync();
            loop.submit(() -> {
                // Recovery cannot run while this task owns the event loop.
                deliverFromRejectedThread(fixture, 1);
                if (manualRead) {
                    fixture.child.config().setAutoRead(false);
                    fixture.reliable(0, 2);
                }
                int scheduled = loop.ownerSubmissions;
                if (manualRead) {
                    fixture.child.read();
                } else {
                    fixture.reliable(0, 2);
                }
                assertEquals(scheduled + 1, loop.ownerSubmissions);
                return null;
            }).sync();
            assertEquals(1, received.poll(3, TimeUnit.SECONDS));
            if (manualRead) {
                loop.submit(() -> { }).sync();
                assertTrue(received.isEmpty());
                fixture.child.read();
            }
            assertEquals(2, received.poll(3, TimeUnit.SECONDS));
            assertTrue(fixture.child.isOpen());
        } finally {
            fixture.child.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @Test
    void inboundOverflowStillClosesWhenTheExecutorRejectsCallbacks() throws Exception {
        InboundRecoveryLoop loop = new InboundRecoveryLoop();
        ChildFixture fixture = new ChildFixture(null).openLinks();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator);
        try {
            loop.register(fixture.child).sync();
            loop.submit(() -> {
                Thread engine = new Thread(() -> {
                    for (int i = 0; i <= 512; i++) {
                        fixture.reliable(0, 1);
                    }
                }, "engine-inbound");
                engine.start();
                engine.join(2000);
                assertFalse(engine.isAlive());
                assertEquals(0, allocator.metric().usedHeapMemory());
                return null;
            }).sync();
            assertTrue(fixture.child.closeFuture().await(3, TimeUnit.SECONDS));
            assertFalse(fixture.child.isOpen());
        } finally {
            fixture.child.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @Test
    void closeReleasesInboundMessagesAwaitingRecovery() throws Exception {
        InboundRecoveryLoop loop = new InboundRecoveryLoop();
        ChildFixture fixture = new ChildFixture(null).openLinks();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator);
        try {
            loop.register(fixture.child).sync();
            loop.submit(() -> {
                deliverFromRejectedThread(fixture, 1);
                assertTrue(allocator.metric().usedHeapMemory() > 0);
                fixture.child.close();
                assertEquals(0, allocator.metric().usedHeapMemory());
                return null;
            }).sync();
            assertFalse(fixture.child.isOpen());
        } finally {
            fixture.child.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private static LinkedBlockingQueue<Integer> captureInboundValues(ChildFixture fixture) {
        LinkedBlockingQueue<Integer> received = new LinkedBlockingQueue<>();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                try {
                    received.add((int) ((ByteBuf) msg).readUnsignedByte());
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }
        });
        return received;
    }

    private static void deliverFromRejectedThread(ChildFixture fixture, int value) throws InterruptedException {
        Thread engine = new Thread(() -> fixture.reliable(0, (byte) value), "engine-inbound");
        engine.start();
        engine.join(2000);
        assertFalse(engine.isAlive());
    }

    private static final class InboundRecoveryLoop extends DefaultEventLoop {
        int ownerSubmissions;

        @Override public void execute(Runnable task) {
            if (Thread.currentThread().getName().equals("engine-inbound")) {
                throw new RejectedExecutionException("task queue is full");
            }
            if (inEventLoop()) {
                ownerSubmissions++;
            }
            super.execute(task);
        }
    }

    @Test
    void transportSendFailureFailsTheWriteAndReleasesItsBuffer() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        IllegalStateException failure = new IllegalStateException("transport rejected send");
        fixture.reliable.refuseSends(failure);
        ByteBuf payload = Unpooled.buffer().writeByte(1);
        try {
            group.register(fixture.child).sync();
            ChannelFuture write = fixture.child.writeAndFlush(payload).await();

            assertFalse(write.isSuccess());
            assertSame(failure, write.cause().getCause());
            assertEquals(0, payload.refCnt());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void unsupportedWritesFailInsteadOfBeingSilentlyDropped() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        try {
            group.register(fixture.child).sync();
            ChannelFuture write = fixture.child.writeAndFlush("not a byte buffer").await();

            assertFalse(write.isSuccess());
            assertInstanceOf(UnsupportedOperationException.class, write.cause());
            assertEquals(0, fixture.reliable.sendCount());
            assertTrue(fixture.child.isOpen());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void aChildWithoutDataChannelsCannotReportASuccessfulWrite() throws Exception {
        ChildFixture fixture = new ChildFixture(null);
        ByteBuf payload = Unpooled.buffer().writeByte(1);
        try {
            fixture.child.markTransportOpen();
            group.register(fixture.child).sync();
            ChannelFuture write = fixture.child.writeAndFlush(payload).await();

            assertFalse(write.isSuccess());
            assertInstanceOf(IllegalStateException.class, write.cause().getCause());
            assertEquals(0, payload.refCnt());
            assertTrue(fixture.child.closeFuture().await(2, TimeUnit.SECONDS));
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void inboundCallbackQueuedBehindCloseIsReleasedWithoutDelivery(boolean reliable) throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator);
        AtomicInteger reads = new AtomicInteger();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                reads.incrementAndGet();
                ReferenceCountUtil.release(msg);
            }
        });
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        try {
            group.register(fixture.child).sync();
            fixture.child.eventLoop().execute(() -> awaitRelease(blocked, resume));
            assertTrue(blocked.await(2, TimeUnit.SECONDS));
            ChannelFuture close = fixture.child.close();
            if (reliable) {
                fixture.reliable(0, 1);
            } else {
                fixture.unreliable(0, 1);
            }
            assertTrue(allocator.metric().usedHeapMemory() > 0);
            resume.countDown();
            close.sync();
            fixture.child.eventLoop().submit(() -> { }).sync();

            assertEquals(0, reads.get());
            assertEquals(0, allocator.metric().usedHeapMemory());
        } finally {
            resume.countDown();
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void aChildClosedBeforeRegistrationClosesALatePeer() throws Exception {
        ChildFixture parent = new ChildFixture(null);
        try {
            group.register(parent.child).sync();
            NetherNetChildChannel child = ChildFixture.child(parent.child, null, null);
            AtomicInteger closeNotifications = new AtomicInteger();
            child.closeFuture().addListener(future -> closeNotifications.incrementAndGet());

            child.close().sync();
            var late = (io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer)
                    child.openPeer(parent.engine, ChildFixture.ANSWERER);

            assertFalse(child.isRegistered());
            assertFalse(child.isOpen());
            assertTrue(child.closeFuture().isSuccess());
            assertEquals(1, closeNotifications.get());
            assertEquals(1, late.closeCalls());
        } finally {
            parent.child.close().syncUninterruptibly();
        }
    }

    @Test
    void anUnregisteredChildClosesItsPeerOnce() throws Exception {
        ChildFixture parent = new ChildFixture(null);
        try {
            group.register(parent.child).sync();
            ChildFixture fixture = new ChildFixture(parent.child);

            fixture.child.close().sync();
            fixture.child.close().sync();

            assertEquals(1, fixture.peer.closeCalls());
            assertTrue(fixture.child.closeFuture().isSuccess());
            assertTrue(fixture.child.termination().toCompletableFuture().isDone());
        } finally {
            parent.child.close().syncUninterruptibly();
        }
    }

    @Test
    void inboundQueuedBeforeRegistrationDoesNotOutliveCloseDuringActivation() throws Exception {
        ChildFixture parent = new ChildFixture(null);
        MultiThreadIoEventLoopGroup workers = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ChildFixture fixture = new ChildFixture(parent.child);
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator);
        AtomicInteger reads = new AtomicInteger();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                ctx.close();
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                reads.incrementAndGet();
                ReferenceCountUtil.release(message);
            }
        });
        CountDownLatch parentBlocked = new CountDownLatch(1);
        CountDownLatch parentResume = new CountDownLatch(1);
        CountDownLatch workerBlocked = new CountDownLatch(1);
        CountDownLatch workerResume = new CountDownLatch(1);
        try {
            group.register(parent.child).sync();
            parent.child.eventLoop().execute(() -> awaitRelease(parentBlocked, parentResume));
            assertTrue(parentBlocked.await(2, TimeUnit.SECONDS));
            fixture.reliable = fixture.peer.remoteChannel(
                    io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters.reliable(
                            NetherNetConstants.RELIABLE_CHANNEL_LABEL));
            fixture.reliable(0, 1);

            workers.register(fixture.child).sync();
            fixture.child.eventLoop().execute(() -> awaitRelease(workerBlocked, workerResume));
            assertTrue(workerBlocked.await(2, TimeUnit.SECONDS));
            fixture.unreliable = fixture.peer.remoteChannel(
                    io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters.unreliable(
                            NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
            fixture.peer.open(fixture.reliable);
            parentResume.countDown();
            parent.child.eventLoop().submit(() -> { }).sync();
            workerResume.countDown();
            fixture.child.closeFuture().sync();
            fixture.child.eventLoop().submit(() -> { }).sync();

            assertEquals(0, reads.get());
            assertEquals(0, allocator.metric().usedHeapMemory());
        } finally {
            parentResume.countDown();
            workerResume.countDown();
            fixture.child.close().syncUninterruptibly();
            parent.child.close().syncUninterruptibly();
            workers.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void inboundWaitsForRegistrationAndActivationInArrivalOrder(boolean transportOpensFirst) throws Exception {
        ChildFixture parent = new ChildFixture(null);
        ChildFixture fixture = new ChildFixture(parent.child);
        List<String> events = new ArrayList<>();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRegistered(ChannelHandlerContext ctx) {
                events.add("registered");
                ctx.fireChannelRegistered();
            }

            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                events.add("active");
                ctx.fireChannelActive();
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                ByteBuf buffer = (ByteBuf) message;
                try {
                    events.add("read " + buffer.readUnsignedByte());
                } finally {
                    buffer.release();
                }
            }
        });
        try {
            group.register(parent.child).sync();
            fixture.reliable = fixture.peer.remoteChannel(
                    io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters.reliable(
                            NetherNetConstants.RELIABLE_CHANNEL_LABEL));
            fixture.unreliable = fixture.peer.remoteChannel(
                    io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters.unreliable(
                            NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
            if (transportOpensFirst) {
                fixture.peer.open(fixture.reliable);
            }
            fixture.reliable(0, 1);
            parent.child.eventLoop().submit(() -> { }).sync();
            assertTrue(events.isEmpty(), "An unregistered child's pipeline is not ready to receive data");

            group.register(fixture.child).sync();
            fixture.child.eventLoop().submit(() -> { }).sync();
            if (!transportOpensFirst) {
                assertEquals(List.of("registered"), events);
                fixture.peer.open(fixture.reliable);
            }
            fixture.reliable(0, 2);
            fixture.child.eventLoop().submit(() -> { }).sync();

            assertEquals(List.of("registered", "active", "read 1", "read 2"), events);
        } finally {
            fixture.child.close().syncUninterruptibly();
            parent.child.close().syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void pendingInboundIsBoundedAndReleasedOnClose(boolean exceedByteLimit) throws Exception {
        ChildFixture parent = new ChildFixture(null);
        ChildFixture fixture = new ChildFixture(parent.child);
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator);
        fixture.child.config().setOption(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, 1024);
        fixture.openLinks();
        try {
            group.register(parent.child).sync();
            if (exceedByteLimit) {
                fixture.reliable(segment(1024));
                fixture.reliable(segment(1024));
            } else {
                for (int i = 0; i < 512; i++) {
                    fixture.reliable(0, 1);
                }
            }
            parent.child.eventLoop().submit(() -> { }).sync();
            assertTrue(fixture.child.isOpen());
            assertTrue(allocator.metric().usedHeapMemory() > 0);

            fixture.reliable(0, 2);
            fixture.child.closeFuture().sync();

            assertFalse(fixture.child.isOpen());
            assertEquals(0, allocator.metric().usedHeapMemory());
        } finally {
            fixture.child.close().syncUninterruptibly();
            parent.child.close().syncUninterruptibly();
        }
    }

    @Test
    void manualReadFinishesOneMessageAndLeavesTheNextQueued() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        fixture.child.config().setAutoRead(false);
        LinkedBlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                ByteBuf buffer = (ByteBuf) message;
                try {
                    messages.add(ByteBufUtil.getBytes(buffer));
                } finally {
                    buffer.release();
                }
            }
        });
        try {
            group.register(fixture.child).sync();
            fixture.reliable(1, 11);
            fixture.reliable(0, 12);
            fixture.reliable(0, 13);
            fixture.child.eventLoop().submit(() -> { }).sync();
            assertTrue(messages.isEmpty());

            fixture.child.read();
            assertArrayEquals(new byte[]{11, 12}, messages.poll(2, TimeUnit.SECONDS));
            assertNull(messages.poll(100, TimeUnit.MILLISECONDS));

            fixture.child.read();
            assertArrayEquals(new byte[]{13}, messages.poll(2, TimeUnit.SECONDS));
            assertTrue(messages.isEmpty());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void disablingAutoReadClearsAutomaticDemand(boolean queuedToggle) throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        LinkedBlockingQueue<Integer> messages = new LinkedBlockingQueue<>();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                ByteBuf buffer = (ByteBuf) message;
                try {
                    messages.add((int) buffer.readUnsignedByte());
                } finally {
                    buffer.release();
                }
            }
        });
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        try {
            group.register(fixture.child).sync();
            fixture.child.eventLoop().submit(() -> { }).sync();
            if (queuedToggle) {
                fixture.child.config().setAutoRead(false);
                fixture.child.eventLoop().execute(() -> awaitRelease(blocked, resume));
                assertTrue(blocked.await(2, TimeUnit.SECONDS));
                fixture.child.config().setAutoRead(true);
                fixture.child.config().setAutoRead(false);
            } else {
                fixture.child.eventLoop().submit(() -> fixture.child.config().setAutoRead(false)).sync();
            }
            fixture.reliable(0, 21);
            fixture.reliable(0, 22);
            resume.countDown();
            fixture.child.eventLoop().submit(() -> { }).sync();
            fixture.child.eventLoop().submit(() -> { }).sync();
            assertTrue(messages.isEmpty());

            fixture.child.read();
            assertEquals(21, messages.poll(2, TimeUnit.SECONDS));
            assertNull(messages.poll(100, TimeUnit.MILLISECONDS));
            fixture.child.read();
            assertEquals(22, messages.poll(2, TimeUnit.SECONDS));
        } finally {
            resume.countDown();
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void enablingAutoReadDrainsQueuedMessagesInFairBatches() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        fixture.child.config().setAutoRead(false);
        List<Integer> messages = new ArrayList<>();
        AtomicInteger completions = new AtomicInteger();
        AtomicInteger countAtMarker = new AtomicInteger();
        CountDownLatch received = new CountDownLatch(192);
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                ByteBuf buffer = (ByteBuf) message;
                try {
                    messages.add(buffer.readInt());
                } finally {
                    buffer.release();
                    received.countDown();
                }
            }

            @Override
            public void channelReadComplete(ChannelHandlerContext ctx) {
                completions.incrementAndGet();
            }
        });
        try {
            group.register(fixture.child).sync();
            for (int i = 0; i < 192; i++) {
                fixture.reliable(ByteBuffer.allocate(5).put((byte) 0).putInt(i).array());
            }
            fixture.child.eventLoop().submit(() -> {
                fixture.child.config().setAutoRead(true);
                fixture.child.eventLoop().execute(() -> countAtMarker.set(messages.size()));
            }).sync();
            assertTrue(received.await(2, TimeUnit.SECONDS));
            fixture.child.eventLoop().submit(() -> { }).sync();

            assertEquals(java.util.stream.IntStream.range(0, 192).boxed().toList(), messages);
            assertTrue(countAtMarker.get() > 0 && countAtMarker.get() < 192);
            assertTrue(completions.get() > 1 && completions.get() < 192);
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void anActivePausedReaderCannotAccumulateAnUnboundedBacklog() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator).setAutoRead(false);
        try {
            group.register(fixture.child).sync();
            for (int i = 0; i < 513; i++) {
                fixture.reliable(0, 1);
            }
            assertTrue(fixture.child.closeFuture().await(2, TimeUnit.SECONDS));
            assertFalse(fixture.child.isOpen());
            assertEquals(0, allocator.metric().usedHeapMemory());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void aPausedReaderCanQueueTwoMessagesOfTheReassemblyCap() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator).setAutoRead(false);
        fixture.child.config().setOption(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, 16 * 1024);
        LinkedBlockingQueue<ByteBuf> messages = new LinkedBlockingQueue<>();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                messages.add((ByteBuf) message);
            }
        });
        try {
            group.register(fixture.child).sync();
            for (int message = 0; message < 2; message++) {
                for (int countdown = 15; countdown >= 0; countdown--) {
                    byte[] segment = segment(1024);
                    segment[0] = (byte) countdown;
                    fixture.reliable(segment);
                }
            }
            assertTrue(fixture.child.isOpen());
            assertTrue(messages.isEmpty());
            for (int message = 0; message < 2; message++) {
                fixture.child.read();
                ByteBuf received = messages.poll(3, TimeUnit.SECONDS);
                assertNotNull(received);
                try {
                    assertEquals(16 * 1024, received.readableBytes());
                } finally {
                    received.release();
                }
            }
            fixture.child.eventLoop().submit(() -> { }).sync();
            assertEquals(0, allocator.metric().usedHeapMemory());
        } finally {
            fixture.child.close().syncUninterruptibly();
            messages.forEach(ByteBuf::release);
        }
    }

    @Test
    void bothDataChannelsShareTheInboundQueueBudget() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        fixture.child.config().setAllocator(allocator).setAutoRead(false);
        try {
            group.register(fixture.child).sync();
            for (int message = 0; message < 512; message++) {
                if (message % 2 == 0) {
                    fixture.reliable(0, 1);
                } else {
                    fixture.unreliable(0, 1);
                }
            }
            assertTrue(fixture.child.isOpen());
            assertTrue(allocator.metric().usedHeapMemory() > 0);

            fixture.unreliable(0, 2);
            assertTrue(fixture.child.closeFuture().await(2, TimeUnit.SECONDS));
            assertEquals(0, allocator.metric().usedHeapMemory());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void unreliableMessagesArriveAsPacketsApartFromReliableReassembly() throws Exception {
        ChildFixture fixture = new ChildFixture(null).openLinks();
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                try {
                    messages.add(message instanceof NetherNetPacket packet
                            ? "unreliable " + ByteBufUtil.hexDump(packet.content())
                            : "reliable " + ByteBufUtil.hexDump((ByteBuf) message));
                } finally {
                    ReferenceCountUtil.release(message);
                }
            }
        });
        try {
            group.register(fixture.child).sync();
            fixture.reliable(1, 0x11);
            fixture.unreliable(0, 0x2a);
            fixture.unreliable(1, 0x2b);
            fixture.unreliable(0);
            fixture.reliable(0, 0x12);
            assertEquals("unreliable 2a", messages.poll(2, TimeUnit.SECONDS));
            assertEquals("reliable 1112", messages.poll(2, TimeUnit.SECONDS));
            assertNull(messages.poll(100, TimeUnit.MILLISECONDS), "split or empty unreliable messages are dropped");
            assertTrue(fixture.child.isOpen());
        } finally {
            fixture.child.close().syncUninterruptibly();
        }
    }

    @Test
    void outgoingLimitUsesTheDefaultAndCapsCallerOverrides() {
        ChildFixture fixture = new ChildFixture(null);
        assertEquals(65536, fixture.child.getMaxOutboundMessageSize());
        fixture.child.setMaxOutboundMessageSize(0);
        assertEquals(262144, fixture.child.getMaxOutboundMessageSize());
        fixture.child.setMaxOutboundMessageSize(Integer.MAX_VALUE);
        assertEquals(262144, fixture.child.getMaxOutboundMessageSize());
        fixture.child.setMaxOutboundMessageSize(2);
        assertEquals(2, fixture.child.getMaxOutboundMessageSize());
        assertThrows(IllegalArgumentException.class, () -> fixture.child.setMaxOutboundMessageSize(1));
        assertThrows(IllegalArgumentException.class, () -> fixture.child.setMaxOutboundMessageSize(-1));
        assertEquals(2, fixture.child.getMaxOutboundMessageSize());
        fixture.child.unsafe().closeForcibly();
    }

    private static byte[] segment(int payload) {
        return new byte[1 + payload];
    }

    private static void awaitRelease(CountDownLatch blocked, CountDownLatch release) {
        blocked.countDown();
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

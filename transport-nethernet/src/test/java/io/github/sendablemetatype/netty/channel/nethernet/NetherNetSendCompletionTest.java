package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultEventLoop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.sendablemetatype.netty.channel.nethernet.ActiveChild.payload;
import static org.junit.jupiter.api.Assertions.*;

/** A write completes once the engine accepted every segment, in write order. */
@Timeout(15)
class NetherNetSendCompletionTest {
    private static final int PAUSE = 1000;

    @Test
    void failedSizeEstimateDoesNotLeaveAMessageInTheSubmissionQueue() {
        try (ActiveChild h = new ActiveChild()) {
            IllegalArgumentException failure = new IllegalArgumentException("size estimate failed");
            h.channel.config().setMessageSizeEstimator(() -> message -> {
                ByteBuf content = ((ByteBufHolder) message).content();
                if (content.getUnsignedByte(content.readerIndex()) == 2) {
                    throw failure;
                }
                return content.readableBytes();
            });
            ChannelFuture first = h.channel.write(payload(1));
            ByteBuf rejected = payload(2);
            ChannelFuture rejectedWrite = h.channel.write(rejected);
            ChannelFuture last = h.channel.writeAndFlush(payload(3));
            assertSame(failure, rejectedWrite.cause());
            assertEquals(0, rejected.refCnt());
            assertEquals(2, h.reliable().sendCount());
            assertArrayEquals(new byte[]{0, 1}, h.reliable().sentBytes().get(0));
            assertArrayEquals(new byte[]{0, 3}, h.reliable().sentBytes().get(1));
            h.reliable().acceptAll();
            h.run();
            assertTrue(first.isSuccess());
            assertTrue(last.isSuccess());
        }
    }

    @Test
    void cancelledTailPreservesTheFlushBoundaryDuringBackpressure() {
        try (ActiveChild h = paced()) {
            h.reliable().setQueuedBytes(PAUSE);
            h.channel.write(payload(1));
            ByteBuf cancelled = payload(2);
            ChannelFuture cancellation = h.channel.write(cancelled);
            assertTrue(cancellation.cancel(false));
            h.channel.flush();
            assertEquals(0, cancelled.refCnt());
            assertEquals(0, h.reliable().sendCount());
            ChannelFuture unflushed = h.channel.write(payload(3));
            h.reliable().drain(PAUSE);
            h.run();
            assertEquals(1, h.reliable().sendCount());
            assertArrayEquals(new byte[]{0, 1}, h.reliable().sentBytes().get(0));
            assertFalse(unflushed.isDone());
            h.channel.flush();
            assertEquals(2, h.reliable().sendCount());
            assertArrayEquals(new byte[]{0, 3}, h.reliable().sentBytes().get(1));
        }
    }

    @Test
    void completionListenerCanLeaveANewWriteUnflushed() {
        try (ActiveChild h = new ActiveChild()) {
            ChannelFuture first = h.channel.writeAndFlush(payload(1));
            List<ChannelFuture> next = new ArrayList<>();
            first.addListener(ignored -> next.add(h.channel.write(payload(2))));
            h.reliable().accept(0);
            h.run();
            assertEquals(1, next.size());
            assertEquals(1, h.reliable().sendCount());
            assertFalse(next.getFirst().isDone());
            h.channel.flush();
            assertEquals(2, h.reliable().sendCount());
            h.reliable().accept(1);
            h.run();
            assertTrue(next.getFirst().isSuccess());
        }
    }

    @Test
    void writesWaitForAcceptanceAndCompleteInOrder() {
        try (ActiveChild h = new ActiveChild()) {
            ByteBuf first = payload(1);
            ByteBuf second = payload(2);
            ChannelFuture a = h.channel.write(first);
            ChannelFuture b = h.channel.writeAndFlush(second);
            assertEquals(2, h.reliable().sendCount());
            assertFalse(a.isDone());
            assertFalse(b.isDone());
            assertEquals(0, first.refCnt(), "the engine holds its own copy once send returned");
            h.reliable().accept(1);
            h.run();
            assertFalse(b.isDone());
            h.reliable().accept(0);
            h.run();
            assertTrue(a.isSuccess());
            assertTrue(b.isSuccess());
            assertEquals(0, second.refCnt());
        }
    }

    @Test
    void eachFlushSubmitsImmediatelyWhileEarlierSendsArePending() {
        try (ActiveChild h = new ActiveChild()) {
            List<ChannelFuture> writes = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                writes.add(h.channel.writeAndFlush(payload(i)));
                assertEquals(i + 1, h.reliable().sendCount());
            }
            assertTrue(writes.stream().noneMatch(ChannelFuture::isDone));
            h.reliable().acceptAll();
            h.run();
            assertTrue(writes.stream().allMatch(ChannelFuture::isSuccess));
        }
    }

    @Test
    void acceptanceDoesNotCountAsQueueDrain() {
        try (ActiveChild h = paced()) {
            h.reliable().setQueuedBytes(PAUSE - 2);
            ChannelFuture first = h.channel.write(payload(1));
            ChannelFuture second = h.channel.writeAndFlush(payload(2));
            assertEquals(1, h.reliable().sendCount(), "the first segment fills the engine queue");
            h.reliable().accept(0);
            h.run();
            assertTrue(first.isSuccess());
            assertFalse(second.isDone());
            assertEquals(1, h.reliable().sendCount());
            h.reliable().drain(PAUSE);
            h.run();
            assertEquals(2, h.reliable().sendCount());
            assertFalse(second.isDone());
            h.reliable().accept(1);
            h.run();
            assertTrue(second.isSuccess());
        }
    }

    @Test
    void completionListenersSubmitTheirNextBatchAfterDraining() {
        try (ActiveChild h = new ActiveChild()) {
            List<ChannelFuture> writes = new ArrayList<>();
            AtomicInteger accepted = new AtomicInteger();
            for (int i = 0; i < 20; i++) {
                ChannelFuture write = h.channel.write(payload(i));
                write.addListener(ignored -> {
                    accepted.incrementAndGet();
                    writes.add(h.channel.writeAndFlush(payload(42)));
                });
            }
            h.channel.flush();
            h.reliable().acceptAll();
            h.run();
            assertEquals(20, accepted.get());
            assertEquals(20, writes.size());
            assertEquals(40, h.reliable().sendCount(), "every listener's write went out after the batch completed");
            assertTrue(writes.stream().noneMatch(ChannelFuture::isDone));
            h.reliable().acceptAll();
            h.run();
            assertTrue(writes.stream().allMatch(ChannelFuture::isSuccess));
        }
    }

    @Test
    void queueDrainCanArriveBeforeAcceptance() {
        try (ActiveChild h = paced()) {
            ChannelFuture first = h.channel.write(ByteBufZeros.of(PAUSE));
            ChannelFuture second = h.channel.writeAndFlush(payload(1));
            assertEquals(1, h.reliable().sendCount());
            h.reliable().drain(PAUSE + 1);
            h.run();
            assertEquals(2, h.reliable().sendCount());
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            h.reliable().acceptAll();
            h.run();
            assertTrue(first.isSuccess());
            assertTrue(second.isSuccess());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void resumingWritesDoesNotFlushNewUnflushedMessages(boolean atWatermark) {
        try (ActiveChild h = paced()) {
            h.channel.writeAndFlush(atWatermark ? ByteBufZeros.of(PAUSE) : payload(1));
            ChannelFuture flushed = h.channel.writeAndFlush(payload(2));
            ChannelFuture unflushed = h.channel.write(payload(3));
            h.reliable().accept(0);
            if (atWatermark) {
                h.reliable().drain(PAUSE + 1);
            }
            h.run();
            assertEquals(2, h.reliable().sendCount());
            h.reliable().accept(1);
            h.run();
            assertTrue(flushed.isSuccess());
            assertFalse(unflushed.isDone());
            h.channel.flush();
            assertEquals(3, h.reliable().sendCount());
            h.reliable().accept(2);
            h.run();
            assertTrue(unflushed.isSuccess());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aRefusalClosesAndReleasesFlushedAndUnflushedWrites(boolean firstAccepted) {
        try (ActiveChild h = new ActiveChild()) {
            ByteBuf first = payload(1);
            ByteBuf second = payload(2);
            ByteBuf unflushed = payload(3);
            ChannelFuture a = h.channel.write(first);
            ChannelFuture b = h.channel.writeAndFlush(second);
            ChannelFuture c = h.channel.write(unflushed);
            IOException failure = new IOException("engine queue refused the segment");
            if (firstAccepted) {
                h.reliable().accept(0);
            }
            h.reliable().reject(1, failure);
            h.run();
            assertEquals(firstAccepted, a.isSuccess());
            assertTrue(a.isDone());
            assertSame(failure, b.cause());
            assertTrue(c.isDone());
            assertFalse(c.isSuccess());
            assertEquals(0, first.refCnt());
            assertEquals(0, second.refCnt());
            assertEquals(0, unflushed.refCnt());
            assertFalse(h.channel.isOpen());
            assertEquals(1, h.peer.closeCalls());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aRefusalClosesEvenWithAutoCloseDisabled(boolean synchronous) {
        try (ActiveChild h = new ActiveChild()) {
            h.channel.config().setAutoClose(false);
            if (synchronous) {
                h.reliable().refuseSends(new IllegalStateException("send refused at once"));
            }
            ByteBuf buffer = payload(1);
            ChannelFuture write = h.channel.writeAndFlush(buffer);
            if (!synchronous) {
                h.reliable().reject(0, new IOException("send refused later"));
            }
            h.run();
            assertTrue(write.isDone());
            assertFalse(write.isSuccess());
            assertFalse(h.channel.isOpen());
            assertTrue(h.channel.closeFuture().isDone());
            assertEquals(0, buffer.refCnt());
        }
    }

    @Test
    void closeFailsPendingWritesAndTheEngineSettlesItsSends() {
        try (ActiveChild h = new ActiveChild()) {
            ByteBuf buffer = payload(1);
            ChannelFuture write = h.channel.writeAndFlush(buffer);
            h.channel.close().syncUninterruptibly();
            h.run();
            assertTrue(write.isDone());
            assertFalse(write.isSuccess());
            assertEquals(0, buffer.refCnt());
            assertEquals(1, h.peer.closeCalls());
            assertTrue(h.reliable().closed());
        }
    }

    @Test
    void cancellationBetweenPendingWritesDoesNotStrandTheQueue() {
        try (ActiveChild h = new ActiveChild()) {
            ChannelFuture first = h.channel.writeAndFlush(payload(1));
            ByteBuf cancelled = payload(2);
            ChannelFuture cancellation = h.channel.write(cancelled);
            assertTrue(cancellation.cancel(false));
            ByteBuf third = payload(3);
            ChannelFuture last = h.channel.writeAndFlush(third);
            h.run();
            assertEquals(2, h.reliable().sendCount());
            assertEquals(0, cancelled.refCnt());
            h.reliable().acceptAll();
            h.run();
            assertTrue(first.isSuccess());
            assertTrue(last.isSuccess());
            assertEquals(0, third.refCnt());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aSplitMessageWaitsForEverySegmentsResult(boolean rejected) {
        try (ActiveChild h = new ActiveChild(child -> child.setMaxOutboundMessageSize(4))) {
            ByteBuf message = payload(1, 2, 3, 4, 5, 6);
            ChannelFuture write = h.channel.writeAndFlush(message);
            assertEquals(2, h.reliable().sendCount());
            h.reliable().accept(0);
            h.run();
            assertFalse(write.isDone());
            if (rejected) {
                h.reliable().reject(1, new IOException("second segment refused"));
            } else {
                h.reliable().accept(1);
            }
            h.run();
            assertTrue(write.isDone());
            assertEquals(!rejected, write.isSuccess());
            assertEquals(0, message.refCnt());
        }
    }

    @Test
    void completionsRunOnTheEventLoop() throws Exception {
        DefaultEventLoop loop = new DefaultEventLoop();
        ChildFixture fixture = new ChildFixture(null).openLinks();
        NetherNetChildChannel channel = fixture.child;
        try {
            loop.register(channel).sync();
            ChannelFuture write = channel.writeAndFlush(payload(1));
            loop.submit(() -> { }).sync();
            CompletableFuture<Boolean> listenerThread = new CompletableFuture<>();
            write.addListener(ignored -> listenerThread.complete(loop.inEventLoop()));
            Thread engine = new Thread(() -> fixture.reliable.accept(0), "engine-send-callback");
            engine.start();
            engine.join(2000);
            assertTrue(listenerThread.get(3, TimeUnit.SECONDS));
            assertTrue(write.isSuccess());
        } finally {
            channel.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @Test
    void loopShutdownFailsPendingWritesBeforeLateCallbacks() throws Exception {
        DefaultEventLoop loop = new DefaultEventLoop();
        ChildFixture fixture = new ChildFixture(null).openLinks();
        NetherNetChildChannel channel = fixture.child;
        ByteBuf buffer = payload(1);
        try {
            loop.register(channel).sync();
            ChannelFuture write = channel.writeAndFlush(buffer);
            loop.submit(() -> { }).sync();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
            assertTrue(write.isDone());
            assertTrue(channel.closeFuture().await(3, TimeUnit.SECONDS));
            assertFalse(write.isSuccess());
            assertEquals(0, buffer.refCnt());
            assertNotEquals("engine-send-callback", fixture.peer.closeThread());
        } finally {
            if (!loop.isTerminated()) {
                channel.close().syncUninterruptibly();
                loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
            }
        }
    }

    @Test
    void aTemporarilyRejectedCompletionTaskIsRetried() throws Exception {
        DefaultEventLoop loop = new DefaultEventLoop() {
            @Override public void execute(Runnable task) {
                if (Thread.currentThread().getName().equals("engine-send-callback")) {
                    throw new RejectedExecutionException("task queue is full");
                }
                super.execute(task);
            }
        };
        ChildFixture fixture = new ChildFixture(null).openLinks();
        NetherNetChildChannel channel = fixture.child;
        ByteBuf buffer = payload(1);
        try {
            loop.register(channel).sync();
            ChannelFuture write = channel.writeAndFlush(buffer);
            loop.submit(() -> { }).sync();
            Thread engine = new Thread(() -> fixture.reliable.accept(0), "engine-send-callback");
            engine.start();
            engine.join(2000);
            assertTrue(write.await(3, TimeUnit.SECONDS));
            assertTrue(write.isSuccess());
            assertTrue(channel.isOpen());
            assertEquals(0, buffer.refCnt());
        } finally {
            channel.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void explicitFlushBypassesRejectedRecoveryWhileEngineSendsRemainPending(boolean atWatermark) throws Exception {
        AtomicInteger rejections = new AtomicInteger();
        DefaultEventLoop loop = new DefaultEventLoop() {
            @Override public void execute(Runnable task) {
                if (Thread.currentThread().getName().equals("engine-send-callback")) {
                    rejections.incrementAndGet();
                    throw new RejectedExecutionException("task queue is full");
                }
                super.execute(task);
            }
        };
        ChildFixture fixture = new ChildFixture(null);
        NetherNetChildChannel channel = fixture.child;
        // Set before the links bind, as their low watermark follows it.
        channel.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, PAUSE);
        FakeDataLink link = fixture.openLinks().reliable;
        try {
            loop.register(channel).sync();
            loop.submit(() -> {
                ChannelFuture first = channel.write(atWatermark ? ByteBufZeros.of(PAUSE) : payload(0));
                ChannelFuture second = channel.writeAndFlush(payload(2));
                ChannelFuture third = channel.write(payload(3));
                assertEquals(atWatermark ? 1 : 2, link.sendCount());

                Thread engine = new Thread(() -> {
                    if (atWatermark) {
                        link.drain(PAUSE + 1);
                    } else {
                        link.accept(1);
                    }
                }, "engine-send-callback");
                engine.start();
                engine.join(2000);
                assertFalse(engine.isAlive());
                assertEquals(1, rejections.get());

                // This task still owns the loop, so recovery cannot run before this flush.
                channel.flush();
                assertEquals(3, link.sendCount());
                assertFalse(first.isDone());
                assertFalse(second.isDone());
                assertFalse(third.isDone());
                return null;
            }).sync();
        } finally {
            channel.close().syncUninterruptibly();
            loop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /** A child that pauses once {@value #PAUSE} bytes wait in the engine queue. */
    private static ActiveChild paced() {
        return new ActiveChild(child -> child.config().setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES, PAUSE));
    }

    /** A message that alone fills the paced engine queue. */
    private static final class ByteBufZeros {
        static ByteBuf of(int bytes) {
            return io.netty.buffer.Unpooled.buffer(bytes).writeZero(bytes);
        }
    }
}

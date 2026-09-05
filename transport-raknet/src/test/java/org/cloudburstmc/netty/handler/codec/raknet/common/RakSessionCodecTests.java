/*
 * Copyright 2026 CloudburstMC
 * Licensed under the Apache License, Version 2.0.
 */

package org.cloudburstmc.netty.handler.codec.raknet.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.RakPriority;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.config.DefaultRakClientConfig;
import org.cloudburstmc.netty.channel.raknet.config.DefaultRakSessionConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelMetrics;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.cloudburstmc.netty.channel.raknet.RakConstants.*;
import static org.junit.jupiter.api.Assertions.*;

class RakSessionCodecTests {
    private static final int DATA_ID = 0xfe;

    @Test
    void sequentialAcksUseOneRangeAndKeepGapsSeparate() {
        try (Session s = new Session(false, 10000)) {
            s.receive();
            s.receive();
            s.receive();
            s.sequence = 5;
            s.receive();
            s.transport.runPendingTasks();
            ByteBuf ack = s.transport.readOutbound();
            try {
                assertEquals((FLAG_VALID | FLAG_ACK) & 0xff, ack.readUnsignedByte());
                assertEquals(2, ack.readUnsignedShort());
                assertFalse(ack.readBoolean());
                assertEquals(0, ack.readUnsignedMediumLE());
                assertEquals(2, ack.readUnsignedMediumLE());
                assertTrue(ack.readBoolean());
                assertEquals(5, ack.readUnsignedMediumLE());
                assertFalse(ack.isReadable());
            } finally {
                ack.release();
            }
            ByteBuf nack = s.transport.readOutbound();
            try {
                assertEquals((FLAG_VALID | FLAG_NACK) & 0xff, nack.readUnsignedByte());
                assertEquals(1, nack.readUnsignedShort());
                assertFalse(nack.readBoolean());
                assertEquals(3, nack.readUnsignedMediumLE());
                assertEquals(4, nack.readUnsignedMediumLE());
            } finally {
                nack.release();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void compatibilityFlagsPreserveDatagramAndFragmentSemantics(boolean compatible) {
        try (Session s = new Session(false, 10000, true, compatible)) {
            EncapsulatedPacket packet = s.codec.createEncapsulatedPacket();
            packet.setBuffer(Unpooled.buffer(1).writeByte(DATA_ID));
            packet.setReliability(RakReliability.RELIABLE);
            packet.setSplit(true);
            packet.setPartIndex(0);
            RakDatagramPacket first = s.codec.createDatagramPacket();
            assertTrue(first.tryAddPacket(packet, 1400));
            try {
                assertEquals(!compatible, packet.isNeedsBAS());
                assertEquals((FLAG_VALID | FLAG_NEEDS_B_AND_AS) & 0xff, first.getFlags() & 0xff);
            } finally {
                first.release();
            }
            RakDatagramPacket later = s.codec.createDatagramPacket();
            assertTrue(later.tryAddPacket(part(0, 1), 1400));
            try {
                assertNotEquals(0, later.getFlags() & FLAG_CONTINUOUS_SEND);
            } finally {
                later.release();
            }
        }
    }

    @Test
    void compatiblePingUsesItsWireTimestampWithoutBreakingElapsedTime() {
        try (Session s = new Session(false, 10000, true, true)) {
            s.write(100, RakPriority.IMMEDIATE);
            s.write(100, RakPriority.IMMEDIATE);
            s.readData();
            long before = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            s.codec.writePing(s.transport.pipeline().context(RakSessionCodec.NAME), 0);
            RakDatagramPacket ping = s.transport.readOutbound();
            long timestamp;
            try {
                ByteBuf data = ping.getPackets().get(0).getBuffer();
                assertEquals(ID_CONNECTED_PING, data.readUnsignedByte());
                timestamp = data.readLong();
                assertTrue(timestamp >= before);
                assertTrue(timestamp <= TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
            } finally {
                ping.release();
            }
            s.advance(25);
            s.codec.recalculatePongTime(timestamp);
            assertEquals(25, s.codec.getPing());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void autoFlushDefaultsOffForServerAndClientSessions(boolean client) {
        try (Session s = new Session(null, 10000, client)) {
            assertFalse(s.config.isAutoFlush());
            assertFalse(s.config.getOption(RakChannelOption.RAK_AUTO_FLUSH));
            s.write(100, RakPriority.NORMAL);
            s.advance(100);
            assertTrue(s.readData().isEmpty());
            s.transport.flush();
            assertEquals(1, s.readData().size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void explicitFlushSendsImmediatelyAndPreservesAutomaticFlushing(boolean writeAndFlush) {
        try (Session s = new Session(true, 10000)) {
            s.config.setFlushInterval(10);
            if (writeAndFlush) {
                ByteBuf payload = Unpooled.buffer(100).writeByte(DATA_ID).writeZero(99);
                s.transport.writeAndFlush(new RakMessage(payload, RakReliability.RELIABLE, RakPriority.NORMAL));
            } else {
                s.write(100, RakPriority.NORMAL);
                s.transport.flush();
            }
            assertEquals(1, s.readData().size(), "Explicit flush must not wait for the auto-flush deadline");
            assertTrue(s.config.isAutoFlush());
            assertTrue(s.transport.runScheduledPendingTasks() >= TimeUnit.SECONDS.toNanos(1),
                    "The explicit flush should cancel the now-unnecessary automatic flush");

            s.advance(5);
            s.write(100, RakPriority.NORMAL);
            s.advance(5);
            assertTrue(s.readData().isEmpty());
            s.advance(5);
            assertEquals(1, s.readData().size(), "Later writes must still auto-flush");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shorterTimeoutAppliesToActiveServerAndClientSessions(boolean client) {
        try (Session s = new Session(false, 10000, client)) {
            s.advance(100);
            s.config.setSessionTimeout(150);
            s.advance(49);
            assertTrue(s.transport.isOpen());
            s.advance(1);
            assertFalse(s.transport.isOpen());
        }
    }

    @Test
    void increasingTimeoutReplacesOldDeadline() {
        try (Session s = new Session(false, 50)) {
            s.advance(25);
            s.config.setSessionTimeout(200);
            s.advance(174);
            assertTrue(s.transport.isOpen());
            s.advance(1);
            assertFalse(s.transport.isOpen());
        }
    }

    @Test
    void inboundActivityMovesIdleDeadline() {
        try (Session s = new Session(false, 100)) {
            s.advance(90);
            s.receive();
            s.advance(99);
            assertTrue(s.transport.isOpen());
            s.advance(1);
            assertFalse(s.transport.isOpen());
        }
    }

    @Test
    void enablingAutoFlushSendsAlreadyQueuedData() {
        try (Session s = new Session(false, 10000)) {
            s.write(100, RakPriority.NORMAL);
            s.advance(100);
            assertTrue(s.readData().isEmpty());
            s.config.setFlushInterval(10);
            s.config.setAutoFlush(true);
            s.advance(9);
            assertTrue(s.readData().isEmpty());
            s.advance(1);
            assertEquals(1, s.readData().size());
        }
    }

    @Test
    void disablingAutoFlushCancelsPendingWorkButAllowsExplicitFlush() {
        try (Session s = new Session(true, 10000)) {
            s.config.setFlushInterval(10);
            s.write(100, RakPriority.NORMAL);
            s.advance(5);
            s.config.setAutoFlush(false);
            s.advance(20);
            assertTrue(s.readData().isEmpty());
            s.transport.flush();
            assertEquals(1, s.readData().size());
        }
    }

    @Test
    void changingFlushIntervalUpdatesPendingDeadline() {
        try (Session s = new Session(true, 10000)) {
            s.config.setFlushInterval(100);
            s.write(100, RakPriority.NORMAL);
            s.advance(10);
            s.config.setFlushInterval(20);
            s.advance(19);
            assertTrue(s.readData().isEmpty());
            s.advance(1);
            assertEquals(1, s.readData().size());
            assertThrows(IllegalArgumentException.class, () -> s.config.setFlushInterval(-1));
        }
    }

    @Test
    void zeroIntervalSendsUnflushedWritesWhenTheirTaskCompletes() {
        try (Session s = new Session(true, 10000)) {
            assertEquals(0, s.config.getFlushInterval(), "Task-end sending is the default");
            s.write(100, RakPriority.NORMAL);
            s.write(100, RakPriority.NORMAL);
            assertTrue(s.readData().isEmpty(), "Nothing leaves while the writing task is still running");
            s.transport.runPendingTasks();
            assertEquals(2, s.readData().size(), "Both writes leave together once the task completes");
            assertTrue(s.transport.runScheduledPendingTasks() >= TimeUnit.SECONDS.toNanos(1),
                    "No timer is involved in task-end sending");
            s.config.setFlushInterval(10);
            s.write(100, RakPriority.NORMAL);
            s.transport.runPendingTasks();
            assertTrue(s.readData().isEmpty(), "A positive interval keeps the write for its window");
            s.advance(10);
            assertEquals(1, s.readData().size());
        }
    }

    @Test
    void disablingAutoFlushInvalidatesTheQueuedAutomaticFlush() {
        try (Session s = new Session(true, 10000)) {
            s.write(100, RakPriority.NORMAL);
            s.config.setAutoFlush(false);
            s.transport.runPendingTasks();
            assertTrue(s.readData().isEmpty(), "A queued automatic flush must not survive disabling auto flush");
            s.transport.flush();
            assertEquals(1, s.readData().size());
        }
    }

    @Test
    void raisingTheIntervalMovesTheQueuedAutomaticFlushIntoTheWindow() {
        try (Session s = new Session(true, 10000)) {
            s.write(100, RakPriority.NORMAL);
            s.config.setFlushInterval(100);
            s.transport.runPendingTasks();
            assertTrue(s.readData().isEmpty(), "The queued flush must yield to the new window");
            s.advance(99);
            assertTrue(s.readData().isEmpty());
            s.advance(1);
            assertEquals(1, s.readData().size());
        }
    }

    @Test
    void explicitFlushInvalidatesTheQueuedAutomaticFlush() {
        AtomicInteger flushes = new AtomicInteger();
        try (Session s = new Session(true, 10000)) {
            s.config.setOption(RakChannelOption.RAK_METRICS, new RakChannelMetrics() {
                @Override
                public void queuedPacketBytes(int count) {
                    flushes.incrementAndGet();
                }
            });
            s.write(100, RakPriority.NORMAL);
            s.transport.flush();
            assertEquals(1, s.readData().size());
            assertEquals(1, flushes.get());
            s.transport.runPendingTasks();
            assertEquals(1, flushes.get(), "The queued automatic flush must not run empty after an explicit flush");
            s.write(100, RakPriority.NORMAL);
            s.transport.runPendingTasks();
            assertEquals(1, s.readData().size(), "Later writes must still be flushed automatically");
            assertEquals(2, flushes.get());
        }
    }

    @Test
    void writesFromTasksQueuedAheadOfTheFlushLeaveTogether() {
        try (Session s = new Session(true, 10000)) {
            s.transport.eventLoop().execute(() -> s.write(100, RakPriority.NORMAL));
            s.transport.eventLoop().execute(() -> s.write(100, RakPriority.NORMAL));
            s.transport.runPendingTasks();
            List<Sent> sent = s.readData();
            assertEquals(2, sent.size());
            assertEquals(sent.get(0).sequence, sent.get(1).sequence, "Both tasks' writes share one datagram");
        }
    }

    @Test
    void idleAutoFlushAndWindowBlockedDataDoNotPoll() {
        try (Session s = new Session(true, 10000)) {
            assertTrue(s.transport.runScheduledPendingTasks() >= TimeUnit.SECONDS.toNanos(1));
            s.write(900, RakPriority.NORMAL);
            s.write(900, RakPriority.NORMAL);
            s.advance(10);
            List<Sent> sent = s.readData();
            assertEquals(1, sent.size());
            assertTrue(s.transport.runScheduledPendingTasks() >= TimeUnit.SECONDS.toNanos(1));
            s.ack(sent.get(0).sequence, false);
            assertEquals(1, s.readData().size(), "ACK must release queued data without waiting for auto-flush");
        }
    }

    @Test
    void reliableDataResendsWithoutReadsOrExplicitFlushes() {
        try (Session s = new Session(false, 10000)) {
            ByteBuf payload = s.write(100, RakPriority.IMMEDIATE);
            Sent original = s.readData().get(0);
            s.advance(1999);
            assertTrue(s.readData().isEmpty());
            s.advance(1);
            Sent resent = s.readData().get(0);
            assertNotEquals(original.sequence, resent.sequence);
            assertEquals(original.reliabilityIndex, resent.reliabilityIndex);
            assertEquals(original.bytes, resent.bytes);
            s.ack(resent.sequence, false);
            assertEquals(0, payload.refCnt(), "ACK must release the retained payload");
            s.advance(2000);
            assertTrue(s.readData().isEmpty(), "ACKed data must leave the deadline queue");
        }
    }

    @Test
    void nackResendsImmediatelyAndReplacesTheOldDeadline() {
        try (Session s = new Session(false, 10000)) {
            s.write(100, RakPriority.IMMEDIATE);
            Sent original = s.readData().get(0);
            s.advance(1000);
            s.ack(original.sequence, true);
            Sent resent = s.readData().get(0);
            assertEquals(original.reliabilityIndex, resent.reliabilityIndex);
            s.advance(1000);
            assertTrue(s.readData().isEmpty());
            s.advance(1000);
            assertEquals(1, s.readData().size());
        }
    }

    @Test
    void ackOfEarliestDatagramPreservesTheNextDeadline() {
        try (Session s = new Session(false, 10000)) {
            s.write(100, RakPriority.IMMEDIATE);
            Sent first = s.readData().get(0);
            s.advance(500);
            s.write(100, RakPriority.IMMEDIATE);
            Sent second = s.readData().get(0);
            s.ack(first.sequence, false);
            s.advance(1500);
            assertTrue(s.readData().isEmpty());
            s.advance(500);
            List<Sent> resent = s.readData();
            assertEquals(1, resent.size());
            assertEquals(second.reliabilityIndex, resent.get(0).reliabilityIndex);
        }
    }

    @Test
    void shortenedRtoCanPutANewerDatagramAheadOfOlderData() {
        try (Session s = new Session(false, 10000)) {
            s.write(100, RakPriority.IMMEDIATE);
            s.write(100, RakPriority.IMMEDIATE);
            List<Sent> first = s.readData();
            s.advance(10);
            s.ack(first.get(1).sequence, false); // RTT 10ms yields an RTO of 90ms.
            s.write(100, RakPriority.IMMEDIATE);
            Sent newer = s.readData().get(0);
            s.advance(89);
            assertTrue(s.readData().isEmpty());
            s.advance(1);
            List<Sent> resent = s.readData();
            assertEquals(1, resent.size());
            assertEquals(newer.reliabilityIndex, resent.get(0).reliabilityIndex);
        }
    }

    @Test
    void splitBuffersExpireAtThirtySecondsWithLongSessionTimeout() {
        try (Session s = new Session(false, 120000)) {
            EncapsulatedPacket fragment = part(0, 0);
            ByteBuf buffer = fragment.getBuffer();
            s.receive(fragment);
            s.advance(29999);
            assertEquals(1, buffer.refCnt());
            s.advance(1);
            assertEquals(0, buffer.refCnt());
            assertTrue(s.transport.isOpen());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 256})
    void replacingExpiredSplitReclaimsBytesBeforeCheckingBudget(int replacementId) {
        try (Session s = new Session(false, 120000)) {
            s.config.setMaxSplitQueuedBytes(150);
            EncapsulatedPacket old = part(0, 0);
            ByteBuf oldBuffer = old.getBuffer();
            s.receive(old);
            // Deliver a fragment before the expired timer gets its turn on the loop.
            s.elapse(30000);
            s.receive(part(replacementId, 0));
            assertEquals(0, oldBuffer.refCnt());
            assertTrue(s.transport.isOpen(), "Expired bytes must not count against the replacement");
            s.receive(part(replacementId, 1));
            EncapsulatedPacket reassembled = s.transport.readInbound();
            assertNotNull(reassembled);
            assertEquals(200, reassembled.getBuffer().readableBytes());
            reassembled.release();
            s.receive(part(1, 0));
            assertTrue(s.transport.isOpen(), "Completed reassembly must leave no phantom bytes");
        }
    }

    @Test
    void completingEarliestSplitDoesNotPrematurelyExpireAnother() {
        try (Session s = new Session(false, 120000)) {
            s.receive(part(0, 0));
            s.advance(10000);
            EncapsulatedPacket later = part(1, 0);
            ByteBuf laterBuffer = later.getBuffer();
            s.receive(later);
            s.receive(part(0, 1));
            ReferenceCountUtil.release(s.transport.readInbound());
            s.advance(20000);
            assertEquals(1, laterBuffer.refCnt());
            s.advance(10000);
            assertEquals(0, laterBuffer.refCnt());
        }
    }

    @Test
    void reducingQueueBudgetAppliesWithoutWaitingForAnotherPacket() {
        try (Session s = new Session(false, 10000)) {
            ByteBuf payload = s.write(100, RakPriority.NORMAL);
            s.config.setMaxQueuedBytes(50);
            assertFalse(s.transport.isOpen());
            s.readData();
            assertEquals(0, payload.refCnt());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeOrRemovalReleasesBuffersAndCancelsAllTasks(boolean removeHandler) {
        try (Session s = new Session(true, 10000)) {
            ByteBuf queued = s.write(100, RakPriority.NORMAL);
            ByteBuf sent = s.write(100, RakPriority.IMMEDIATE);
            s.readData();
            EncapsulatedPacket split = part(0, 0);
            ByteBuf retainedSplit = split.getBuffer();
            s.receive(split); // Also queues an ACK flush.
            if (removeHandler) {
                s.transport.pipeline().remove(s.codec);
            } else {
                s.transport.close();
            }
            s.readData(); // EmbeddedChannel may run an already queued flush before closing.
            assertEquals(0, queued.refCnt());
            assertEquals(0, sent.refCnt());
            assertEquals(0, retainedSplit.refCnt());
            s.advance(120000);
            assertTrue(s.readData().isEmpty());
            assertEquals(-1, s.transport.runScheduledPendingTasks());
            s.transport.checkException();
        }
    }

    private static EncapsulatedPacket part(int partId, int partIndex) {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.UNRELIABLE);
        packet.setSplit(true);
        packet.setPartId(partId);
        packet.setPartCount(2);
        packet.setPartIndex(partIndex);
        packet.setBuffer(Unpooled.buffer(100).writeZero(100));
        return packet;
    }

    private static class Sent {
        final int sequence;
        final int reliabilityIndex;
        final int bytes;

        Sent(RakDatagramPacket datagram, EncapsulatedPacket packet) {
            this.sequence = datagram.getSequenceIndex();
            this.reliabilityIndex = packet.getReliabilityIndex();
            this.bytes = packet.getBuffer().readableBytes();
        }
    }

    /** A transport pipeline with a RakChannel facade, matching the parent-loop arrangement in production. */
    private static class Session implements AutoCloseable {
        final EmbeddedChannel transport = new EmbeddedChannel();
        final RakChannel channel;
        DefaultRakSessionConfig config;
        final RakSessionCodec codec;
        long now;
        int sequence;

        Session(boolean autoFlush, long timeout) {
            this(autoFlush, timeout, false);
        }

        Session(Boolean autoFlush, long timeout, boolean client) {
            this(autoFlush, timeout, client, false);
        }

        Session(Boolean autoFlush, long timeout, boolean client, boolean compatible) {
            this.transport.freezeTime();
            this.channel = (RakChannel) Proxy.newProxyInstance(RakChannel.class.getClassLoader(),
                    new Class<?>[]{RakChannel.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "config": return this.config;
                            case "rakPipeline": case "pipeline": return this.transport.pipeline();
                            case "parent": return this.transport;
                            case "remoteAddress": case "localAddress": return new InetSocketAddress("127.0.0.1", 19132);
                            default: return method.invoke(this.transport, args);
                        }
                    });
            this.config = client ? new DefaultRakClientConfig(this.channel) : new DefaultRakSessionConfig(this.channel);
            if (client) {
                this.config.setOption(RakChannelOption.RAK_COMPATIBILITY_MODE, compatible);
            }
            if (autoFlush != null) {
                this.config.setAutoFlush(autoFlush);
            }
            this.config.setSessionTimeout(timeout);
            this.codec = compatible ? new RakSessionCodecCompatible(this.channel, () -> this.now)
                    : new RakSessionCodec(this.channel, () -> this.now);
            ChannelPipeline pipeline = this.transport.pipeline();
            pipeline.addLast(RakAcknowledgeHandler.NAME, new RakAcknowledgeHandler(this.codec));
            pipeline.addLast(RakSessionCodec.NAME, this.codec);
            pipeline.fireChannelActive();
            this.transport.runPendingTasks();
            this.transport.runScheduledPendingTasks();
            this.readData(); // Initial connected ping.
        }

        void elapse(long millis) {
            this.now += millis;
            this.transport.advanceTimeBy(millis, TimeUnit.MILLISECONDS);
        }

        void advance(long millis) {
            this.elapse(millis);
            this.transport.runScheduledPendingTasks();
            this.transport.runPendingTasks();
            this.transport.checkException();
        }

        ByteBuf write(int bytes, RakPriority priority) {
            ByteBuf payload = Unpooled.buffer(bytes).writeByte(DATA_ID).writeZero(bytes - 1);
            // Write within the current task; EmbeddedChannel.writeOneOutbound also drains tasks in newer Netty.
            this.transport.pipeline().write(new RakMessage(payload, RakReliability.RELIABLE, priority));
            return payload;
        }

        void receive(EncapsulatedPacket... packets) {
            RakDatagramPacket datagram = RakDatagramPacket.newInstance();
            datagram.setSequenceIndex(this.sequence++);
            for (EncapsulatedPacket packet : packets) {
                datagram.getPackets().add(packet);
            }
            this.transport.pipeline().fireChannelRead(datagram);
        }

        void ack(int sequence, boolean nack) {
            ByteBuf buffer = Unpooled.buffer(7);
            buffer.writeByte(FLAG_VALID | (nack ? FLAG_NACK : FLAG_ACK));
            buffer.writeShort(1).writeBoolean(true).writeMediumLE(sequence);
            this.transport.writeInbound(buffer);
        }

        List<Sent> readData() {
            List<Sent> sent = new ArrayList<>();
            Object message;
            while ((message = this.transport.readOutbound()) != null) {
                try {
                    if (message instanceof RakDatagramPacket) {
                        RakDatagramPacket datagram = (RakDatagramPacket) message;
                        for (EncapsulatedPacket packet : datagram.getPackets()) {
                            if (packet.getBuffer().getUnsignedByte(packet.getBuffer().readerIndex()) == DATA_ID) {
                                sent.add(new Sent(datagram, packet));
                            }
                        }
                    }
                } finally {
                    ReferenceCountUtil.release(message);
                }
            }
            return sent;
        }

        @Override
        public void close() {
            this.transport.finishAndReleaseAll();
        }
    }
}

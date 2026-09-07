/*
 * Copyright 2022 CloudburstMC
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

package io.github.sendablemetatype.netty.handler.codec.raknet.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.github.sendablemetatype.netty.channel.raknet.*;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelMetrics;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelOption;
import io.github.sendablemetatype.netty.channel.raknet.config.RakSessionConfigUpdate;
import io.github.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;
import io.github.sendablemetatype.netty.channel.raknet.packet.RakDatagramPacket;
import io.github.sendablemetatype.netty.channel.raknet.packet.RakMessage;
import io.github.sendablemetatype.netty.util.*;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.*;

public class RakSessionCodec extends ChannelDuplexHandler {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakSessionCodec.class);
    public static final String NAME = "rak-session-codec";

    private static final long CONNECTED_PING_INTERVAL_MS = 2000;
    private static final Comparator<OrderedPacket> ORDERING_COMPARATOR = (first, second) -> {
        int order = Long.compare(first.orderingIndex(), second.orderingIndex());
        if (order != 0) {
            return order;
        }
        boolean firstSequenced = first.packet().getReliability().isSequenced();
        boolean secondSequenced = second.packet().getReliability().isSequenced();
        if (firstSequenced && secondSequenced) {
            // Future ordering indices start at sequence zero; modular pairwise comparison is not transitive.
            return Integer.compare(first.packet().getSequenceIndex(), second.packet().getSequenceIndex());
        }
        // Sequenced messages precede the ordered message that closes their ordering index.
        return firstSequenced ? -1 : secondSequenced ? 1 : 0;
    };

    private final RakChannel channel;
    private final LongSupplier clock;
    private ChannelHandlerContext context;

    // Only pings are periodic. Other tasks sleep until there is work and a deadline to meet.
    private ScheduledFuture<?> autoFlushFuture;
    private ScheduledFuture<?> timeoutFuture;
    private ScheduledFuture<?> pingFuture;
    private ScheduledFuture<?> resendFuture;
    private ScheduledFuture<?> splitExpiryFuture;
    private boolean autoFlushQueued;
    private int autoFlushGeneration;
    private long resendDeadline = Long.MAX_VALUE;
    private boolean flushRequested;
    private boolean deinitialized;

    private volatile RakState state;

    private volatile long lastTouched = System.currentTimeMillis();
    private long lastActivity;

    // Reliability, Ordering, Sequencing and datagram indexes
    private RakSlidingWindow slidingWindow;
    private int splitIndex;
    private int datagramReadIndex;
    private long datagramWriteIndex;
    private int reliabilityReadIndex;
    private int reliabilityWriteIndex;
    private long[] orderReadIndex;
    private int[] orderWriteIndex;
    private int[] sequenceReadIndex;
    private int[] sequenceWriteIndex;

    private RoundRobinArray<SplitPacketHelper> splitPackets;
    private int splitPacketCount;
    private BitQueue reliableDatagramQueue;

    private FastBinaryMinHeap<EncapsulatedPacket> outgoingPackets;
    private long[] outgoingPacketNextWeights;
    private PriorityQueue<OrderedPacket>[] orderingHeaps;
    private long currentPingTime = -1;
    private long currentPingSentAt;
    private long lastPingTime = -1;
    private long lastPongTime = -1;
    private IntObjectMap<RakDatagramPacket> sentDatagrams;
    // The map owns the retained datagram; the indexed heap is a non-owning deadline index.
    private DefaultPriorityQueue<RakDatagramPacket> resendQueue;
    private Queue<IntRange> incomingAcks;
    private Queue<IntRange> incomingNaks;
    private Deque<IntRange> outgoingAcks;
    private Queue<IntRange> outgoingNaks;
    private long lastMinWeight;

    private int queuedBytes = 0;
    // Payload bytes retained by inbound split reassembly and ordered-reorder buffering respectively.
    private long splitQueuedBytes = 0;
    private long orderingQueuedBytes = 0;

    public RakSessionCodec(RakChannel channel) {
        this(channel, () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    RakSessionCodec(RakChannel channel, LongSupplier clock) {
        this.channel = channel;
        this.clock = clock;
        this.setState(RakState.UNCONNECTED);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.context = ctx;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        this.deinitialize();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (this.deinitialized || this.state == RakState.CONNECTED) {
            return;
        }
        this.setState(RakState.CONNECTED);
        this.lastActivity = this.clock.getAsLong();
        this.lastTouched = System.currentTimeMillis();
        int mtu = this.getMtu();

        this.slidingWindow = new RakSlidingWindow(mtu);

        this.outgoingPacketNextWeights = new long[4];
        this.initHeapWeights();

        int maxChannels = this.channel.config().getOption(RakChannelOption.RAK_ORDERING_CHANNELS);
        this.orderReadIndex = new long[maxChannels];
        this.orderWriteIndex = new int[maxChannels];
        this.sequenceReadIndex = new int[maxChannels];
        this.sequenceWriteIndex = new int[maxChannels];

        // Noinspection unchecked
        this.orderingHeaps = new PriorityQueue[maxChannels];
        for (int i = 0; i < maxChannels; i++) {
            orderingHeaps[i] = new PriorityQueue<>(64, ORDERING_COMPARATOR);
        }

        this.outgoingPackets = new FastBinaryMinHeap<>(8);
        this.sentDatagrams = new IntObjectHashMap<>();
        this.resendQueue = new DefaultPriorityQueue<>(Comparator.comparingLong(RakDatagramPacket::getNextSend), 8);

        this.incomingAcks = new ArrayDeque<>();
        this.incomingNaks = new ArrayDeque<>();
        this.outgoingAcks = new ArrayDeque<>();
        this.outgoingNaks = new ArrayDeque<>();

        this.reliableDatagramQueue = new BitQueue(512);
        this.splitPackets = new RoundRobinArray<>(256);

        // After the session is fully initialized, start its timed duties.
        this.scheduleTimeoutCheck(this.channel.config().getOption(RakChannelOption.RAK_SESSION_TIMEOUT));
        this.pingFuture = this.eventLoop().scheduleWithFixedDelay(
                () -> this.safeRun(this::sendConnectedPing), 0, CONNECTED_PING_INTERVAL_MS, TimeUnit.MILLISECONDS);

        ctx.fireChannelActive(); // fire channel active on rakPipeline()
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.deinitialize();
        super.channelInactive(ctx);
    }

    private void deinitialize() {
        if (this.deinitialized) {
            return;
        }
        this.deinitialized = true;
        this.setState(RakState.DISCONNECTED);
        this.cancelScheduled();

        // Perform resource clean up.
        if (this.splitPackets != null) {
            for (SplitPacketHelper helper : this.splitPackets) {
                if (helper != null) {
                    helper.release();
                }
            }
            this.splitPackets = null;
        }
        this.splitPacketCount = 0;

        if (this.resendQueue != null) {
            this.resendQueue.clear();
        }
        if (this.sentDatagrams != null) {
            for (RakDatagramPacket packet : this.sentDatagrams.values()) {
                packet.release();
            }
            this.sentDatagrams.clear();
        }

        PriorityQueue<OrderedPacket>[] orderingHeaps = this.orderingHeaps;
        this.orderingHeaps = null;
        if (orderingHeaps != null) {
            for (PriorityQueue<OrderedPacket> orderingHeap : orderingHeaps) {
                OrderedPacket packet;
                while ((packet = orderingHeap.poll()) != null) {
                    packet.packet().release();
                }
            }
        }

        FastBinaryMinHeap<EncapsulatedPacket> outgoingPackets = this.outgoingPackets;
        this.outgoingPackets = null;
        if (outgoingPackets != null) {
            EncapsulatedPacket packet;
            while ((packet = outgoingPackets.poll()) != null) {
                packet.release();
            }
            outgoingPackets.release();
        }

        this.queuedBytes = 0;
        this.splitQueuedBytes = 0;
        this.orderingQueuedBytes = 0;

        if (log.isTraceEnabled()) {
            log.trace("RakNet Session ({} => {}) closed!", this.channel.localAddress(), this.getRemoteAddress());
        }
    }

    private void initHeapWeights() {
        for (int priorityLevel = 0; priorityLevel < 4; priorityLevel++) {
            this.outgoingPacketNextWeights[priorityLevel] = (1 << priorityLevel) * priorityLevel + priorityLevel;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!this.channel.parent().eventLoop().inEventLoop()) {
            // Make sure this runs on correct thread
            log.error("Tried to write packet from wrong thread: {}", Thread.currentThread().getName(), new Throwable());
            final Object finalMsg = msg;
            this.channel.parent().eventLoop().execute(() -> this.write(ctx, finalMsg, promise));
            return;
        }
        if (msg instanceof ByteBuf) {
            msg = new RakMessage((ByteBuf) msg);
        } else if (!(msg instanceof RakMessage)) {
            throw new IllegalArgumentException("Message must be a ByteBuf or RakMessage");
        }

        try {
            this.send(ctx, (RakMessage) msg);
            promise.setSuccess(null);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (!this.deinitialized && this.state == RakState.CONNECTED) {
            this.internalFlush(ctx);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof RakSessionConfigUpdate)) {
            ctx.fireUserEventTriggered(evt);
            return;
        }
        if (this.deinitialized || this.state != RakState.CONNECTED) {
            return;
        }
        this.safeRun(() -> {
            switch ((RakSessionConfigUpdate) evt) {
                case SESSION_TIMEOUT:
                    this.scheduleTimeoutCheck(this.channel.config().getSessionTimeout()
                            - (this.clock.getAsLong() - this.lastActivity));
                    break;
                case AUTO_FLUSH:
                    this.cancelAutoFlush();
                    this.scheduleAutoFlush();
                    break;
                case QUEUE_LIMITS:
                    this.checkQueuedBytes();
                    if (this.state == RakState.CONNECTED) {
                        this.checkSplitQueuedBytes();
                    }
                    if (this.state == RakState.CONNECTED) {
                        this.checkOrderingQueuedBytes();
                    }
                    break;
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            if (!(msg instanceof RakDatagramPacket)) {
                // We don't want to let anything through that isn't RakNet related.
                return;
            }
            RakDatagramPacket packet = (RakDatagramPacket) msg;
            if (this.deinitialized || this.state != RakState.CONNECTED) {
                log.debug("{} received message from inactive channel: {}", this.getRemoteAddress(), packet);
            } else {
                this.handleDatagram(ctx, packet);
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        this.disconnect0(RakDisconnectReason.DISCONNECTED).addListener(future -> {
            if (future.cause() == null) {
                promise.trySuccess();
            } else {
                promise.tryFailure(future.cause());
            }
        });
    }

    private void send(ChannelHandlerContext ctx, RakMessage message) {
        if (this.deinitialized || this.state == RakState.UNCONNECTED) {
            throw new IllegalStateException("Can not send RakMessage to inactive channel");
        }

        if (message.content().getUnsignedByte(message.content().readerIndex()) == 0xc0) {
            throw new IllegalArgumentException();
        }

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.encapsulatedOut(1);
        }
        EncapsulatedPacket[] packets = this.createEncapsulated(message);
        if (message.priority() == RakPriority.IMMEDIATE) {
            this.sendImmediate(ctx, packets);
            return;
        }

        long weight = this.getNextWeight(message.priority());
        if (packets.length == 1) {
            this.outgoingPackets.insert(weight, packets[0]);
            this.queuedBytes += packets[0].getBuffer().readableBytes();
        } else {
            this.outgoingPackets.insertSeries(weight, packets);
            for (EncapsulatedPacket packet : packets) {
                this.queuedBytes += packet.getBuffer().readableBytes();
            }
        }
        this.checkQueuedBytes();
        this.scheduleAutoFlush();
    }

    // The byte budgets are enforced where the bytes are added, not on a poll.

    private void checkQueuedBytes() {
        int maxQueuedBytes = this.channel.config().getOption(RakChannelOption.RAK_MAX_QUEUED_BYTES);
        if (maxQueuedBytes > 0 && this.queuedBytes > maxQueuedBytes) {
            this.disconnect(RakDisconnectReason.QUEUE_TOO_LONG);
        }
    }

    private void checkSplitQueuedBytes() {
        int maxSplitQueuedBytes = this.channel.config().getOption(RakChannelOption.RAK_MAX_SPLIT_QUEUED_BYTES);
        if (maxSplitQueuedBytes > 0 && this.splitQueuedBytes > maxSplitQueuedBytes) {
            // Reassemblies the peer legitimately abandoned must not count against it.
            this.evictExpiredSplitPackets();
            if (this.splitQueuedBytes > maxSplitQueuedBytes) {
                this.disconnect(RakDisconnectReason.SPLIT_QUEUE_TOO_LONG);
            }
        }
    }

    private void checkOrderingQueuedBytes() {
        int limit = this.channel.config().getMaxOrderingQueuedBytes();
        if (limit > 0 && this.orderingQueuedBytes > limit) {
            this.disconnect(RakDisconnectReason.ORDERING_QUEUE_TOO_LONG);
        }
    }

    private void handleDatagram(ChannelHandlerContext ctx, RakDatagramPacket packet) {
        this.touch();
        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.rakDatagramsIn(1);
        }

        this.slidingWindow.onPacketReceived(packet.getSendTime());

        int sequenceIndex = packet.getSequenceIndex();
        int prevSequenceIndex = this.datagramReadIndex;
        int missedDatagrams = RakSequence.difference(sequenceIndex, prevSequenceIndex);
        if (missedDatagrams >= 0) {
            this.datagramReadIndex = (sequenceIndex + 1) & RakSequence.MASK;
        }
        if (missedDatagrams > 0) {
            if (sequenceIndex < prevSequenceIndex) {
                // ACK ranges cannot wrap, even though their sequence numbers can.
                this.outgoingNaks.offer(new IntRange(prevSequenceIndex, RakSequence.MASK));
                if (sequenceIndex > 0) {
                    this.outgoingNaks.offer(new IntRange(0, sequenceIndex - 1));
                }
            } else {
                this.outgoingNaks.offer(new IntRange(prevSequenceIndex, sequenceIndex - 1));
            }
        }

        IntRange lastAck = this.outgoingAcks.peekLast();
        if (lastAck != null && lastAck.end == sequenceIndex - 1) {
            lastAck.end = sequenceIndex;
        } else {
            this.outgoingAcks.offer(new IntRange(sequenceIndex, sequenceIndex));
        }
        // Acknowledgements leave right after the read that produced them, coalesced per read batch.
        this.requestFlush();

        for (final EncapsulatedPacket encapsulated : packet.getPackets()) {
            if (this.state != RakState.CONNECTED) {
                // A budget check disconnected the session mid-datagram.
                break;
            }
            if (encapsulated.getReliability().isReliable()) {
                int missed = RakSequence.difference(encapsulated.getReliabilityIndex(), this.reliabilityReadIndex);
                if (missed > 0) {
                    if (missed < this.reliableDatagramQueue.size()) {
                        if (this.reliableDatagramQueue.get(missed)) {
                            this.reliableDatagramQueue.set(missed, false);
                        } else {
                            // Duplicate packet
                            continue;
                        }
                    } else {
                        int count = (missed - this.reliableDatagramQueue.size());
                        this.reliableDatagramQueue.add(true, count);

                        this.reliableDatagramQueue.add(false);
                    }
                } else if (missed == 0) {
                    this.reliabilityReadIndex = (this.reliabilityReadIndex + 1) & RakSequence.MASK;
                    if (!this.reliableDatagramQueue.isEmpty()) {
                        this.reliableDatagramQueue.poll();
                    }
                } else {
                    // Duplicate packet
                    continue;
                }

                while (!this.reliableDatagramQueue.isEmpty() && !this.reliableDatagramQueue.peek()) {
                    this.reliableDatagramQueue.poll();
                    this.reliabilityReadIndex = (this.reliabilityReadIndex + 1) & RakSequence.MASK;
                }
            }

            if (encapsulated.isSplit()) {
                final EncapsulatedPacket reassembled = this.getReassembledPacket(encapsulated, ctx.alloc());
                if (reassembled == null) {
                    // Not reassembled
                    continue;
                }
                if (metrics != null) {
                    metrics.encapsulatedIn(1);
                }
                try {
                    this.checkForOrdered(ctx, reassembled);
                } finally {
                    reassembled.release();
                }
            } else {
                if (metrics != null) {
                    metrics.encapsulatedIn(1);
                }
                this.checkForOrdered(ctx, encapsulated);
            }
        }
    }

    private void checkForOrdered(ChannelHandlerContext ctx, EncapsulatedPacket packet) {
        if (packet.getReliability().isOrdered() || packet.getReliability().isSequenced()) {
            this.onOrderedReceived(ctx, packet);
        } else {
            ctx.fireChannelRead(packet.retain());
        }
    }

    private void onOrderedReceived(ChannelHandlerContext ctx, EncapsulatedPacket packet) {
        int orderingChannel = packet.getOrderingChannel();
        PriorityQueue<OrderedPacket> binaryHeap = this.orderingHeaps[orderingChannel];
        long expectedIndex = this.orderReadIndex[orderingChannel];
        int distance = RakSequence.difference(packet.getOrderingIndex(), (int) expectedIndex);
        if (distance > 0) {
            // Not next in line so add to queue.
            binaryHeap.add(new OrderedPacket(expectedIndex + distance, packet.retain()));
            this.orderingQueuedBytes += packet.getBuffer().readableBytes();
            this.checkOrderingQueuedBytes();
            return;
        } else if (distance < 0) {
            // We already have this
            return;
        }
        this.deliverOrdered(ctx, packet, orderingChannel);

        OrderedPacket queued;
        while (this.state == RakState.CONNECTED && (queued = binaryHeap.peek()) != null) {
            EncapsulatedPacket queuedPacket = queued.packet();
            int queuedDistance = Long.compare(queued.orderingIndex(), this.orderReadIndex[orderingChannel]);
            if (queuedDistance <= 0) {
                try {
                    binaryHeap.remove();
                    this.orderingQueuedBytes -= queuedPacket.getBuffer().readableBytes();
                    // Repeated ordering indices must not block the next gap from draining.
                    if (queuedDistance == 0) {
                        this.deliverOrdered(ctx, queuedPacket, orderingChannel);
                    }
                } finally {
                    queuedPacket.release();
                }
            } else {
                // Found a gap. Wait till we start receive another ordered packet.
                break;
            }
        }
    }

    private void deliverOrdered(ChannelHandlerContext ctx, EncapsulatedPacket packet, int orderingChannel) {
        if (packet.getReliability().isSequenced()) {
            if (RakSequence.difference(packet.getSequenceIndex(), this.sequenceReadIndex[orderingChannel]) < 0) {
                return;
            }
            this.sequenceReadIndex[orderingChannel] = (packet.getSequenceIndex() + 1) & RakSequence.MASK;
        } else {
            this.orderReadIndex[orderingChannel]++;
            this.sequenceReadIndex[orderingChannel] = 0;
        }
        ctx.fireChannelRead(packet.retain());
    }

    private record OrderedPacket(long orderingIndex, EncapsulatedPacket packet) {
    }

    private EncapsulatedPacket getReassembledPacket(EncapsulatedPacket splitPacket, ByteBufAllocator alloc) {
        this.checkForClosed();

        int partId = splitPacket.getPartId();
        SplitPacketHelper helper = this.splitPackets.get(partId);
        long now = this.clock.getAsLong();
        if (helper != null && helper.expired(now)) {
            // Reclaim the old helper's bytes before replacing its array slot, including matching IDs.
            this.removeSplitPacket(partId, helper);
            helper = null;
        }
        if (helper != null && !helper.matches(splitPacket)) {
            // Part IDs are only unique modulo the size of this array, so an unrelated split packet may
            // hold the slot. Drop the part rather than corrupt a reassembly that is still in progress.
            return null;
        }

        if (helper == null) {
            this.splitPackets.set(partId, helper = new SplitPacketHelper(partId, splitPacket.getPartCount(), now));
            this.splitPacketCount++;
            if (this.splitExpiryFuture == null) {
                this.scheduleSplitExpiry(helper.getExpiresAt() - now);
            }
        }

        // Try reassembling the packet, tracking how many bytes this session now retains for split reassembly.
        int sizeBefore = helper.getReassembledSize();
        EncapsulatedPacket result = helper.add(splitPacket, alloc);
        this.splitQueuedBytes += helper.getReassembledSize() - sizeBefore;
        if (result != null) {
            // Packet reassembled. Remove the helper and reclaim the bytes it held.
            this.removeSplitPacket(partId, helper);
        } else {
            this.checkSplitQueuedBytes();
        }

        return result;
    }

    private void removeSplitPacket(int partId, SplitPacketHelper helper) {
        this.splitQueuedBytes -= helper.getReassembledSize();
        this.splitPackets.remove(partId, helper);
        if (--this.splitPacketCount == 0 && this.splitExpiryFuture != null) {
            this.splitExpiryFuture.cancel(false);
            this.splitExpiryFuture = null;
        }
    }

    private void scheduleSplitExpiry(long delayMillis) {
        this.splitExpiryFuture = this.eventLoop().schedule(() -> this.safeRun(this::evictExpiredSplitPackets),
                Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
    }

    // The bounded table is scanned only on an expiry deadline or when enforcing its byte budget.
    private void evictExpiredSplitPackets() {
        if (this.splitExpiryFuture != null) {
            this.splitExpiryFuture.cancel(false);
            this.splitExpiryFuture = null;
        }
        if (this.deinitialized) {
            return;
        }
        long now = this.clock.getAsLong();
        long earliest = Long.MAX_VALUE;
        Iterator<SplitPacketHelper> iterator = this.splitPackets.iterator();
        while (iterator.hasNext()) {
            SplitPacketHelper helper = iterator.next();
            if (helper == null) {
                continue;
            }
            if (helper.expired(now)) {
                this.splitQueuedBytes -= helper.getReassembledSize();
                this.splitPacketCount--;
                iterator.remove();
            } else {
                earliest = Math.min(earliest, helper.getExpiresAt());
            }
        }
        if (earliest != Long.MAX_VALUE) {
            this.scheduleSplitExpiry(earliest - now);
        }
    }

    private void safeRun(Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            log.error("[{}] Error in RakSessionCodec task state={} channelActive={}", this.getRemoteAddress(), this.state, this.channel.isActive(), t);
            this.channel.close();
        }
    }

    private void cancelScheduled() {
        ScheduledFuture<?>[] futures = {this.autoFlushFuture, this.timeoutFuture, this.pingFuture,
                this.resendFuture, this.splitExpiryFuture};
        this.autoFlushFuture = null;
        this.timeoutFuture = null;
        this.pingFuture = null;
        this.resendFuture = null;
        this.splitExpiryFuture = null;
        this.resendDeadline = Long.MAX_VALUE;
        this.autoFlushGeneration++;
        this.autoFlushQueued = false;
        for (ScheduledFuture<?> future : futures) {
            if (future != null) {
                future.cancel(false);
            }
        }
    }

    private void cancelAutoFlush() {
        if (this.autoFlushFuture != null) {
            this.autoFlushFuture.cancel(false);
            this.autoFlushFuture = null;
        }
        // A queued task cannot be removed from the event loop; it checks this generation instead.
        this.autoFlushGeneration++;
        this.autoFlushQueued = false;
    }

    /**
     * Queues the automatic flush for interval 0. Unlike {@link #requestFlush()} this is configuration driven, so
     * {@link #cancelAutoFlush()} must be able to invalidate it: the task re-checks the generation it was queued
     * under and the configuration, and does nothing when auto flush was disabled, the interval changed, or an
     * explicit flush already sent the data. Protocol flushes are never suppressed this way.
     */
    private void requestAutoFlush() {
        if (this.autoFlushQueued) {
            return;
        }
        this.autoFlushQueued = true;
        int generation = this.autoFlushGeneration;
        this.eventLoop().execute(() -> {
            if (generation != this.autoFlushGeneration) {
                return;
            }
            this.autoFlushQueued = false;
            if (!this.deinitialized && this.state == RakState.CONNECTED && this.channel.config().isAutoFlush()
                    && this.channel.config().getFlushInterval() == 0) {
                this.safeRun(() -> this.internalFlush(this.ctx()));
            }
        });
    }

    private void scheduleAutoFlush() {
        if (this.deinitialized || this.state != RakState.CONNECTED || this.autoFlushFuture != null
                || !this.channel.config().isAutoFlush()) {
            return;
        }
        EncapsulatedPacket next = this.outgoingPackets.peek();
        if (next == null || next.getSize() > this.slidingWindow.getTransmissionBandwidth()) {
            // An incoming ACK will release data blocked by the congestion window.
            return;
        }
        if (this.channel.config().getFlushInterval() == 0) {
            // No window: the data leaves when the queued flush runs, after the task that wrote it
            // and any task queued ahead of the flush, coalesced with everything those tasks wrote.
            this.requestAutoFlush();
            return;
        }
        this.autoFlushFuture = this.eventLoop().schedule(() -> this.safeRun(() -> {
            this.autoFlushFuture = null;
            if (!this.deinitialized && this.channel.config().isAutoFlush()) {
                this.internalFlush(this.ctx());
            }
        }), this.channel.config().getFlushInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * Queues a protocol flush: acknowledgements produced by a read batch, resends triggered by a NACK, or data
     * released by an incoming ACK. It runs after the current task and any task queued ahead of it, so requests
     * made until then coalesce into one flush. Configuration changes never cancel it.
     */
    private void requestFlush() {
        if (this.flushRequested || this.deinitialized) {
            return;
        }
        this.flushRequested = true;
        this.eventLoop().execute(() -> {
            this.flushRequested = false;
            if (!this.deinitialized && this.state == RakState.CONNECTED) {
                this.safeRun(() -> this.internalFlush(this.ctx()));
            }
        });
    }

    /**
     * Idle timeout without polling: the check is scheduled for the timeout and, when the session turns out to be
     * active, rescheduled for the remaining time. Configuration changes also replace the pending deadline.
     */
    private void scheduleTimeoutCheck(long delayMs) {
        if (this.timeoutFuture != null) {
            this.timeoutFuture.cancel(false);
        }
        this.timeoutFuture = this.eventLoop().schedule(() -> this.safeRun(this::checkTimeout),
                Math.max(0, delayMs), TimeUnit.MILLISECONDS);
    }

    private void checkTimeout() {
        if (this.deinitialized) {
            return;
        }
        long timeout = this.channel.config().getOption(RakChannelOption.RAK_SESSION_TIMEOUT);
        long idle = this.clock.getAsLong() - this.lastActivity;
        if (idle >= timeout) {
            if (this.state == RakState.UNCONNECTED) {
                this.close(RakDisconnectReason.TIMED_OUT);
            } else {
                this.disconnect(RakDisconnectReason.TIMED_OUT);
            }
            return;
        }
        this.scheduleTimeoutCheck(timeout - idle);
    }

    private void sendConnectedPing() {
        if (this.deinitialized || this.state != RakState.CONNECTED) {
            return;
        }
        this.writePing(this.ctx(), System.currentTimeMillis());
    }

    void writePing(ChannelHandlerContext ctx, long curTime) {
        // The compatible client sends its first ping with the final handshake batch.
        if (Boolean.TRUE.equals(this.channel.config().getOption(RakChannelOption.RAK_COMPATIBILITY_MODE))
                && this.datagramWriteIndex <= 1) {
            return;
        }
        long pingTime = this.pingTimestamp();
        ByteBuf buffer = ctx.alloc().ioBuffer(9);
        buffer.writeByte(ID_CONNECTED_PING);
        buffer.writeLong(pingTime);
        this.currentPingTime = pingTime;
        this.currentPingSentAt = this.clock.getAsLong();
        this.write(ctx, new RakMessage(buffer, RakReliability.UNRELIABLE, RakPriority.IMMEDIATE), ctx.voidPromise());
    }

    long pingTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * Retransmission is driven by one deadline timer set to the earliest pending retransmission time instead of
     * polling every sent datagram on a tick. Nothing runs while nothing is unacknowledged.
     */
    private void scheduleResend() {
        if (this.deinitialized) {
            return;
        }
        RakDatagramPacket next = this.resendQueue.peek();
        long deadline = next == null ? Long.MAX_VALUE : next.getNextSend();
        if (this.resendFuture != null && deadline == this.resendDeadline) {
            return;
        }
        if (this.resendFuture != null) {
            this.resendFuture.cancel(false);
            this.resendFuture = null;
        }
        this.resendDeadline = deadline;
        if (next != null) {
            long delay = Math.max(1, deadline - this.clock.getAsLong());
            this.resendFuture = this.eventLoop().schedule(() -> this.safeRun(this::onResendDue), delay, TimeUnit.MILLISECONDS);
        }
    }

    private void onResendDue() {
        this.resendFuture = null;
        this.resendDeadline = Long.MAX_VALUE;
        if (this.deinitialized || this.state != RakState.CONNECTED) {
            return;
        }
        int resent = this.sendStaleDatagrams(this.ctx(), this.clock.getAsLong());
        this.internalFlush(this.ctx());
        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null && resent != 0) {
            metrics.rakStaleDatagrams(resent);
        }
    }

    /**
     * Applies the acknowledgements a just-read ACK or NACK datagram carried, then sends what they released:
     * NACK'ed datagrams are rewritten at once and a widened window can admit queued data.
     */
    public void processAcknowledgements() {
        if (this.deinitialized || this.state != RakState.CONNECTED) {
            return;
        }
        ChannelHandlerContext ctx = this.ctx();
        long curTime = this.clock.getAsLong();
        this.handleIncomingAcknowledge(ctx, curTime, this.incomingAcks, false);
        this.handleIncomingAcknowledge(ctx, curTime, this.incomingNaks, true);
        this.scheduleResend();
        this.requestFlush();
    }

    private void internalFlush(ChannelHandlerContext ctx) {
        if (this.deinitialized || this.state != RakState.CONNECTED) {
            return;
        }
        this.cancelAutoFlush();
        long curTime = this.clock.getAsLong();

        // Send pending acknowledgements.
        int mtuSize = this.getMtu();
        int ackMtu = mtuSize - RAKNET_DATAGRAM_HEADER_SIZE;
        int writtenAcks = 0;
        int writtenNacks = 0;

        while (!this.outgoingAcks.isEmpty()) {
            ByteBuf buffer = ctx.alloc().ioBuffer(ackMtu);
            buffer.writeByte(FLAG_VALID | FLAG_ACK);
            writtenAcks += RakUtils.writeAckEntries(buffer, this.outgoingAcks, ackMtu - 1);
            ctx.write(buffer);
            this.slidingWindow.onSendAck();
        }

        while (!this.outgoingNaks.isEmpty()) {
            ByteBuf buffer = ctx.alloc().ioBuffer(ackMtu);
            buffer.writeByte(FLAG_VALID | FLAG_NACK);
            writtenNacks += RakUtils.writeAckEntries(buffer, this.outgoingNaks, ackMtu - 1);
            ctx.write(buffer);
        }

        // Retransmissions have their own deadline task; normal flushes never scan pending datagrams.
        this.sendDatagrams(ctx, curTime, mtuSize);
        // Finally flush channel
        ctx.flush();

        this.scheduleResend();
        this.scheduleAutoFlush();

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.nackOut(writtenNacks);
            metrics.ackOut(writtenAcks);
            metrics.queuedPacketBytes(this.queuedBytes);
        }
    }

    private void handleIncomingAcknowledge(ChannelHandlerContext ctx, long curTime, Queue<IntRange> queue, boolean nack) {
        if (queue.isEmpty()) {
            return;
        }

        IntRange range;
        while ((range = queue.poll()) != null) {
            if (range.start < 0 || range.end > RakSequence.MASK || range.end < range.start
                    || RakSequence.difference(range.end, (int) this.datagramWriteIndex) >= 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Received {} with out-of-range indices [{}, {}] from {} (write index: {})",
                            nack ? "NACK" : "ACK", range.start, range.end, this.getRemoteAddress(), this.datagramWriteIndex);
                }
                continue;
            }

            if (range.end - range.start + 1 > this.sentDatagrams.size()) {
                // Sparse or hostile ranges must cost at most a scan of the datagrams we retain.
                Queue<RakDatagramPacket> acknowledged = new ArrayDeque<>();
                Iterator<IntObjectMap.PrimitiveEntry<RakDatagramPacket>> iterator = this.sentDatagrams.entries().iterator();
                while (iterator.hasNext()) {
                    IntObjectMap.PrimitiveEntry<RakDatagramPacket> entry = iterator.next();
                    if (entry.key() >= range.start && entry.key() <= range.end) {
                        acknowledged.add(entry.value());
                        iterator.remove();
                    }
                }
                RakDatagramPacket datagram;
                while ((datagram = acknowledged.poll()) != null) {
                    this.resendQueue.removeTyped(datagram);
                    if (nack) {
                        this.onIncomingNack(ctx, datagram, curTime);
                    } else {
                        this.onIncomingAck(datagram, curTime);
                    }
                }
                continue;
            }

            for (int i = range.start; i <= range.end; i++) {
                RakDatagramPacket datagram = this.sentDatagrams.remove(i);
                if (datagram != null) {
                    this.resendQueue.removeTyped(datagram);
                    if (nack) {
                        this.onIncomingNack(ctx, datagram, curTime);
                    } else {
                        this.onIncomingAck(datagram, curTime);
                    }
                }
            }
        }
    }

    private void onIncomingAck(RakDatagramPacket datagram, long curTime) {
        try {
            this.slidingWindow.onAck(curTime, datagram, this.datagramWriteIndex);
        } finally {
            datagram.release();
        }
    }

    private void onIncomingNack(ChannelHandlerContext ctx, RakDatagramPacket datagram, long curTime) {
        if (log.isTraceEnabled()) {
            log.trace("NAK'ed datagram {} from {}", datagram.getSequenceIndex(), this.getRemoteAddress());
        }

        this.slidingWindow.onNak(); // TODO: verify this
        this.sendDatagram(ctx, datagram, curTime);
    }

    private int sendStaleDatagrams(ChannelHandlerContext ctx, long curTime) {
        int resendCount = 0;
        int transmissionBandwidth = this.slidingWindow.getRetransmissionBandwidth();
        RakDatagramPacket datagram;
        while ((datagram = this.resendQueue.peek()) != null && datagram.getNextSend() <= curTime) {
            int size = datagram.getSize();
            if (transmissionBandwidth < size) {
                break;
            }
            transmissionBandwidth -= size;
            this.resendQueue.poll();
            this.sentDatagrams.remove(datagram.getSequenceIndex());
            this.sendDatagram(ctx, datagram, curTime);
            resendCount++;
        }

        if (resendCount != 0) {
            this.slidingWindow.onResend(this.datagramWriteIndex);
        }

        return resendCount;
    }

    private void sendDatagrams(ChannelHandlerContext ctx, long curTime, int mtuSize) {
        if (this.outgoingPackets.isEmpty()) {
            return;
        }

        int transmissionBandwidth = this.slidingWindow.getTransmissionBandwidth();
        if (transmissionBandwidth < this.outgoingPackets.peek().getSize()) {
            return;
        }
        RakDatagramPacket datagram = this.createDatagramPacket();
        datagram.setSendTime(curTime);
        EncapsulatedPacket packet;

        while ((packet = this.outgoingPackets.peek()) != null) {
            int size = packet.getSize();
            if (transmissionBandwidth < size) {
                break;
            }

            transmissionBandwidth -= size;
            this.outgoingPackets.remove();
            this.queuedBytes -= packet.getBuffer().readableBytes();

            // Send full datagram
            if (!datagram.tryAddPacket(packet, mtuSize)) {
                this.sendDatagram(ctx, datagram, curTime);

                datagram = this.createDatagramPacket();
                datagram.setSendTime(curTime);
                if (!datagram.tryAddPacket(packet, mtuSize)) {
                    throw new IllegalArgumentException("Packet too large to fit in MTU (size: " + packet.getSize() + ", MTU: " + mtuSize + ")");
                }
            }
        }

        if (!datagram.getPackets().isEmpty()) {
            this.sendDatagram(ctx, datagram, curTime);
        } else {
            datagram.release();
        }
    }

    private void sendImmediate(ChannelHandlerContext ctx, EncapsulatedPacket[] packets) {
        long curTime = this.clock.getAsLong();
        for (EncapsulatedPacket packet : packets) {
            RakDatagramPacket datagram = this.createDatagramPacket();
            datagram.setSendTime(curTime);
            if (!datagram.tryAddPacket(packet, this.getMtu())) {
                throw new IllegalArgumentException("Packet too large to fit in MTU (size: " + packet.getSize() + ", MTU: " + this.getMtu() + ")");
            }
            this.sendDatagram(ctx, datagram, curTime);
        }
        this.scheduleResend();
        ctx.flush();
    }

    private void sendDatagram(ChannelHandlerContext ctx, RakDatagramPacket datagram, long time) {
        if (datagram.getPackets().isEmpty()) {
            throw new IllegalArgumentException("RakNetDatagram with no packets");
        }

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.rakDatagramsOut(1);
        }

        int oldIndex = datagram.getSequenceIndex();
        datagram.setSequenceIndex((int) this.datagramWriteIndex++ & RakSequence.MASK);

        for (EncapsulatedPacket packet : datagram.getPackets()) {
            // Check if packet is reliable so it can be resent later if a NAK is received.
            if (packet.getReliability().isReliable()) {
                datagram.setNextSend(time + this.slidingWindow.getRtoForRetransmission());
                if (oldIndex == -1) {
                    this.slidingWindow.onReliableSend(datagram);
                }
                this.sentDatagrams.put(datagram.getSequenceIndex(), datagram.retain()); // Keep for resending
                this.resendQueue.offer(datagram);
                break;
            }
        }
        ctx.write(datagram);
    }

    private ChannelHandlerContext ctx() {
        return this.context;
    }

    // Session handlers run on the transport loop, which can differ from the child application's loop.
    private EventLoop eventLoop() {
        return this.ctx().channel().eventLoop();
    }

    private EncapsulatedPacket[] createEncapsulated(RakMessage rakMessage) {
        int maxLength = this.getMtu() - MAXIMUM_ENCAPSULATED_HEADER_SIZE - RAKNET_DATAGRAM_HEADER_SIZE;

        ByteBuf[] buffers;
        int splitId = 0;
        RakReliability reliability = rakMessage.reliability();
        ByteBuf buffer = rakMessage.content();
        int orderingChannel = rakMessage.channel();

        if (buffer.readableBytes() > maxLength) {
            // Packet requires splitting
            // Adjust reliability
            switch (reliability) {
                case UNRELIABLE:
                    reliability = RakReliability.RELIABLE;
                    break;
                case UNRELIABLE_SEQUENCED:
                    reliability = RakReliability.RELIABLE_SEQUENCED;
                    break;
                case UNRELIABLE_SEQUENCED_WITH_ACK_RECEIPT:
                    reliability = RakReliability.RELIABLE_SEQUENCED_WITH_ACK_RECEIPT;
                    break;
                case UNRELIABLE_WITH_ACK_RECEIPT:
                    reliability = RakReliability.RELIABLE_WITH_ACK_RECEIPT;
                    break;
            }

            int split = ((buffer.readableBytes() - 1) / maxLength) + 1;
            buffer.retain(split);

            buffers = new ByteBuf[split];
            for (int i = 0; i < split; i++) {
                buffers[i] = buffer.readSlice(Math.min(maxLength, buffer.readableBytes()));
            }
            if (buffer.isReadable()) {
                throw new IllegalStateException("Buffer still has bytes to read!");
            }

            // Allocate split ID
            splitId = this.splitIndex++;
        } else {
            buffers = new ByteBuf[]{buffer.readRetainedSlice(buffer.readableBytes())};
        }

        // Set meta
        int orderingIndex = 0;
        int sequenceIndex = 0;
        if (reliability.isSequenced()) {
            orderingIndex = this.orderWriteIndex[orderingChannel];
            sequenceIndex = this.sequenceWriteIndex[orderingChannel];
            this.sequenceWriteIndex[orderingChannel] = (sequenceIndex + 1) & RakSequence.MASK;
        } else if (reliability.isOrdered()) {
            orderingIndex = this.orderWriteIndex[orderingChannel];
            this.orderWriteIndex[orderingChannel] = (orderingIndex + 1) & RakSequence.MASK;
            this.sequenceWriteIndex[orderingChannel] = 0;
        }

        // Now create the packets.
        EncapsulatedPacket[] packets = new EncapsulatedPacket[buffers.length];
        for (int i = 0, parts = buffers.length; i < parts; i++) {
            EncapsulatedPacket packet = this.createEncapsulatedPacket();
            packet.setBuffer(buffers[i]);
            packet.setOrderingChannel((short) orderingChannel);
            packet.setOrderingIndex(orderingIndex);
            packet.setSequenceIndex(sequenceIndex);
            packet.setReliability(reliability);
            if (reliability.isReliable()) {
                packet.setReliabilityIndex(this.reliabilityWriteIndex);
                this.reliabilityWriteIndex = (this.reliabilityWriteIndex + 1) & RakSequence.MASK;
            }

            if (parts > 1) {
                packet.setSplit(true);
                packet.setPartIndex(i);
                packet.setPartCount(parts);
                packet.setPartId(splitId);
            }

            packets[i] = packet;
        }
        return packets;
    }

    private long getNextWeight(RakPriority priority) {
        int priorityLevel = priority.ordinal();
        long next = this.outgoingPacketNextWeights[priorityLevel];

        if (!this.outgoingPackets.isEmpty()) {
            if (next >= this.lastMinWeight) {
                next = this.lastMinWeight + (1L << priorityLevel) * priorityLevel + priorityLevel;
                this.outgoingPacketNextWeights[priorityLevel] = next + (1L << priorityLevel) * (priorityLevel + 1) + priorityLevel;
            }
        } else {
            this.initHeapWeights();
        }
        this.lastMinWeight = next - (1L << priorityLevel) * priorityLevel + priorityLevel;
        return next;
    }

    public void disconnect() {
        this.disconnect(RakDisconnectReason.DISCONNECTED);
    }

    public void disconnect(RakDisconnectReason reason) {
        // Ensure we disconnect on the right thread
        if (this.channel.parent().eventLoop().inEventLoop()) {
            this.disconnect0(reason);
        } else {
            this.channel.parent().eventLoop().execute(() -> this.disconnect0(reason));
        }
    }

    private ChannelPromise disconnect0(RakDisconnectReason reason) {
        if (this.deinitialized || this.state == RakState.UNCONNECTED || this.state == RakState.DISCONNECTING) {
            return this.channel.voidPromise();
        }
        this.setState(RakState.DISCONNECTING);

        if (log.isDebugEnabled()) {
            log.debug("Disconnecting RakNet Session ({} => {}) due to {}", this.channel.localAddress(), this.getRemoteAddress(), reason);
        }

        ChannelHandlerContext ctx = this.ctx();

        ByteBuf buffer = ctx.alloc().ioBuffer(1);
        buffer.writeByte(ID_DISCONNECTION_NOTIFICATION);
        RakMessage rakMessage = new RakMessage(buffer, RakReliability.RELIABLE, RakPriority.IMMEDIATE);

        ChannelPromise promise = ctx.newPromise();
        promise.addListener((ChannelFuture future) -> // The channel provided in ChannelFuture is parent channel,
                this.channel.pipeline().fireUserEventTriggered(reason).close()); // but we want RakChannel instead
        this.write(ctx, rakMessage, promise);
        return promise;
    }

    public void close(RakDisconnectReason reason) {
        if (this.deinitialized || this.state == RakState.DISCONNECTING) {
            return;
        }
        this.setState(RakState.DISCONNECTING);

        if (log.isDebugEnabled()) {
            log.debug("Closing RakNet Session ({} => {}) due to {}", this.channel.localAddress(), this.getRemoteAddress(), reason);
        }

        this.channel.pipeline().fireUserEventTriggered(reason).close();
    }

    public boolean isClosed() {
        return this.deinitialized || this.state == RakState.UNCONNECTED;
    }

    private void checkForClosed() {
        if (this.isClosed()) {
            throw new IllegalStateException("RakSession is closed!");
        }
    }

    private void setState(RakState state) {
        if (this.state == state) {
            return;
        }
        this.state = state;

        RakChannelMetrics metrics = this.getMetrics();
        if (metrics != null) {
            metrics.stateChange(state);
        }
    }

    public void recalculatePongTime(long pingTime) {
        if (this.currentPingTime == pingTime) {
            this.lastPingTime = this.currentPingSentAt;
            this.lastPongTime = this.clock.getAsLong();
        }
    }

    private void touch() {
        this.checkForClosed();
        this.lastTouched = System.currentTimeMillis();
        this.lastActivity = this.clock.getAsLong();
    }

    public boolean isStale(long curTime) {
        return curTime - this.lastTouched >= SESSION_STALE_MS;
    }

    public boolean isStale() {
        return this.isStale(System.currentTimeMillis());
    }

    public boolean isTimedOut(long curTime) {
        return curTime - this.lastTouched >= this.channel.config().getOption(RakChannelOption.RAK_SESSION_TIMEOUT);
    }

    public boolean isTimedOut() {
        return this.isTimedOut(System.currentTimeMillis());
    }

    public long getPing() {
        return this.lastPongTime - this.lastPingTime;
    }

    public double getRTT() {
        return this.slidingWindow.getRTT();
    }

    public int getMtu() {
        return this.channel.config().getMtu() - UDP_HEADER_SIZE - (this.getRemoteAddress().getAddress() instanceof Inet6Address ? 40 : 20);
    }

    public RakChannelMetrics getMetrics() {
        return this.channel.config().getMetrics();
    }

    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) this.channel.remoteAddress();
    }

    protected Queue<IntRange> getAcknowledgeQueue(boolean nack) {
        return nack ? this.incomingNaks : this.incomingAcks;
    }

    public Channel getChannel() {
        return channel;
    }

    RakDatagramPacket createDatagramPacket() {
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        datagram.setFlag(FLAG_NEEDS_B_AND_AS);
        return datagram;
    }

    EncapsulatedPacket createEncapsulatedPacket() {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setNeedsBAS(true);
        return packet;
    }
}

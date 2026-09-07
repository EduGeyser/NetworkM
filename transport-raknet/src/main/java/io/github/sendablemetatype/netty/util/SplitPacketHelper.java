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

package io.github.sendablemetatype.netty.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.github.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SplitPacketHelper extends AbstractReferenceCounted {
    private static final long TIMEOUT_MILLIS = 30000;
    private final IntObjectMap<EncapsulatedPacket> packets = new IntObjectHashMap<>();
    private final int expectedLength;
    private final int partId;
    private final long created;
    private int reassembledSize;

    public SplitPacketHelper(int partId, long expectedLength) {
        this(partId, expectedLength, TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    /** Creates a reassembly using the session's monotonic clock, in milliseconds. */
    public SplitPacketHelper(int partId, long expectedLength, long created) {
        if (expectedLength < 2) {
            throw new IllegalArgumentException("expectedLength must be greater than 1");
        }
        if (expectedLength > 8192) {
            throw new IllegalArgumentException("Too many split parts, expectedLength must be less than 8192");
        }
        this.partId = partId;
        this.created = created;
        this.expectedLength = (int) expectedLength;
    }

    /**
     * Whether this helper is reassembling the split packet the given part was cut from.
     */
    public boolean matches(EncapsulatedPacket packet) {
        return this.partId == packet.getPartId() && this.expectedLength == packet.getPartCount();
    }

    public EncapsulatedPacket add(EncapsulatedPacket packet, ByteBufAllocator alloc) {
        Objects.requireNonNull(packet, "packet cannot be null");
        if (!packet.isSplit()) throw new IllegalArgumentException("Packet is not split");
        if (this.refCnt() <= 0) throw new IllegalReferenceCountException(this.refCnt());
        if (packet.getPartIndex() < 0 || packet.getPartIndex() >= this.expectedLength) {
            throw new IllegalArgumentException(String.format("Split packet part index out of range. Got %s, expected 0-%s",
                    packet.getPartIndex(), this.expectedLength - 1));
        }

        int partIndex = packet.getPartIndex();
        if (this.packets.containsKey(partIndex)) {
            // Duplicate
            return null;
        }
        // Retain the packet so it can be reassembled later.
        this.packets.put(partIndex, packet.retain());
        this.reassembledSize += packet.getBuffer().readableBytes();

        if (this.packets.size() != this.expectedLength) {
            return null;
        }

        // We can't use a composite buffer as the native code will choke on it
        ByteBuf reassembled = alloc.ioBuffer(this.reassembledSize);
        for (int i = 0; i < this.expectedLength; i++) {
            EncapsulatedPacket netPacket = this.packets.get(i);
            ByteBuf buf = netPacket.getBuffer();
            reassembled.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
        }

        return packet.fromSplit(reassembled);
    }

    /**
     * The number of payload bytes currently retained by this reassembly across all received parts.
     */
    public int getReassembledSize() {
        return this.reassembledSize;
    }

    public boolean expired() {
        return this.expired(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    public boolean expired(long now) {
        // If we're waiting on a split packet for more than 30 seconds, the client on the other end is either severely
        // lagging, or has died.
        if (this.refCnt() <= 0) throw new IllegalReferenceCountException(this.refCnt());
        return now - this.created >= TIMEOUT_MILLIS;
    }

    public long getExpiresAt() {
        return this.created + TIMEOUT_MILLIS;
    }

    @Override
    protected void deallocate() {
        for (EncapsulatedPacket packet : this.packets.values()) {
            ReferenceCountUtil.release(packet);
        }
        this.packets.clear();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        throw new UnsupportedOperationException();
    }
}

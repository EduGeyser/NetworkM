/*
 * Copyright 2025 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.cloudburstmc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.util.SplitPacketHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SplitPacketHelperTests {

    private static final PooledByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;

    private static EncapsulatedPacket part(int partCount, int partIndex, int payloadBytes) {
        ByteBuf buffer = ALLOC.ioBuffer(payloadBytes);
        buffer.writeZero(payloadBytes);

        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.RELIABLE);
        packet.setSplit(true);
        packet.setPartCount(partCount);
        packet.setPartId(0);
        packet.setPartIndex(partIndex);
        packet.setBuffer(buffer);
        return packet;
    }

    @Test
    public void reassembledSizeTracksRetainedBytes() {
        SplitPacketHelper helper = new SplitPacketHelper(0, 3);
        Assertions.assertEquals(0, helper.getReassembledSize());

        EncapsulatedPacket p0 = part(3, 0, 100);
        Assertions.assertNull(helper.add(p0, ALLOC));
        Assertions.assertEquals(100, helper.getReassembledSize());
        p0.release();

        // Duplicate part must not be counted twice.
        EncapsulatedPacket dup = part(3, 0, 100);
        Assertions.assertNull(helper.add(dup, ALLOC));
        Assertions.assertEquals(100, helper.getReassembledSize());
        dup.release();

        EncapsulatedPacket p1 = part(3, 1, 50);
        Assertions.assertNull(helper.add(p1, ALLOC));
        Assertions.assertEquals(150, helper.getReassembledSize());
        p1.release();

        // Final part completes the reassembly; the helper still reports the full retained size until released.
        EncapsulatedPacket p2 = part(3, 2, 25);
        EncapsulatedPacket reassembled = helper.add(p2, ALLOC);
        Assertions.assertNotNull(reassembled);
        Assertions.assertEquals(175, helper.getReassembledSize());
        Assertions.assertEquals(175, reassembled.getBuffer().readableBytes());
        reassembled.release();
        p2.release();

        helper.release();
    }

    @Test
    public void expiresAfterTimeout() {
        SplitPacketHelper helper = new SplitPacketHelper(0, 2, 100);
        Assertions.assertFalse(helper.expired(30099));
        Assertions.assertTrue(helper.expired(30100));
        helper.release();
    }

    @Test
    public void sparsePartsKeepIdentityAndEnforceThePartLimit() {
        SplitPacketHelper helper = new SplitPacketHelper(0, 8192);
        EncapsulatedPacket sparse = part(8192, 8191, 1);
        try {
            Assertions.assertTrue(helper.matches(sparse));
            Assertions.assertNull(helper.add(sparse, ALLOC));
            Assertions.assertEquals(1, helper.getReassembledSize());
            sparse.setPartId(256);
            Assertions.assertFalse(helper.matches(sparse));
            sparse.setPartIndex(8192);
            Assertions.assertThrows(IllegalArgumentException.class, () -> helper.add(sparse, ALLOC));
            Assertions.assertThrows(IllegalArgumentException.class, () -> new SplitPacketHelper(0, 8193));
        } finally {
            sparse.release();
            helper.release();
        }
    }

    @Test
    public void outOfOrderPartsAndDuplicatesReassembleOnce() {
        SplitPacketHelper helper = new SplitPacketHelper(0, 3);
        EncapsulatedPacket last = part(3, 2, 3);
        last.getBuffer().setZero(0, 3).setByte(0, 3);
        EncapsulatedPacket first = part(3, 0, 1);
        first.getBuffer().setByte(0, 1);
        EncapsulatedPacket middle = part(3, 1, 2);
        middle.getBuffer().setByte(0, 2);
        try {
            Assertions.assertNull(helper.add(last, ALLOC));
            Assertions.assertNull(helper.add(last, ALLOC));
            Assertions.assertNull(helper.add(first, ALLOC));
            EncapsulatedPacket reassembled = helper.add(middle, ALLOC);
            Assertions.assertNotNull(reassembled);
            try {
                ByteBuf buffer = reassembled.getBuffer();
                Assertions.assertEquals(6, buffer.readableBytes());
                Assertions.assertEquals(1, buffer.getByte(0));
                Assertions.assertEquals(2, buffer.getByte(1));
                Assertions.assertEquals(3, buffer.getByte(3));
            } finally {
                reassembled.release();
            }
        } finally {
            first.release();
            middle.release();
            last.release();
            helper.release();
        }
    }
}

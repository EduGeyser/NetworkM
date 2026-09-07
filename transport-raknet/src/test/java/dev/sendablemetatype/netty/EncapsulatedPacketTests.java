package dev.sendablemetatype.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import dev.sendablemetatype.netty.channel.raknet.RakReliability;
import dev.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class EncapsulatedPacketTests {
    @ParameterizedTest
    @EnumSource(RakReliability.class)
    void everyReliabilityUsesTheReferenceWireLayout(RakReliability reliability) {
        EncapsulatedPacket source = EncapsulatedPacket.newInstance();
        EncapsulatedPacket decoded = EncapsulatedPacket.newInstance();
        CompositeByteBuf wire = Unpooled.compositeBuffer();
        source.setReliability(reliability);
        source.setReliabilityIndex(0x123456);
        source.setSequenceIndex(0x234567);
        source.setOrderingIndex(0x345678);
        source.setOrderingChannel((short) 3);
        source.setBuffer(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        try {
            source.encode(wire);
            int id = switch (reliability) {
                case UNRELIABLE, UNRELIABLE_WITH_ACK_RECEIPT -> 0;
                case UNRELIABLE_SEQUENCED, UNRELIABLE_SEQUENCED_WITH_ACK_RECEIPT -> 1;
                case RELIABLE, RELIABLE_WITH_ACK_RECEIPT -> 2;
                case RELIABLE_ORDERED, RELIABLE_ORDERED_WITH_ACK_RECEIPT -> 3;
                case RELIABLE_SEQUENCED, RELIABLE_SEQUENCED_WITH_ACK_RECEIPT -> 4;
            };
            assertEquals(id, wire.getUnsignedByte(0) >>> 5);
            assertEquals(source.getSize(), wire.readableBytes());
            decoded.decode(wire);
            assertEquals(RakReliability.fromId(id), decoded.getReliability());
            assertArrayEquals(new byte[]{1, 2, 3}, new byte[]{decoded.getBuffer().readByte(), decoded.getBuffer().readByte(), decoded.getBuffer().readByte()});
            if (reliability.isReliable()) {
                assertEquals(0x123456, decoded.getReliabilityIndex());
            }
            if (reliability.isSequenced()) {
                assertEquals(0x234567, decoded.getSequenceIndex());
            }
            if (reliability.isOrdered() || reliability.isSequenced()) {
                assertEquals(0x345678, decoded.getOrderingIndex());
                assertEquals(3, decoded.getOrderingChannel());
            }
            assertFalse(wire.isReadable());
        } finally {
            source.release();
            decoded.release();
            wire.release();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 6, 7})
    void legacyReceiptIdsDecodeWithTheirCanonicalHeader(int id) {
        ByteBuf wire = Unpooled.buffer();
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        try {
            wire.writeByte(id << 5).writeShort(8);
            if (id >= 6) {
                wire.writeMediumLE(42);
            }
            if (id == 7) {
                wire.writeMediumLE(17).writeByte(2);
            }
            wire.writeByte(99);
            packet.decode(wire);
            assertTrue(packet.getReliability().isWithAckReceipt());
            assertEquals(id >= 6, packet.getReliability().isReliable());
            assertEquals(id == 7, packet.getReliability().isOrdered());
            assertEquals(99, packet.getBuffer().readUnsignedByte());
            assertFalse(wire.isReadable());
        } finally {
            packet.release();
            wire.release();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 8192})
    void invalidPayloadLengthsCannotCorruptTheWireFormat(int size) {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        CompositeByteBuf wire = Unpooled.compositeBuffer();
        ByteBuf payload = Unpooled.buffer(size).writeZero(size);
        packet.setReliability(RakReliability.UNRELIABLE);
        packet.setBuffer(payload);
        try {
            assertThrows(IllegalArgumentException.class, () -> packet.encode(wire));
            assertEquals(0, wire.numComponents());
            assertEquals(1, payload.refCnt());
        } finally {
            packet.release();
            wire.release();
        }
    }
}

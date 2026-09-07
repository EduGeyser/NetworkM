package dev.sendablemetatype.netty.handler.codec.raknet.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.EncoderException;
import dev.sendablemetatype.netty.channel.raknet.RakReliability;
import dev.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;
import dev.sendablemetatype.netty.channel.raknet.packet.RakDatagramPacket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static dev.sendablemetatype.netty.channel.raknet.RakConstants.FLAG_VALID;
import static org.junit.jupiter.api.Assertions.*;

class RakDatagramCodecTests {
    @Test
    void failedEncodeReleasesPreviouslyRetainedComponents() {
        EmbeddedChannel channel = new EmbeddedChannel(new RakDatagramCodec());
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        ByteBuf first = Unpooled.buffer(8).writeZero(8);
        ByteBuf oversized = Unpooled.buffer(8192).writeZero(8192);
        datagram.getPackets().add(encapsulated(first));
        datagram.getPackets().add(encapsulated(oversized));
        try {
            assertThrows(EncoderException.class, () -> channel.writeOutbound(datagram));
            assertEquals(0, first.refCnt(), "The composite must release payloads retained before the failure");
            assertEquals(0, oversized.refCnt());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void successfulEncodeRetainsThePayloadUntilTheWireBufferIsReleased() {
        EmbeddedChannel channel = new EmbeddedChannel(new RakDatagramCodec());
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        ByteBuf payload = Unpooled.buffer(1).writeByte(99);
        datagram.setSequenceIndex(42);
        datagram.getPackets().add(encapsulated(payload));
        try {
            assertTrue(channel.writeOutbound(datagram));
            assertEquals(1, payload.refCnt());
            ByteBuf wire = channel.readOutbound();
            try {
                assertEquals(FLAG_VALID & 0xff, wire.readUnsignedByte());
                assertEquals(42, wire.readUnsignedMediumLE());
                assertEquals(0, wire.readUnsignedByte());
                assertEquals(8, wire.readUnsignedShort());
                assertEquals(99, wire.readUnsignedByte());
                assertFalse(wire.isReadable());
            } finally {
                wire.release();
            }
            assertEquals(0, payload.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void emptyInputIsReleasedWithoutProducingAPacket() {
        EmbeddedChannel channel = new EmbeddedChannel(new RakDatagramCodec());
        ByteBuf wire = Unpooled.buffer(1);
        try {
            assertFalse(channel.writeInbound(wire));
            assertEquals(0, wire.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
    void incompleteOrEmptyDataPacketsAreRejected(int size) {
        EmbeddedChannel channel = new EmbeddedChannel(new RakDatagramCodec());
        ByteBuf wire = Unpooled.buffer(size).writeZero(size).setByte(0, FLAG_VALID);
        try {
            assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(wire));
            assertEquals(0, wire.refCnt());
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void aTruncatedLaterPacketReleasesEarlierDecodedPayloads() {
        EmbeddedChannel channel = new EmbeddedChannel(new RakDatagramCodec());
        ByteBuf wire = Unpooled.buffer();
        wire.writeByte(FLAG_VALID).writeMediumLE(42);
        wire.writeByte(0).writeShort(8).writeByte(99);
        wire.writeByte(2 << 5).writeShort(8).writeMediumLE(3);
        try {
            assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(wire));
            assertEquals(0, wire.refCnt(), "No slice from the earlier packet may survive the malformed datagram");
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 0xa0, 0xc0})
    void otherPacketTypesPassThroughWithoutConsumingBytes(int flags) {
        EmbeddedChannel channel = new EmbeddedChannel(new RakDatagramCodec());
        ByteBuf wire = Unpooled.buffer(1).writeByte(flags);
        try {
            assertTrue(channel.writeInbound(wire));
            ByteBuf forwarded = channel.readInbound();
            try {
                assertSame(wire, forwarded);
                assertEquals(0, forwarded.readerIndex());
                assertEquals(flags, forwarded.readUnsignedByte());
            } finally {
                forwarded.release();
            }
            assertEquals(0, wire.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static EncapsulatedPacket encapsulated(ByteBuf payload) {
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.UNRELIABLE);
        packet.setBuffer(payload);
        return packet;
    }
}

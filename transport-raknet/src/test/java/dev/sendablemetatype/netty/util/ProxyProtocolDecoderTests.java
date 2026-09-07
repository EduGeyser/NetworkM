package dev.sendablemetatype.netty.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolException;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class ProxyProtocolDecoderTests {

    @ParameterizedTest
    @ValueSource(ints = {0, 7, 48})
    void binaryHeaderRespectsReaderIndexAndLeavesPayload(int prefixLength) {
        ByteBuf packet = Unpooled.buffer();
        HAProxyMessage message = null;
        try {
            packet.writeZero(prefixLength);
            writeHeader(packet, 0);
            packet.writeInt(0x12345678);
            packet.readerIndex(prefixLength);

            message = ProxyProtocolDecoder.decode(packet, ProxyProtocolDecoder.findVersion(packet));

            assertNotNull(message);
            assertEquals(HAProxyProxiedProtocol.UDP4, message.proxiedProtocol());
            assertEquals("192.0.2.1", message.sourceAddress());
            assertEquals(12345, message.sourcePort());
            assertEquals(prefixLength + 28, packet.readerIndex());
            assertEquals(4, packet.readableBytes());
            assertEquals(0x12345678, packet.readInt());
        } finally {
            if (message != null) {
                message.release();
            }
            packet.release();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4})
    void rejectsTruncatedTlvHeadersAndValues(int tlvLength) {
        ByteBuf packet = Unpooled.buffer();
        try {
            writeHeader(packet, tlvLength);
            packet.writeByte(0x04);
            if (tlvLength >= 3) {
                packet.writeShort(tlvLength - 2);
                packet.writeZero(tlvLength - 3);
            } else {
                packet.writeZero(tlvLength - 1);
            }
            packet.writeInt(0x12345678);

            assertThrows(HAProxyProtocolException.class,
                    () -> ProxyProtocolDecoder.decode(packet, ProxyProtocolDecoder.findVersion(packet)));
            assertEquals(4, packet.readableBytes(), "A TLV must not consume the datagram payload");
        } finally {
            packet.release();
        }
    }

    @Test
    void acceptsEmptyAndUnknownTlvs() {
        ByteBuf packet = Unpooled.buffer();
        HAProxyMessage message = null;
        try {
            writeHeader(packet, 8);
            packet.writeByte(0x04).writeShort(0);
            packet.writeByte(0xf0).writeShort(2).writeShort(0xabcd);
            packet.writeInt(0x12345678);

            message = ProxyProtocolDecoder.decode(packet, ProxyProtocolDecoder.findVersion(packet));

            assertNotNull(message);
            assertEquals(4, packet.readableBytes());
            assertEquals(0x12345678, packet.readInt());
        } finally {
            if (message != null) {
                message.release();
            }
            packet.release();
        }
    }

    private static void writeHeader(ByteBuf packet, int tlvLength) {
        packet.writeBytes(ProxyProtocolDecoder.BINARY_PREFIX);
        packet.writeByte(0x21);
        packet.writeByte(HAProxyProxiedProtocol.UDP4.byteValue());
        packet.writeShort(12 + tlvLength);
        packet.writeInt(0xc0000201);
        packet.writeInt(0xc0000202);
        packet.writeShort(12345);
        packet.writeShort(19132);
    }
}

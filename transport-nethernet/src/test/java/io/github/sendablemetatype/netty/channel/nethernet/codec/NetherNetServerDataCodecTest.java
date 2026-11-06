package io.github.sendablemetatype.netty.channel.nethernet.codec;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class NetherNetServerDataCodecTest {
    private static final LanServerData.V6 STABLE_DATA = new LanServerData.V6("Dedicated Server", "Bedrock level",
            0, 0, 10, false, false, true, false, "3e8a5e7a932c5aa4", 2, 4);
    private static final LanServerData.V7 PREVIEW_DATA = new LanServerData.V7("Dedicated Server", 2207, "1.26.60",
            "Bedrock level", 0, 10, 0, false, false, false, false, "24728cbd0c0b6df4", 4);

    /** Upstream CloudburstMC/Network's ServerDataTest vector, labeled there as what BDS 1.26.51 answered. */
    private static final String BDS_1_26_51 = "071542445320312e32362e3531204e65746865724e6574a22207312e32362e3531"
            + "0774657374656e7600280200000101106436643563343535636333343537303008";
    private static final LanServerData.V7 BDS_1_26_51_DATA = new LanServerData.V7("BDS 1.26.51 NetherNet", 2193,
            "1.26.51", "testenv", 0, 20, 1, false, false, true, true, "d6d5c455cc345700", 4);

    @Test
    void encodingMatchesTheBds12660PreviewResponse() throws Exception {
        byte[] capture = fixture("preview-1.26.60.21-v7.hex");
        assertEquals(67, capture.length);
        assertArrayEquals(capture, encode(PREVIEW_DATA));
    }

    @Test
    void encodingMatchesUpstreamsBds12651Vector() {
        assertEquals(BDS_1_26_51, ByteBufUtil.hexDump(encode(BDS_1_26_51_DATA)));
    }

    @Test
    void bothRecordedVersionsDecodeAndRespectTheReaderIndexAndOwnership() throws Exception {
        assertEquals(PREVIEW_DATA, decodeAfterPrefix(fixture("preview-1.26.60.21-v7.hex")));
        assertEquals(STABLE_DATA, decodeAfterPrefix(fixture("stable-1.26.45.1-v6.hex")));
        assertEquals(BDS_1_26_51_DATA, decodeAfterPrefix(ByteBufUtil.decodeHexDump(BDS_1_26_51)));
    }

    @Test
    void negativeAndMultiByteNumbersRoundTripAsZigZagVarints() {
        LanServerData.V7 data = new LanServerData.V7("世界😀".repeat(40), Integer.MIN_VALUE, "", "Ž".repeat(80),
                -1, Integer.MAX_VALUE, 16384, true, true, true, false, "a", -128);
        ByteBuf buffer = Unpooled.wrappedBuffer(encode(data));
        try {
            assertEquals(data, NetherNetServerDataCodec.decode(buffer));
        } finally {
            buffer.release();
        }
    }

    @Test
    void everyTruncationOfEitherVersionIsRejected() throws Exception {
        for (String name : new String[]{"preview-1.26.60.21-v7.hex", "stable-1.26.45.1-v6.hex"}) {
            byte[] capture = fixture(name);
            for (int length = 0; length < capture.length; length++) {
                ByteBuf buffer = Unpooled.wrappedBuffer(capture, 0, length);
                try {
                    assertThrows(IllegalArgumentException.class, () -> NetherNetServerDataCodec.decode(buffer),
                            name + " truncated at byte " + length);
                } finally {
                    buffer.release();
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 5, 8, 255})
    void otherVersionsAreRejected(int version) throws Exception {
        ByteBuf buffer = Unpooled.wrappedBuffer(fixture("preview-1.26.60.21-v7.hex"));
        buffer.setByte(0, version);
        try {
            assertThrows(IllegalArgumentException.class, () -> NetherNetServerDataCodec.decode(buffer));
        } finally {
            buffer.release();
        }
    }

    /** Offsets of editor, hardcore, onlineAuth and selfSignedAuth in the preview response. */
    @ParameterizedTest
    @ValueSource(ints = {45, 46, 47, 48})
    void booleanFieldsRejectValuesOtherThanZeroAndOne(int offset) throws Exception {
        ByteBuf buffer = Unpooled.wrappedBuffer(fixture("preview-1.26.60.21-v7.hex"));
        buffer.setByte(offset, 2);
        try {
            assertThrows(IllegalArgumentException.class, () -> NetherNetServerDataCodec.decode(buffer));
        } finally {
            buffer.release();
        }
    }

    @Test
    void trailingBytesAreRejected() throws Exception {
        ByteBuf buffer = Unpooled.buffer().writeBytes(fixture("preview-1.26.60.21-v7.hex")).writeByte(0);
        try {
            assertThrows(IllegalArgumentException.class, () -> NetherNetServerDataCodec.decode(buffer));
        } finally {
            buffer.release();
        }
    }

    private static byte[] encode(LanServerData.V7 data) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            NetherNetServerDataCodec.encode(buffer, data);
            return ByteBufUtil.getBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    private static LanServerData decodeAfterPrefix(byte[] record) {
        ByteBuf buffer = Unpooled.buffer().writeZero(3).writeBytes(record).skipBytes(3);
        try {
            LanServerData data = NetherNetServerDataCodec.decode(buffer);
            assertFalse(buffer.isReadable());
            assertEquals(1, buffer.refCnt());
            return data;
        } finally {
            buffer.release();
        }
    }

    /*
     * stable-1.26.45.1-v6.hex: the decoded stable BDS capture the owner supplied on 2026-09-06.
     * preview-1.26.60.21-v7.hex: rebuilt from the owner's decoded BDS 1.26.60.21 preview response
     * of the same day, which records every field value and the 67-byte length but not the bytes.
     * The name and level are BDS's defaults, as in the stable capture; with them the record is
     * exactly 67 bytes.
     */
    private static byte[] fixture(String name) throws IOException {
        try (var stream = NetherNetServerDataCodecTest.class.getResourceAsStream("/discovery/" + name)) {
            assertNotNull(stream);
            return ByteBufUtil.decodeHexDump(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).strip());
        }
    }
}

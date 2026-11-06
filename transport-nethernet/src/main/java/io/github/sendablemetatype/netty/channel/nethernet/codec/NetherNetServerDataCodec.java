package io.github.sendablemetatype.netty.channel.nethernet.codec;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;

/**
 * Binary server data for LAN discovery responses, without the outer hex encoding and framing.
 *
 * <p>Servers write v7 only. Readers accept v6 and v7. Strings are a varuint32 byte length and
 * UTF-8 bytes; numbers are zigzag varint32 except v6's fixed-width little-endian player counts;
 * booleans are one byte, 0 or 1.
 *
 * <p>The v7 layout comes from upstream CloudburstMC/Network's {@code ServerData} (labeled there
 * as what BDS 1.26.51 sends) and from the owner's decoded BDS 1.26.60.21 preview response
 * (2026-09-06). It is to be confirmed by a retail capture: no current retail client response
 * has been captured yet. The v6 layout matches a stable BDS 1.26.45.1 capture.
 */
public final class NetherNetServerDataCodec {
    /** The version servers write. */
    public static final int VERSION = 7;

    private NetherNetServerDataCodec() {
    }

    /**
     * Writes the version byte and all v7 fields.
     *
     * @param buffer the caller-owned output buffer
     * @param data the complete advertisement
     */
    public static void encode(ByteBuf buffer, LanServerData.V7 data) {
        buffer.writeByte(VERSION);
        writeString(buffer, data.serverName());
        writeSignedVarInt(buffer, data.protocol());
        writeString(buffer, data.version());
        writeString(buffer, data.levelName());
        writeSignedVarInt(buffer, data.playerCount());
        writeSignedVarInt(buffer, data.maxPlayerCount());
        writeSignedVarInt(buffer, data.gameType());
        buffer.writeBoolean(data.editorWorld());
        buffer.writeBoolean(data.hardcore());
        buffer.writeBoolean(data.onlineAuth());
        buffer.writeBoolean(data.selfSignedAuth());
        writeString(buffer, data.nonce());
        writeSignedVarInt(buffer, data.connectionType());
    }

    /**
     * Reads one complete v6 or v7 record. Other versions and trailing bytes are rejected.
     *
     * @param buffer the caller-owned buffer containing only the binary record
     * @return the decoded advertisement
     * @throws IllegalArgumentException if the record is malformed or of another version
     */
    public static LanServerData decode(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            throw new IllegalArgumentException("Missing LAN advertisement version");
        }
        int version = buffer.readUnsignedByte();
        LanServerData data = switch (version) {
            case 6 -> new LanServerData.V6(readString(buffer), readString(buffer), readSignedVarInt(buffer),
                    readIntLE(buffer), readIntLE(buffer), readBoolean(buffer), readBoolean(buffer),
                    readBoolean(buffer), readBoolean(buffer), readString(buffer), readSignedVarInt(buffer),
                    readSignedVarInt(buffer));
            case 7 -> new LanServerData.V7(readString(buffer), readSignedVarInt(buffer), readString(buffer),
                    readString(buffer), readSignedVarInt(buffer), readSignedVarInt(buffer), readSignedVarInt(buffer),
                    readBoolean(buffer), readBoolean(buffer), readBoolean(buffer), readBoolean(buffer),
                    readString(buffer), readSignedVarInt(buffer));
            default -> throw new IllegalArgumentException("Unsupported LAN advertisement version: " + version);
        };
        if (buffer.isReadable()) {
            throw new IllegalArgumentException("Trailing bytes in LAN advertisement");
        }
        return data;
    }

    private static void writeString(ByteBuf buffer, String value) {
        int length = ByteBufUtil.utf8Bytes(value);
        writeUnsignedVarInt(buffer, length);
        ByteBufUtil.reserveAndWriteUtf8(buffer, value, length);
    }

    private static void writeSignedVarInt(ByteBuf buffer, int value) {
        writeUnsignedVarInt(buffer, (value << 1) ^ (value >> 31));
    }

    private static void writeUnsignedVarInt(ByteBuf buffer, int value) {
        while ((value & ~0x7f) != 0) {
            buffer.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        buffer.writeByte(value);
    }

    private static String readString(ByteBuf buffer) {
        int length = readUnsignedVarInt(buffer);
        if (length < 0 || length > buffer.readableBytes()) {
            throw new IllegalArgumentException("Invalid LAN advertisement string length");
        }
        return buffer.readCharSequence(length, StandardCharsets.UTF_8).toString();
    }

    private static int readIntLE(ByteBuf buffer) {
        if (!buffer.isReadable(Integer.BYTES)) {
            throw new IllegalArgumentException("Truncated LAN advertisement integer");
        }
        return buffer.readIntLE();
    }

    private static boolean readBoolean(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            throw new IllegalArgumentException("Truncated LAN advertisement boolean");
        }
        int value = buffer.readUnsignedByte();
        if (value > 1) {
            throw new IllegalArgumentException("Invalid LAN advertisement boolean: " + value);
        }
        return value != 0;
    }

    private static int readSignedVarInt(ByteBuf buffer) {
        int encoded = readUnsignedVarInt(buffer);
        return (encoded >>> 1) ^ -(encoded & 1);
    }

    private static int readUnsignedVarInt(ByteBuf buffer) {
        int value = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            if (!buffer.isReadable()) {
                throw new IllegalArgumentException("Truncated LAN advertisement varint");
            }
            int next = buffer.readUnsignedByte();
            if (shift == 28 && (next & 0xf0) != 0) {
                throw new IllegalArgumentException("LAN advertisement varint exceeds 32 bits");
            }
            value |= (next & 0x7f) << shift;
            if ((next & 0x80) == 0) {
                return value;
            }
        }
        throw new IllegalArgumentException("LAN advertisement varint exceeds 32 bits");
    }
}

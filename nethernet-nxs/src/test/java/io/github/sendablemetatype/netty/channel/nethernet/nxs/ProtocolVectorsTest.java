package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The specification's protocol vectors, which have no Node verifier: the RFC 5769 STUN sample, whose
 * MESSAGE-INTEGRITY is recomputed here. The NetherNet frame vectors are checked against the transport's own
 * framing in transport-nethernet ({@code NetherNetMessageAssemblerTest}, {@code NetherNetSegmentTest}).
 */
class ProtocolVectorsTest {
    private static final HexFormat HEX = HexFormat.of();
    private static final JsonObject VECTORS = fixture();

    private static JsonObject fixture() {
        try {
            return ProviderInteropTest.fixture("cloudburst-protocol-vectors.v1.json");
        } catch (Exception unavailable) {
            throw new ExceptionInInitializerError(unavailable);
        }
    }

    @Test
    void theStunSampleCarriesTheIntegrityItsPasswordGives() throws Exception {
        JsonObject sample = VECTORS.getAsJsonObject("stun").getAsJsonObject("rfc5769");
        byte[] packet = HEX.parseHex(sample.get("packetHex").getAsString());
        ByteBuffer message = ByteBuffer.wrap(packet);
        assertEquals(0x2112a442, message.getInt(4), "Magic cookie");
        assertEquals(sample.get("transactionIdHex").getAsString(), HEX.formatHex(packet, 8, 20));
        String username = null;
        int integrity = -1;
        for (int offset = 20; offset < packet.length; ) {
            int type = message.getShort(offset) & 0xffff, length = message.getShort(offset + 2) & 0xffff;
            if (type == 0x0006) {
                username = new String(packet, offset + 4, length, StandardCharsets.UTF_8);
            } else if (type == 0x0008) {
                integrity = offset;
            }
            offset += 4 + ((length + 3) & ~3);
        }
        assertEquals(sample.get("username").getAsString(), username);
        assertTrue(integrity > 0);
        // RFC 5389 section 15.4: the length covers MESSAGE-INTEGRITY itself, not what follows it
        byte[] covered = Arrays.copyOf(packet, integrity);
        ByteBuffer.wrap(covered).putShort(2, (short) (integrity - 20 + 24));
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(sample.get("passwordUtf8").getAsString().getBytes(StandardCharsets.UTF_8),
                "HmacSHA1"));
        String computed = HEX.formatHex(mac.doFinal(covered));
        assertEquals(sample.get("messageIntegrityHex").getAsString(), computed);
        assertEquals(computed, HEX.formatHex(packet, integrity + 4, integrity + 24));
    }
}

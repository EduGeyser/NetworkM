package io.github.sendablemetatype.netty.channel.nethernet.nxs.control;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AssistedJoinTest {
    private static final String FINGERPRINT = "sha-256 " + String.join(":", Collections.nCopies(32, "11"));

    private static String cpk(String curve) throws Exception {
        var generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec(curve));
        return Base64.getEncoder().encodeToString(generator.generateKeyPair().getPublic().getEncoded());
    }

    private static String offer(String... extra) {
        StringBuilder offer = new StringBuilder("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=setup:actpass\r\na=sctp-port:5000\r\na=max-message-size:262144\r\na=ice-ufrag:clientUfrag\r\n"
                + "a=ice-pwd:" + "c".repeat(24) + "\r\na=fingerprint:sha-256 "
                + String.join(":", Collections.nCopies(32, "AA")) + "\r\n");
        for (String line : extra) {
            offer.append(line).append("\r\n");
        }
        return offer.toString();
    }

    private static JsonObject player() throws Exception {
        JsonObject join = new JsonObject();
        join.addProperty("kind", "assisted-join");
        join.addProperty("version", 1);
        join.addProperty("id", "ab".repeat(16));
        join.addProperty("instanceId", "example-machine-1");
        join.addProperty("generation", 3);
        join.addProperty("incarnation", "0123456789abcdef".repeat(2));
        join.addProperty("keyId", "K001");
        join.addProperty("hostFingerprint", FINGERPRINT);
        join.addProperty("expiresAt", 1788484830000L);
        join.addProperty("networkId", "18446744073709551615");
        join.addProperty("cpk", cpk("secp384r1"));
        join.addProperty("localUfrag", "NXS1K001assisted");
        join.addProperty("localPassword", "h".repeat(32));
        join.addProperty("offer", offer("a=candidate:1 1 UDP 2130706431 192.0.2.10 50000 typ host",
                "a=candidate:2 1 udp 1694498815 2001:db8::7 50001 typ srflx raddr 0.0.0.0 rport 0"));
        return join;
    }

    @Test
    void aPlayerJoinDecodesWithItsOfferAndKey() throws Exception {
        JsonObject wire = player();
        wire.add("kind", wire.remove("kind")); // Members arrive in any order
        var join = AssistedJoin.decode(" \n" + wire);
        assertFalse(join.diagnostic());
        assertEquals(3, join.generation());
        assertEquals(FINGERPRINT, join.hostFingerprint().value());
        var offer = join.parseOffer();
        assertEquals("clientUfrag", offer.ufrag());
        assertEquals(List.of(new InetSocketAddress("192.0.2.10", 50000), new InetSocketAddress("2001:db8::7", 50001)),
                offer.candidates());
        assertEquals(120, join.canonicalCpk().length);
        assertEquals("AssistedJoin[id=" + "ab".repeat(16) + "]", join.toString(), "Credentials stay out of logs");
    }

    @Test
    void theFrameShapeIsEnforcedOnDecode() throws Exception {
        for (String missing : List.of("kind", "version", "id", "instanceId", "generation", "incarnation", "keyId",
                "hostFingerprint", "expiresAt", "networkId", "localUfrag", "localPassword", "offer", "cpk")) {
            JsonObject wire = player();
            wire.remove(missing);
            assertThrows(IllegalArgumentException.class, () -> AssistedJoin.decode(wire.toString()), missing);
        }
        for (var change : List.<java.util.function.Consumer<JsonObject>>of(
                w -> w.addProperty("kind", "assisted-join-result"),
                w -> w.addProperty("version", 2),
                w -> w.addProperty("id", "AB".repeat(16)),
                w -> w.addProperty("generation", 0),
                w -> w.addProperty("generation", 9007199254740992L),
                w -> w.addProperty("localUfrag", "assistedHost"),
                w -> w.addProperty("localPassword", "short"),
                w -> w.addProperty("networkId", "18446744073709551616"),
                w -> w.addProperty("networkId", "01"),
                w -> w.addProperty("cpk", "A".repeat(159)),
                w -> w.addProperty("offer", "x".repeat(AssistedJoin.MAX_OFFER_BYTES + 1)))) {
            JsonObject wire = player();
            change.accept(wire);
            assertThrows(IllegalArgumentException.class, () -> AssistedJoin.decode(wire.toString()), wire.toString());
        }
        assertThrows(IllegalArgumentException.class, () -> AssistedJoin.decode("[]"));
        assertThrows(IllegalArgumentException.class, () -> AssistedJoin.decode("x".repeat(AssistedJoin.MAX_BYTES + 1)));
    }

    @Test
    void aDiagnosticJoinCarriesNoKeyAndNamesItsAdmissionKey() throws Exception {
        JsonObject wire = player();
        wire.addProperty("networkId", "0");
        wire.remove("cpk");
        var join = AssistedJoin.decode(wire.toString());
        assertTrue(join.diagnostic());
        assertThrows(IllegalArgumentException.class, join::canonicalCpk);

        JsonObject withKey = player();
        withKey.addProperty("networkId", "0");
        assertThrows(IllegalArgumentException.class, () -> AssistedJoin.decode(withKey.toString()));
        JsonObject otherKey = player();
        otherKey.addProperty("networkId", "0");
        otherKey.remove("cpk");
        otherKey.addProperty("localUfrag", "NXS1K002assisted");
        assertThrows(IllegalArgumentException.class, () -> AssistedJoin.decode(otherKey.toString()));
    }

    @Test
    void onlyTheControlProfileOfferIsServed() throws Exception {
        String candidate = "a=candidate:1 1 UDP 2130706431 192.0.2.10 50000 typ host";
        for (String offer : List.of(
                offer(),
                offer(candidate, "a=ice-lite"),
                offer(candidate).replace("a=setup:actpass", "a=setup:active"),
                offer(candidate).replace("a=sctp-port:5000", "a=sctp-port:5001"),
                offer(candidate).replace("a=max-message-size:262144", "a=max-message-size:65536"),
                offer(candidate, "a=ice-ufrag:second"),
                offer("a=candidate:1 1 UDP 2130706431 client.example 50000 typ host"),
                offer("a=candidate:1 2 UDP 2130706431 192.0.2.10 50000 typ host"),
                offer("a=candidate:1 1 TCP 2130706431 192.0.2.10 50000 typ host"),
                offer("a=candidate:1 1 UDP 2130706431 192.0.2.10 0 typ host"),
                offer(Collections.nCopies(33, candidate).toArray(String[]::new)))) {
            JsonObject wire = player();
            wire.addProperty("offer", offer);
            var join = AssistedJoin.decode(wire.toString());
            assertThrows(IllegalArgumentException.class, join::parseOffer, offer);
        }
        JsonObject wire = player();
        wire.addProperty("offer", offer(Collections.nCopies(32, candidate).toArray(String[]::new)));
        assertEquals(32, AssistedJoin.decode(wire.toString()).parseOffer().candidates().size());
    }

    @Test
    void onlyACanonicalP384KeyBindsAPlayer() throws Exception {
        byte[] key = Base64.getDecoder().decode(cpk("secp384r1"));
        // A different structure prefix, and a point that is not on the curve, both of the right length
        for (int index : new int[]{0, key.length - 1}) {
            byte[] changed = key.clone();
            changed[index] ^= 1;
            JsonObject wire = player();
            wire.addProperty("cpk", Base64.getEncoder().encodeToString(changed));
            var join = AssistedJoin.decode(wire.toString());
            assertThrows(IllegalArgumentException.class, join::canonicalCpk, "byte " + index);
        }
    }
}

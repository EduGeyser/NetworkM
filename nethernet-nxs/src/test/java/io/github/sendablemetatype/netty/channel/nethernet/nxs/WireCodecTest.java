package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {
    private static HostCandidate candidate(String address, CandidateType type) {
        return new HostCandidate(new InetSocketAddress(address, 19132), type);
    }

    @Test
    void candidatePrioritiesRankByTypeThenByOrderWithinTheType() {
        var encoded = WireCodec.candidates(List.of(
                candidate("10.0.0.2", CandidateType.HOST),
                candidate("203.0.113.7", CandidateType.SERVER_REFLEXIVE),
                candidate("10.0.0.3", CandidateType.HOST),
                candidate("198.51.100.9", CandidateType.RELAY)));
        // RFC 8445 section 5.1.2.1 with component 1: the first host candidate is 2130706431
        assertEquals(2130706431, encoded.get(0).getAsJsonObject().get("priority").getAsInt());
        assertEquals((100 << 24) | (65535 << 8) | 255, encoded.get(1).getAsJsonObject().get("priority").getAsInt());
        assertEquals((126 << 24) | (65534 << 8) | 255, encoded.get(2).getAsJsonObject().get("priority").getAsInt());
        assertEquals((65535 << 8) | 255, encoded.get(3).getAsJsonObject().get("priority").getAsInt());
        long previous = Long.MAX_VALUE;
        for (int index : new int[]{0, 2, 1, 3}) {
            long priority = encoded.get(index).getAsJsonObject().get("priority").getAsLong();
            assertTrue(priority < previous, "Every host beats every reflexive candidate, which beats every relay");
            previous = priority;
        }
        assertEquals(List.of("1", "2", "3", "4"), encoded.asList().stream()
                .map(c -> c.getAsJsonObject().get("foundation").getAsString()).toList());
        assertEquals("srflx", encoded.get(1).getAsJsonObject().get("type").getAsString());
    }

    private static JsonObject reply() {
        return JsonParser.parseString("""
                {"hostProfileRevision":"example-profile-1","readiness":{"routable":true,"reasons":[]},
                 "admissionKeyRetirements":[{"keyId":"T001","retireAfter":1700000000000}],"serverTime":1700000000000}
                """).getAsJsonObject();
    }

    @Test
    void aHeartbeatReplyMustCarryItsRequiredMembers() throws Exception {
        var parsed = WireCodec.heartbeatReply(reply());
        assertEquals("example-profile-1", parsed.hostProfileRevision());
        assertTrue(parsed.readiness().routable());
        assertEquals(List.of(new Wire.KeyRetirement("T001", 1700000000000L)), parsed.retirements());

        var unpublished = reply();
        unpublished.add("hostProfileRevision", com.google.gson.JsonNull.INSTANCE);
        assertNull(WireCodec.heartbeatReply(unpublished).hostProfileRevision(), "Null means nothing is published");

        for (String member : List.of("hostProfileRevision", "readiness", "admissionKeyRetirements")) {
            var incomplete = reply();
            incomplete.remove(member);
            assertThrows(IOException.class, () -> WireCodec.heartbeatReply(incomplete), member);
        }
        var noRoutable = reply();
        noRoutable.getAsJsonObject("readiness").remove("routable");
        assertThrows(IOException.class, () -> WireCodec.heartbeatReply(noRoutable));
        var textRoutable = reply();
        textRoutable.getAsJsonObject("readiness").addProperty("routable", "true");
        assertThrows(IOException.class, () -> WireCodec.heartbeatReply(textRoutable));
        var noReasons = reply();
        noReasons.getAsJsonObject("readiness").remove("reasons");
        assertThrows(IOException.class, () -> WireCodec.heartbeatReply(noReasons));
        var badRetirement = reply();
        badRetirement.getAsJsonArray("admissionKeyRetirements").get(0).getAsJsonObject().addProperty("keyId", "t1");
        assertThrows(IOException.class, () -> WireCodec.heartbeatReply(badRetirement));
    }

    @Test
    void readinessKeepsOnlyWellFormedReasons() throws Exception {
        var reply = reply();
        var reasons = reply.getAsJsonObject("readiness").getAsJsonArray("reasons");
        reasons.add("key_not_acknowledged");
        reasons.add("Not A Code");
        reasons.add(7);
        reasons.add("key_not_acknowledged");
        for (int i = 0; i < 40; i++) {
            reasons.add("reason_" + i);
        }
        var readiness = WireCodec.heartbeatReply(reply).readiness();
        assertEquals("key_not_acknowledged", readiness.reasons().getFirst());
        assertEquals(32, readiness.reasons().size());
        assertTrue(readiness.reasons().stream().allMatch(r -> r.matches("[a-z][a-z0-9_]*")));
    }

    @Test
    void errorBodiesAreReadMemberByMember() {
        var full = WireCodec.error("""
                {"code":"capacity_reached","message":"Add a token.","actionUrl":"https://provider.example/tokens"}
                """);
        assertEquals(new Wire.ErrorReply("capacity_reached", "Add a token.",
                URI.create("https://provider.example/tokens")), full);
        assertEquals(new Wire.ErrorReply("request_rejected", null, null), WireCodec.error("not json"));
        assertEquals(new Wire.ErrorReply("request_rejected", null, null), WireCodec.error("[]"));
        var invalid = WireCodec.error("""
                {"code":"Capacity Reached","message":"line\\nbreak","actionUrl":"http://provider.example/tokens"}
                """);
        assertEquals(new Wire.ErrorReply("request_rejected", null, null), invalid,
                "Each invalid member is dropped alone");
        assertNull(WireCodec.error("{\"actionUrl\":\"https://user@provider.example/\"}").actionUrl());
        assertEquals(URI.create("http://127.0.0.1:8080/claim"),
                WireCodec.error("{\"actionUrl\":\"http://127.0.0.1:8080/claim\"}").actionUrl());
        assertNull(WireCodec.error("{\"message\":\"" + "x".repeat(257) + "\"}").message());
    }
}

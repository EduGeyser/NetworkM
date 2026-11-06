package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CandidateLinesTest {

    @Test
    void aPeerReflexiveClientKeepsItsRealAddress() {
        TransportPath path = CandidateLines.path(
                "candidate:1 1 udp 2122260223 192.168.1.10 19133 typ host generation 0",
                "candidate:2 1 udp 1686052607 203.0.113.7 61022 typ prflx raddr 0.0.0.0 rport 0 generation 0");

        assertEquals(new TransportPath(new InetSocketAddress("192.168.1.10", 19133), CandidateType.HOST,
                new InetSocketAddress("203.0.113.7", 61022), CandidateType.PEER_REFLEXIVE), path);
    }

    @Test
    void aRelayedClientIsTheRelayNotItsDeclaredAddress() {
        TransportPath path = CandidateLines.path(
                "candidate:1 1 udp 2122260223 2001:db8::1 19133 typ host",
                "a=candidate:3 1 udp 41885439 198.51.100.4 3478 typ relay raddr 10.0.0.5 rport 50000");

        assertEquals(new InetSocketAddress("2001:db8::1", 19133), path.local());
        assertEquals(new InetSocketAddress("198.51.100.4", 3478), path.remote());
        assertEquals(CandidateType.RELAY, path.remoteType());
        assertTrue(path.relayed());
    }

    @Test
    void addressesThatAreNotKnownBecomeNull() {
        TransportPath path = CandidateLines.path(
                "candidate:1 1 udp 1 0.0.0.0 0 typ srflx",
                "candidate:2 1 udp 1686052607 redacted-ip.invalid 61022 typ prflx");

        assertEquals(new TransportPath(null, CandidateType.SERVER_REFLEXIVE, null, CandidateType.PEER_REFLEXIVE), path);
        assertEquals(new TransportPath(null, CandidateType.UNKNOWN, null, CandidateType.UNKNOWN),
                CandidateLines.path("", "candidate:2 1 udp 1 192.0.2.1 x typ local"));
    }
}

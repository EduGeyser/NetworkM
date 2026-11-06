package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssistedAnswerTest {
    @Test
    void theAnswerNamesOnlyTheAddressOfItsOneCandidate() throws Exception {
        // As libwebrtc describes a peer that gathered on a private interface, m= and c= set from its candidates
        String local = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 50000 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 192.168.1.20\r\n"
                + "a=ice-ufrag:NXS1K001assisted\r\na=ice-pwd:assistedpassword0123456789\r\na=mid:0\r\n"
                + "a=candidate:1 1 udp 2122260223 192.168.1.20 50000 typ host generation 0\r\n"
                + "a=candidate:2 1 udp 1686052607 203.0.113.9 61000 typ srflx raddr 192.168.1.20 rport 50000\r\n"
                + "a=end-of-candidates\r\n";

        String answer = AssistedAnswer.build(local, new HostCandidate(new InetSocketAddress("93.184.216.40", 40404),
                CandidateType.SERVER_REFLEXIVE), null).sdp();

        assertTrue(answer.contains("\r\nm=application 40404 UDP/DTLS/SCTP webrtc-datachannel\r\n"), answer);
        assertTrue(answer.contains("\r\nc=IN IP4 93.184.216.40\r\n"), answer);
        assertTrue(answer.contains("\r\na=candidate:1 1 UDP 2130706431 93.184.216.40 40404 typ srflx\r\n"), answer);
        assertFalse(answer.contains("192.168.1.20") || answer.contains("50000"),
                "nothing of the discarded candidates stays in the answer: " + answer);
    }
}

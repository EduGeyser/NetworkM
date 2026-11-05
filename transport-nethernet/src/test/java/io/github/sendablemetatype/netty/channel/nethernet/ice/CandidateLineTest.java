package io.github.sendablemetatype.netty.channel.nethernet.ice;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CandidateLineTest {

    @Test
    void readsEveryFormACandidateTravelsIn() {
        for (String line : List.of("candidate:1 1 udp 2130706431 203.0.113.10 19191 typ host",
                "a=candidate:1 1 udp 2130706431 203.0.113.10 19191 typ host\r\n",
                " candidate:1 1 udp 2130706431 203.0.113.10 19191 typ host ")) {
            CandidateLine candidate = CandidateLine.parse(line).orElseThrow();
            assertEquals("candidate:1 1 udp 2130706431 203.0.113.10 19191 typ host", candidate.sdp(), line);
            assertEquals("a=candidate:1 1 udp 2130706431 203.0.113.10 19191 typ host", candidate.attribute());
            assertEquals(Optional.of(new InetSocketAddress("203.0.113.10", 19191)), candidate.endpoint());
            assertEquals(CandidateType.HOST, candidate.type());
            assertTrue(candidate.isUdp());
        }
    }

    @Test
    void refusesWhatIsNoCandidate() {
        for (String line : List.of("a=end-of-candidates", "candidate:1 1 udp", "candidate:1 1 udp 1 203.0.113.10 5 typ",
                "candidate:1 1 udp 1 203.0.113.10 5 not-typ host", "candidate:1 1 udp 1 203.0.113.10 70000 typ host",
                "candidate:1 x udp 1 203.0.113.10 5 typ host", "candidate:1 1 udp 1 203.0.113.10 5 typ bogus")) {
            assertTrue(CandidateLine.parse(line).isEmpty(), line);
        }
    }

    @Test
    void readsANameWithoutLookingItUp() {
        CandidateLine mdns = CandidateLine.parse("candidate:1 1 udp 1 a1b2c3d4.local 5000 typ host").orElseThrow();
        assertEquals("a1b2c3d4.local", mdns.address());
        assertTrue(mdns.ipAddress().isEmpty());
        assertTrue(mdns.endpoint().isEmpty());
    }

    @Test
    void hidesTheRelatedAddressInItsOwnFamily() {
        assertEquals("candidate:1 1 udp 1 198.51.100.9 40000 typ relay raddr 0.0.0.0 rport 0 generation 0",
                CandidateLine.parse("candidate:1 1 udp 1 198.51.100.9 40000 typ relay raddr 203.0.113.50 rport 19135"
                        + " generation 0").orElseThrow().withRelatedAddressHidden().sdp());
        assertEquals("candidate:1 1 udp 1 2001:db8::9 40000 typ relay raddr :: rport 0",
                CandidateLine.parse("candidate:1 1 udp 1 2001:db8::9 40000 typ relay raddr 2001:db8::50 rport 19135")
                        .orElseThrow().withRelatedAddressHidden().sdp());
        CandidateLine host = CandidateLine.parse("candidate:1 1 udp 1 10.0.0.1 5 typ host generation 0").orElseThrow();
        assertSame(host, host.withRelatedAddressHidden());
        assertFalse(host.withRelatedAddressHidden().sdp().contains("raddr"));
    }
}

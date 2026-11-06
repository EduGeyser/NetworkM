package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HostCandidateTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress("203.0.113.7", 19132);

    @Test
    void publishesOnlyTheTypesAHostProfileHas() {
        assertEquals("host", HostCandidate.host(ENDPOINT).typeName());
        assertEquals("srflx", new HostCandidate(ENDPOINT, CandidateType.SERVER_REFLEXIVE).typeName());
        assertEquals("relay", new HostCandidate(ENDPOINT, CandidateType.RELAY).typeName());
        assertThrows(IllegalArgumentException.class, () -> new HostCandidate(ENDPOINT, CandidateType.PEER_REFLEXIVE));
        assertThrows(IllegalArgumentException.class, () -> new HostCandidate(ENDPOINT, CandidateType.UNKNOWN));
    }
}

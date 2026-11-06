package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertThrows;

class HostCandidateTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress("203.0.113.7", 19132);

    @Test
    void publishesNoTypeThatOnlyAConnectivityCheckOrTheEngineKnows() {
        assertThrows(IllegalArgumentException.class, () -> new HostCandidate(ENDPOINT, CandidateType.PEER_REFLEXIVE));
        assertThrows(IllegalArgumentException.class, () -> new HostCandidate(ENDPOINT, CandidateType.UNKNOWN));
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CandidateAnnouncementTest {
    private static final List<InetSocketAddress> ADVERTISED = List.of(EndpointAddress.parseEndpoint("203.0.113.99"));
    private static final List<InetSocketAddress> DISCOVERED =
            List.of(EndpointAddress.parseEndpoint("198.51.100.4:40404"));
    private static final Optional<InetSocketAddress> TYPED = Optional.of(EndpointAddress.parseEndpoint("192.0.0.9:19132"));

    @Test
    void configuredAddressesWinAndBecomeRelaysBehindProxyProtocol() {
        assertEquals(new CandidateAnnouncement.Advertised(ADVERTISED),
                CandidateAnnouncement.of(ADVERTISED, false, DISCOVERED, TYPED));
        assertEquals(new CandidateAnnouncement.Relayed(ADVERTISED),
                CandidateAnnouncement.of(ADVERTISED, true, DISCOVERED, TYPED));
    }

    @Test
    void otherwiseAddsTheMappingsAndTheTypedEndpoint() {
        assertEquals(new CandidateAnnouncement.Added(List.of(DISCOVERED.getFirst(), TYPED.orElseThrow())),
                CandidateAnnouncement.of(List.of(), false, DISCOVERED, TYPED));
        assertEquals(new CandidateAnnouncement.Added(DISCOVERED),
                CandidateAnnouncement.of(List.of(), true, DISCOVERED, TYPED), "players reach the media through relays");
        assertSame(CandidateAnnouncement.AS_GATHERED, CandidateAnnouncement.of(List.of(), false, List.of(), Optional.empty()));
    }

    @Test
    void theTypedEndpointTakesTheIcePortUnlessTheyShareOne() {
        assertEquals(Optional.of(new InetSocketAddress("8.8.8.8", 19133)),
                CandidateAnnouncement.typedEndpoint("8.8.8.8:25000", false, 19133, false));
        assertEquals(Optional.of(new InetSocketAddress("8.8.8.8", 25000)),
                CandidateAnnouncement.typedEndpoint("8.8.8.8:25000", false, 19133, true),
                "one forward carries both");
        assertEquals(Optional.of(new InetSocketAddress("8.8.8.8", 19133)),
                CandidateAnnouncement.typedEndpoint("8.8.8.8", false, 19133, true));
        assertEquals(Optional.of(new InetSocketAddress("2606:4700::1", 0)),
                CandidateAnnouncement.typedEndpoint("[2606:4700::1]:19132", false, 0, false),
                "without a fixed ICE port each host candidate keeps its own");
    }

    @Test
    void theTypedEndpointIsOnlyAReachableLiteralFromThePlayer() {
        assertTrue(CandidateAnnouncement.typedEndpoint("8.8.8.8", true, 19133, false).isEmpty(),
                "a proxy's Host names the proxy");
        for (String host : List.of("play.example.test", "play.example.test:19132", "127.0.0.1:19132",
                "0.0.0.0", "203.0.113.5", "[fe80::1]:19132")) {
            assertTrue(CandidateAnnouncement.typedEndpoint(host, false, 19133, false).isEmpty(), host);
        }
        assertTrue(CandidateAnnouncement.typedEndpoint(null, false, 19133, false).isEmpty());
        assertTrue(CandidateAnnouncement.typedEndpoint("192.168.1.10", false, 19133, false).isPresent(),
                "a LAN player typed a LAN address");
    }
}

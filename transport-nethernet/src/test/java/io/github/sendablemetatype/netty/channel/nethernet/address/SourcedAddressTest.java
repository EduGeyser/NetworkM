package io.github.sendablemetatype.netty.channel.nethernet.address;

import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateType;
import io.github.sendablemetatype.netty.util.address.IpRangeSet;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SourcedAddressTest {
    private static final InetSocketAddress PROXY = new InetSocketAddress("10.0.0.2", 40000);
    private static final InetSocketAddress CLIENT = new InetSocketAddress("203.0.113.7", 54321);
    private static final TrustedProxies BOTH_ROLES = new TrustedProxies(IpRangeSet.parse(List.of("10.0.0.0/8")),
            IpRangeSet.parse(List.of("10.0.0.0/8")));

    @Test
    void aProxyProtocolHeaderWinsOverForwardedFor() {
        assertEquals(new SourcedAddress(CLIENT, AddressSource.SIGNALING_PROXY_PROTOCOL, PROXY),
                SourcedAddress.signaling(PROXY, CLIENT, List.of("198.51.100.2"), BOTH_ROLES));
    }

    @Test
    void forwardedForNamesTheClientAndTheForwarder() {
        assertEquals(new SourcedAddress(new InetSocketAddress("198.51.100.2", PROXY.getPort()),
                        AddressSource.SIGNALING_FORWARDED_FOR, PROXY),
                SourcedAddress.signaling(PROXY, null, List.of("198.51.100.2"), BOTH_ROLES));
    }

    @Test
    void anUntrustedPeerSpeaksForItselfOnly() {
        InetSocketAddress stranger = new InetSocketAddress("198.51.100.66", 1234);
        assertEquals(new SourcedAddress(stranger, AddressSource.SIGNALING, null),
                SourcedAddress.signaling(stranger, CLIENT, List.of("198.51.100.2"), BOTH_ROLES));
        assertEquals(new SourcedAddress(PROXY, AddressSource.SIGNALING, null),
                SourcedAddress.signaling(PROXY, null, List.of(), BOTH_ROLES));
    }

    @Test
    void classifiesTheSelectedPair() {
        InetSocketAddress relay = new InetSocketAddress("198.51.100.1", 19132);
        assertEquals(new SourcedAddress(CLIENT, AddressSource.ICE, null),
                SourcedAddress.selectedPair(CLIENT, CandidateType.PEER_REFLEXIVE, false, null));
        assertEquals(new SourcedAddress(CLIENT, AddressSource.ICE_PROXY_PROTOCOL, relay),
                SourcedAddress.selectedPair(CLIENT, CandidateType.SERVER_REFLEXIVE, false, relay));
        assertEquals(AddressSource.TURN, SourcedAddress.selectedPair(CLIENT, CandidateType.RELAY, false, null).source());
        // Through a PROXY protocol relay a TURN relay shows as peer reflexive; only the peer's signal tells
        assertEquals(new SourcedAddress(CLIENT, AddressSource.TURN, relay),
                SourcedAddress.selectedPair(CLIENT, CandidateType.PEER_REFLEXIVE, true, relay));
    }

    @Test
    void neverHoldsANullOrUnresolvedAddress() {
        assertThrows(NullPointerException.class, () -> new SourcedAddress(null, AddressSource.ICE, null));
        assertThrows(IllegalArgumentException.class,
                () -> new SourcedAddress(InetSocketAddress.createUnresolved("player.example", 1), AddressSource.ICE, null));
    }
}

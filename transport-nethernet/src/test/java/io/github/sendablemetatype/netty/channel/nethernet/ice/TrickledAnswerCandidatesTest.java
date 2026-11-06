package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TrickledAnswerCandidatesTest {
    private static final String DOCKER = "candidate:1 1 udp 2122260223 172.17.0.2 19135 typ host generation 0";
    private static final String LAN = "candidate:2 1 udp 2122194687 10.88.0.4 19135 typ host generation 0";
    private static final String LAN_V6 = "candidate:3 1 udp 2122129151 fd7a::1 19135 typ host generation 0";
    private static final String SRFLX =
            "candidate:4 1 udp 1686052607 203.0.113.50 19135 typ srflx raddr 10.88.0.4 rport 19135 generation 0";
    private static final String RELAY =
            "candidate:5 1 udp 41885439 198.51.100.9 40000 typ relay raddr 203.0.113.50 rport 19135 generation 0";

    private static TrickledAnswerCandidates advertised(String... endpoints) {
        return AnswerCandidates.trickled(new CandidateAnnouncement.Advertised(
                Stream.of(endpoints).map(EndpointAddress::parseEndpoint).toList()));
    }

    /** Everything one session signals, in order, for the candidates the engine reports. */
    private static List<String> signaled(TrickledAnswerCandidates filter, String... candidates) {
        List<String> out = new ArrayList<>();
        for (String candidate : candidates) {
            out.addAll(filter.local(candidate));
        }
        out.addAll(filter.gatheringComplete());
        return out;
    }

    @Test
    void passesEverythingWhenNothingIsAnnounced() {
        assertEquals(List.of(DOCKER, LAN, SRFLX),
                signaled(AnswerCandidates.trickled(CandidateAnnouncement.AS_GATHERED), DOCKER, LAN, SRFLX));
    }

    @Test
    void sendsAddedEndpointsAsExtraCandidatesBasedOnTheFirstHostOfTheirFamily() {
        TrickledAnswerCandidates filter = AnswerCandidates.trickled(new CandidateAnnouncement.Added(
                List.of(EndpointAddress.parseEndpoint("198.51.100.77"), EndpointAddress.parseEndpoint("[2001:db8::77]"))));

        assertEquals(List.of(DOCKER,
                        "candidate:80000000 1 udp 1694498815 198.51.100.77 19135 typ srflx raddr 172.17.0.2 rport 19135",
                        LAN, LAN_V6,
                        "candidate:80000001 1 udp 1694498559 2001:db8::77 19135 typ srflx raddr fd7a::1 rport 19135"),
                signaled(filter, DOCKER, LAN, LAN_V6));
    }

    @Test
    void doesNotRepeatAnAddedEndpointAlreadyGathered() {
        TrickledAnswerCandidates filter = AnswerCandidates.trickled(new CandidateAnnouncement.Added(
                List.of(EndpointAddress.parseEndpoint("10.88.0.4:19135"))));

        assertEquals(List.of(LAN, DOCKER), signaled(filter, LAN, DOCKER));
    }

    @Test
    void narrowsToAHeldAddressOnceGatheringTellsWhichAreHeld() {
        TrickledAnswerCandidates filter = advertised("10.88.0.4", "203.0.113.99");

        assertEquals(List.of(), filter.local(DOCKER), "held until the narrowing is known");
        assertEquals(List.of(), filter.local(LAN));
        // The first reflexive candidate settles it: the translation is based on the held host
        assertEquals(List.of(LAN,
                        "candidate:80000000 1 udp 1694498815 203.0.113.99 19135 typ srflx raddr 10.88.0.4 rport 19135",
                        SRFLX),
                filter.local(SRFLX));
        assertEquals(List.of(RELAY), filter.local(RELAY));
        assertEquals(List.of(), filter.local("candidate:9 1 udp 2122260223 192.168.5.5 19135 typ host"),
                "a host candidate after the decision is narrowed away too");
        assertEquals(List.of(), filter.gatheringComplete());
    }

    @Test
    void releasesHostsAtTheEndOfGatheringWhenNothingIsHeld() {
        // The usual NAT case: the list names only the public address, so every host stays
        TrickledAnswerCandidates filter = advertised("203.0.113.99");

        assertEquals(List.of(DOCKER, LAN,
                        "candidate:80000000 1 udp 1694498815 203.0.113.99 19135 typ srflx raddr 172.17.0.2 rport 19135"),
                signaled(filter, DOCKER, LAN));
    }

    @Test
    void anAdvertisedListThatMatchesNothingNarrowsNothing() {
        assertEquals(List.of(DOCKER, LAN), signaled(advertised("2001:db8::99:5678"), DOCKER, LAN));
    }

    @Test
    void aLateHostOfANewFamilyStillGetsItsTranslation() {
        TrickledAnswerCandidates filter = advertised("10.88.0.4", "[2001:db8::99]");
        filter.local(LAN);
        filter.local(SRFLX);

        // The list narrowed and names no IPv6 host, so the host goes, but it still bases the translation
        assertEquals(List.of("candidate:80000000 1 udp 1694498815 2001:db8::99 19135 typ srflx raddr fd7a::1 rport 19135"),
                filter.local(LAN_V6));
    }

    @Test
    void behindRelaysSendsOnlyTheRelaysAndTurnWithoutItsOrigin() {
        TrickledAnswerCandidates filter = AnswerCandidates.trickled(new CandidateAnnouncement.Relayed(
                List.of(EndpointAddress.parseEndpoint("198.51.100.200:19132"))));

        assertEquals(List.of(
                        "candidate:80000000 1 udp 1694498815 198.51.100.200 19132 typ srflx raddr 0.0.0.0 rport 0",
                        "candidate:5 1 udp 41885439 198.51.100.9 40000 typ relay raddr 0.0.0.0 rport 0 generation 0"),
                signaled(filter, DOCKER, LAN, SRFLX, RELAY));
    }

    @Test
    void passesALineItCannotReadAsItIs() {
        assertEquals(List.of("candidate:garbled"), signaled(advertised("203.0.113.99"), "candidate:garbled"));
    }
}

/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnswerCandidatesTest {

    private static final String SDP = "v=0\r\n"
            + "o=- 1 2 IN IP4 127.0.0.1\r\n"
            + "a=candidate:1 1 udp 2130706431 203.0.113.10 19191 typ host\r\n"
            + "a=candidate:2 1 udp 2130706431 172.17.0.2 19191 typ host\r\n"
            + "a=candidate:3 1 udp 2130706431 10.88.0.4 19191 typ host\r\n"
            + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n";

    private static final String GATHERED = "v=0\r\n"
            + "a=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n"
            + "a=candidate:2 1 udp 2130706431 172.17.0.2 19135 typ host\r\n"
            + "a=candidate:3 1 udp 2130706431 fd7a::1 19135 typ host\r\n"
            + "a=candidate:4 1 tcp 2130706431 10.88.0.4 9 typ host tcptype active\r\n"
            + "a=end-of-candidates\r\n"
            + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n";

    private static final String TRANSLATED =
            "a=candidate:80000000 1 udp 1694498815 203.0.113.99 19135 typ srflx raddr 10.88.0.4 rport 19135\r\n";

    /** An answer as libwebrtc writes one after gathering: candidates right after m= and c=. */
    private static final String ANSWER = "v=0\r\n"
            + "o=- 5 2 IN IP4 127.0.0.1\r\n"
            + "s=-\r\n"
            + "t=0 0\r\n"
            + "a=group:BUNDLE 0\r\n"
            + "m=application 19135 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "c=IN IP4 203.0.113.50\r\n"
            + "a=candidate:1 1 udp 2122260223 192.168.1.5 19135 typ host generation 0 network-id 1\r\n"
            + "a=candidate:2 1 udp 1686052607 203.0.113.50 19135 typ srflx raddr 192.168.1.5 rport 19135"
            + " generation 0 network-id 1\r\n"
            + "a=candidate:3 1 udp 41885439 198.51.100.9 40000 typ relay raddr 203.0.113.50 rport 19135"
            + " generation 0 network-id 1\r\n"
            + "a=ice-ufrag:abcd\r\n"
            + "a=ice-pwd:0123456789abcdefghijklmn\r\n"
            + "a=mid:0\r\n"
            + "a=sctp-port:5000\r\n";

    private static final String ANSWER_WITHOUT_TURN = ANSWER.replaceAll("a=candidate:3 [^\\r]*\\r\\n", "");

    private static List<InetSocketAddress> endpoints(String... values) {
        return Stream.of(values).map(EndpointAddress::parseEndpoint).toList();
    }

    private static String advertised(String sdp, String... endpoints) {
        return AnswerCandidates.fullAnswer(sdp, new CandidateAnnouncement.Advertised(endpoints(endpoints)));
    }

    private static String added(String sdp, String... endpoints) {
        return AnswerCandidates.fullAnswer(sdp, new CandidateAnnouncement.Added(endpoints(endpoints)));
    }

    private static String relayed(String sdp, String... endpoints) {
        return AnswerCandidates.fullAnswer(sdp, new CandidateAnnouncement.Relayed(endpoints(endpoints)));
    }

    private static long count(String sdp, String part) {
        return sdp.lines().filter(line -> line.contains(part)).count();
    }

    // Advertised addresses, ported from upstream's SdpUtilTest

    @Test
    void keepsOnlyTheAdvertisedCandidates() {
        String filtered = advertised(SDP, "203.0.113.10");

        assertTrue(filtered.contains("203.0.113.10"));
        assertFalse(filtered.contains("172.17.0.2"));
        assertFalse(filtered.contains("10.88.0.4"));
        // Everything that is not a candidate survives
        assertTrue(filtered.contains("m=application"));
        assertTrue(filtered.contains("o=- 1 2 IN IP4 127.0.0.1"));
    }

    @Test
    void matchesTheSameAddressWrittenTwoWays() {
        String sdp = "v=0\r\n"
                + "a=candidate:1 1 udp 2130706431 2001:db8::1 5000 typ host\r\n"
                + "a=candidate:2 1 udp 2130706431 2001:db8::2 5000 typ host\r\n";

        String filtered = advertised(sdp, "2001:0db8:0000:0000:0000:0000:0000:0001");

        assertTrue(filtered.contains("2001:db8::1"));
        assertFalse(filtered.contains("2001:db8::2"));
    }

    @Test
    void leavesMdnsCandidatesToTheFilterRatherThanResolvingThem() {
        String sdp = "v=0\r\n"
                + "a=candidate:1 1 udp 2130706431 a1b2c3d4.local 5000 typ host\r\n"
                + "a=candidate:2 1 udp 2130706431 203.0.113.10 5000 typ host\r\n";

        String filtered = advertised(sdp, "203.0.113.10");

        assertTrue(filtered.contains("203.0.113.10"));
        assertFalse(filtered.contains("a1b2c3d4.local"));
    }

    @Test
    void announcesEverythingWhenNothingIsConfigured() {
        assertEquals(SDP, AnswerCandidates.fullAnswer(SDP, CandidateAnnouncement.AS_GATHERED));
        assertEquals(SDP, advertised(SDP));
    }

    @Test
    void keepsEveryCandidateWhenNothingCouldBeAnnounced() {
        // Nothing gathered matches, and nothing of that family exists to translate from
        assertEquals(SDP, advertised(SDP, "2001:db8::1"));
    }

    @Test
    void dropsACandidateItCannotReadAnAddressOutOf() {
        String sdp = "v=0\r\n"
                + "a=candidate:1 1 udp 2130706431\r\n"
                + "a=candidate:2 1 udp 2130706431 203.0.113.10 19191 typ host\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n";

        String filtered = advertised(sdp, "203.0.113.10");

        assertFalse(filtered.contains("a=candidate:1 "), "a candidate with no address cannot be advertised");
        assertTrue(filtered.contains("a=candidate:2 "));
    }

    @Test
    void neverLeavesABlankLine() {
        // libwebrtc rejects a description with an empty line anywhere, the end included
        assertFalse(advertised(SDP + "\r\n", "203.0.113.10").endsWith("\r\n\r\n"));
        String filtered = advertised(SDP.replace("a=candidate:2", "\r\na=candidate:2"), "203.0.113.10");
        assertFalse(filtered.contains("\r\n\r\n"));
        assertTrue(filtered.contains("a=candidate:1 "), "and the candidates around it still stand");
    }

    @Test
    void announcesAnAddressThisHostDoesNotHoldOnTheMediaPort() {
        // Everything gathered stays, since nothing listed is ours to narrow to. One line for the
        // address and port whatever the number of interfaces, and never for another family or transport
        String expected = GATHERED.replace("a=end-of-candidates", TRANSLATED + "a=end-of-candidates");
        assertEquals(expected, advertised(GATHERED, "203.0.113.99"));
    }

    @Test
    void narrowsToTheHeldAddressesAndTranslatesTheRest() {
        String result = advertised(GATHERED, "10.88.0.4", "203.0.113.99");
        assertTrue(result.contains("a=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n"));
        assertTrue(result.contains(TRANSLATED));
        assertFalse(result.contains("172.17.0.2"));
    }

    @Test
    void aTranslationIsBasedOnAHeldHostWhenThereIsOne() {
        String result = advertised(GATHERED, "172.17.0.2", "203.0.113.99");
        assertTrue(result.contains(" 203.0.113.99 19135 typ srflx raddr 172.17.0.2 rport 19135\r\n"));
        assertFalse(result.contains("10.88.0.4"));
    }

    @Test
    void translatesOnlyWithinTheFamily() {
        String result = advertised(GATHERED, "2001:db8::99");
        assertTrue(result.contains(" 2001:db8::99 19135 typ srflx raddr fd7a::1 rport 19135\r\n"));
        assertFalse(result.contains("raddr 10.88.0.4"));
        assertTrue(result.contains("a=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n"));
    }

    @Test
    void givesEachTranslationItsOwnPriority() {
        String result = advertised(GATHERED, "203.0.113.99", "198.51.100.7");

        assertEquals(2, result.lines().filter(line -> line.contains(" typ srflx "))
                .map(line -> line.split(" ")[3]).distinct().count());
    }

    @Test
    void announcesTheExternalPortAndKeepsTheLocalRelatedPort() {
        String translated = "a=candidate:80000000 1 udp 1694498815 203.0.113.99 56789 "
                + "typ srflx raddr 10.88.0.4 rport 19135\r\n";
        String expected = GATHERED.replace("a=end-of-candidates", translated + "a=end-of-candidates");

        assertEquals(expected, advertised(GATHERED, "203.0.113.99:56789"));
    }

    @Test
    void announcesMultipleExternalPortsOnTheSameAddress() {
        String result = advertised(GATHERED, "203.0.113.99:56789", "203.0.113.99:46565");

        assertTrue(result.contains(" 203.0.113.99 56789 typ srflx raddr 10.88.0.4 rport 19135\r\n"));
        assertTrue(result.contains(" 203.0.113.99 46565 typ srflx raddr 10.88.0.4 rport 19135\r\n"));
        assertEquals(2, count(result, " typ srflx "));
    }

    @Test
    void translatesIpv6EndpointsOnlyFromIpv6HostsAndDeduplicatesEquivalentAddresses() {
        String result = advertised(GATHERED, "[2001:db8::99]:56789", "[2001:0db8:0:0:0:0:0:99]:56789");

        assertTrue(result.contains(" 2001:db8::99 56789 typ srflx raddr fd7a::1 rport 19135\r\n"));
        assertEquals(1, count(result, " typ srflx "));
    }

    @Test
    void translatesADifferentPortEvenWhenTheAddressWasGathered() {
        String result = advertised(GATHERED, "10.88.0.4:56789");

        assertTrue(result.contains(" 10.88.0.4 56789 typ srflx raddr 10.88.0.4 rport 19135\r\n"));
        assertTrue(result.contains("a=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n"));
        assertTrue(result.contains("a=candidate:2 1 udp 2130706431 172.17.0.2 19135 typ host\r\n"));
    }

    @Test
    void anExplicitPortSelectsOnlyTheMatchingGatheredUdpEndpoint() {
        String otherPort = "a=candidate:5 1 udp 2130706431 10.88.0.4 19136 typ host\r\n";
        String gathered = GATHERED.replace("a=end-of-candidates", otherPort + "a=end-of-candidates");
        String result = advertised(gathered, "10.88.0.4:19135");

        assertTrue(result.contains("a=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n"));
        assertFalse(result.contains(otherPort));
        assertFalse(result.contains("172.17.0.2"));
        assertFalse(result.contains("fd7a::1"));
        assertFalse(result.contains(" tcp "));
        assertFalse(result.contains(" typ srflx "));
    }

    @Test
    void doesNotMistakeATcpCandidateForAnAlreadyGatheredUdpEndpoint() {
        String result = advertised(GATHERED, "10.88.0.4:9");

        assertTrue(result.contains(" 10.88.0.4 9 typ srflx raddr 10.88.0.4 rport 19135\r\n"));
        assertTrue(result.contains("a=candidate:2 1 udp 2130706431 172.17.0.2 19135 typ host\r\n"));
    }

    @Test
    void deduplicatesBareAddressesAndEndpointsThatAnnounceTheSameCandidate() {
        assertEquals(GATHERED.replace("a=end-of-candidates", TRANSLATED + "a=end-of-candidates"),
                advertised(GATHERED, "203.0.113.99", "203.0.113.99:19135"));
    }

    @Test
    void leavesTheDescriptionAloneWithoutAUdpHostOfTheSameFamily() {
        String ipv6Only = "v=0\r\na=candidate:1 1 udp 2130706431 fd7a::1 19135 typ host\r\n";
        String tcpOnly = "v=0\r\na=candidate:1 1 tcp 2130706431 10.88.0.4 9 typ host tcptype active\r\n";

        assertEquals(ipv6Only, advertised(ipv6Only, "203.0.113.99:56789"));
        assertEquals(tcpOnly, advertised(tcpOnly, "203.0.113.99:56789"));
    }

    @Test
    void narrowingLeavesReflexiveAndRelayedCandidatesAlone() {
        // A STUN or TURN server answers with what the outside sees, which is never an interface
        String gathered = "v=0\r\n"
                + "a=candidate:1 1 udp 2130706431 192.168.1.5 19135 typ host\r\n"
                + "a=candidate:2 1 udp 2130706431 172.17.0.2 19135 typ host\r\n"
                + "a=candidate:3 1 udp 1694498815 203.0.113.7 19135 typ srflx raddr 192.168.1.5 rport 19135\r\n"
                + "a=candidate:4 1 udp 16777215 198.51.100.9 40000 typ relay raddr 203.0.113.7 rport 19135\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n";

        String result = advertised(gathered, "192.168.1.5");
        assertTrue(result.contains("192.168.1.5 19135 typ host"));
        assertFalse(result.contains("172.17.0.2"));
        assertTrue(result.contains("203.0.113.7 19135 typ srflx"));
        assertTrue(result.contains("198.51.100.9 40000 typ relay"));

        String withEndpoint = advertised(gathered, "192.168.1.5:19135", "203.0.113.99:56789");
        assertTrue(withEndpoint.contains("192.168.1.5 19135 typ host"));
        assertFalse(withEndpoint.contains("172.17.0.2"));
        assertTrue(withEndpoint.contains("203.0.113.7 19135 typ srflx"));
        assertTrue(withEndpoint.contains("198.51.100.9 40000 typ relay"));
        assertTrue(withEndpoint.contains("203.0.113.99 56789 typ srflx raddr 192.168.1.5 rport 19135"));
    }

    // Added endpoints: STUN mappings and the typed address

    @Test
    void aBareAddedAddressTakesTheMediaPort() {
        assertEquals(GATHERED.replace("a=end-of-candidates", TRANSLATED + "a=end-of-candidates"),
                added(GATHERED, "203.0.113.99"));
        String ipv4Only = "v=0\r\na=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n";
        assertEquals(ipv4Only, added(ipv4Only, "[2001:db8::99]:19132"), "nor one with no host of the family");
    }

    @Test
    void addsADiscoveredMappingOnItsOwnPortWithoutNarrowing() {
        String result = added(GATHERED, "203.0.113.99:40404");
        assertTrue(result.contains(" 203.0.113.99 40404 typ srflx raddr 10.88.0.4 rport 19135\r\n"));
        assertTrue(result.contains("172.17.0.2") && result.contains("fd7a::1"));
        assertEquals(GATHERED, added(GATHERED, "10.88.0.4:19135"), "an endpoint this host gathered is not repeated");
    }

    @Test
    void aGatheredMappingHidesOnlyItsOwnPort() {
        // ICE's own STUN found the port the router moved to, while a forward may still take the typed one
        String gathered = GATHERED.replace("a=end-of-candidates",
                "a=candidate:5 1 udp 1677729535 203.0.113.99 1454 typ srflx raddr 10.88.0.4 rport 19135\r\n"
                        + "a=end-of-candidates");

        assertEquals(gathered, added(gathered, "203.0.113.99:1454"));
        assertTrue(added(gathered, "203.0.113.99:19135")
                .contains(" 203.0.113.99 19135 typ srflx raddr 10.88.0.4 rport 19135\r\n"));
    }

    // Relays

    @Test
    void relayedMediaAnnouncesOnlyTheRelaysWithTheirBaseHidden() {
        assertEquals("v=0\r\n"
                + "a=candidate:80000000 1 udp 1694498815 203.0.113.99 19165 typ srflx raddr 0.0.0.0 rport 0\r\n"
                + "a=candidate:80000001 1 udp 1694498559 2001:db8::99 19165 typ srflx raddr :: rport 0\r\n"
                + "a=end-of-candidates\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n",
                relayed(GATHERED, "203.0.113.99:19165", "[2001:db8::99]:19165"));
    }

    @Test
    void relayedMediaNeverKeepsAGatheredHostOrReflexiveCandidate() {
        String gathered = GATHERED.replace("a=end-of-candidates",
                "a=candidate:5 1 udp 1677729535 203.0.113.50 1454 typ srflx raddr 10.88.0.4 rport 19135\r\n"
                        + "a=end-of-candidates");
        String result = relayed(gathered, "10.88.0.4:19135");

        assertEquals(1, count(result, "a=candidate:"), result);
        assertTrue(result.contains(" 10.88.0.4 19135 typ srflx raddr 0.0.0.0 rport 0\r\n"), result);
    }

    @Test
    void relayedMediaAnnouncesNothingRatherThanThisHost() {
        String ipv4Only = "v=0\r\na=candidate:1 1 udp 2130706431 10.88.0.4 19135 typ host\r\n";
        assertEquals("v=0\r\n", relayed(ipv4Only, "[2001:db8::99]:19165"));
    }

    @Test
    void relayedMediaKeepsTurnWithoutItsOrigin() {
        // TURN uses sockets of its own, so it stays a path behind relays, but its related address
        // is this host's public mapping, which the relays exist to hide
        String result = relayed(ANSWER, "198.51.100.200:19132");

        assertTrue(result.contains("a=candidate:3 1 udp 41885439 198.51.100.9 40000 typ relay raddr 0.0.0.0 rport 0"
                + " generation 0 network-id 1\r\n"), result);
        assertFalse(result.contains("203.0.113.50"), result);
        assertFalse(result.contains("192.168.1.5"), result);
        assertTrue(result.contains(" 198.51.100.200 19132 typ srflx raddr 0.0.0.0 rport 0\r\n"), result);
    }

    // m= and c=

    @Test
    void connectionLinesNameOnlyWhatTheAnswerAnnounces() {
        // Upstream left m= and c= at the host's own address in relayed answers
        String result = relayed(ANSWER_WITHOUT_TURN, "198.51.100.200:19132");

        assertTrue(result.contains("m=application 19132 UDP/DTLS/SCTP webrtc-datachannel\r\n"), result);
        assertTrue(result.contains("c=IN IP4 198.51.100.200\r\n"), result);
    }

    @Test
    void connectionLinesPreferRelayedOverReflexiveOverHost() {
        assertEquals(ANSWER.replace("m=application 19135", "m=application 40000")
                        .replace("c=IN IP4 203.0.113.50", "c=IN IP4 198.51.100.9"),
                AnswerCandidates.fullAnswer(ANSWER, CandidateAnnouncement.AS_GATHERED));

        String narrowed = advertised(ANSWER_WITHOUT_TURN, "192.168.1.5");
        assertTrue(narrowed.contains("m=application 19135 ") && narrowed.contains("c=IN IP4 203.0.113.50\r\n"),
                "the reflexive candidate stays and wins over the host");
    }

    @Test
    void connectionLinesPreferIpv4AndFallBackToTheDummyAddress() {
        List<String> ipv6First = AnswerCandidates.withConnectionAddresses(List.of(
                "m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
                "c=IN IP4 0.0.0.0",
                "a=candidate:1 1 udp 1 2001:db8::5 5000 typ relay",
                "a=candidate:2 1 udp 1 203.0.113.5 6000 typ host"));
        assertEquals("m=application 6000 UDP/DTLS/SCTP webrtc-datachannel", ipv6First.get(0));
        assertEquals("c=IN IP4 203.0.113.5", ipv6First.get(1));

        List<String> ipv6Only = AnswerCandidates.withConnectionAddresses(List.of(
                "m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
                "c=IN IP4 0.0.0.0",
                "a=candidate:1 1 udp 1 2001:db8::5 5000 typ host"));
        assertEquals("c=IN IP6 2001:db8::5", ipv6Only.get(1));

        List<String> none = AnswerCandidates.withConnectionAddresses(List.of(
                "m=application 19135 UDP/DTLS/SCTP webrtc-datachannel",
                "c=IN IP4 203.0.113.50",
                "a=candidate:1 1 tcp 1 203.0.113.5 9 typ host tcptype active"));
        assertEquals(List.of("m=application 9 UDP/DTLS/SCTP webrtc-datachannel", "c=IN IP4 0.0.0.0",
                "a=candidate:1 1 tcp 1 203.0.113.5 9 typ host tcptype active"), none);

        List<String> rejected = AnswerCandidates.withConnectionAddresses(List.of(
                "m=application 0 UDP/DTLS/SCTP webrtc-datachannel", "c=IN IP4 0.0.0.0"));
        assertEquals("m=application 0 UDP/DTLS/SCTP webrtc-datachannel", rejected.get(0), "a rejected section stays");
    }

    @Test
    void addedCandidatesGoAheadOfAnEndOfCandidatesLine() {
        String answer = ANSWER.replace("a=ice-ufrag", "a=end-of-candidates\r\na=ice-ufrag");
        String result = added(answer, "198.51.100.77");

        assertTrue(result.contains(" 198.51.100.77 19135 typ srflx raddr 192.168.1.5 rport 19135\r\n"
                + "a=end-of-candidates\r\n"), result);
    }
}

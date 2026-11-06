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

import org.junit.jupiter.api.Test;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteCandidateIntakeTest {
    private static final InetSocketAddress PUBLIC_PEER = new InetSocketAddress("93.184.216.34", 44321);

    /** An HTTP offer as retail 1.26.62 sends it: host candidates only, mid 0. */
    private static final String OFFER = "v=0\r\n"
            + "o=- 1 2 IN IP4 127.0.0.1\r\n"
            + "a=group:BUNDLE 0\r\n"
            + "m=application 55473 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "c=IN IP4 192.168.1.76\r\n"
            + "a=candidate:1 1 udp 2122194687 192.168.1.76 55473 typ host generation 0 network-id 1\r\n"
            + "a=candidate:2 1 udp 2122129151 100.108.10.95 55472 typ host generation 0 network-id 2\r\n"
            + "a=ice-ufrag:abcd\r\n"
            + "a=mid:0\r\n";

    private static final String HOST_ONLY_OFFER = "v=0\r\n"
            + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "a=candidate:1 1 udp 2122194687 192.168.1.76 55473 typ host\r\n"
            + "a=candidate:2 1 udp 2122129151 100.108.10.95 55472 typ host\r\n"
            + "a=candidate:3 1 tcp 2122063615 10.7.0.2 9 typ host tcptype active\r\n"
            + "a=mid:0\r\n";

    private final ManualResolver resolver = new ManualResolver();
    private final List<RemoteCandidate> lookedUp = new CopyOnWriteArrayList<>();

    private static long candidates(String sdp) {
        return sdp.lines().filter(line -> line.startsWith("a=candidate:")).count();
    }

    /** A client attempt's intake whose lookups the test answers, recording what they hand on. */
    private RemoteCandidateIntake client(String offer) {
        return RemoteCandidateIntake.client(offer, resolver, lookedUp::add);
    }

    // The remote description

    @Test
    void keepsOnlyIpUdpCandidatesUpToTheCap() {
        StringBuilder offer = new StringBuilder("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n")
                .append("a=candidate:1 1 udp 2130706431 a1b2c3d4.local 5000 typ host\r\n")
                .append("a=candidate:2 1 tcp 2130706431 203.0.113.10 9 typ host tcptype active\r\n")
                .append("a=candidate:3 1 udp 2130706431 2001:db8::1 5000 typ host\r\n");
        for (int i = 0; i < 40; i++) {
            offer.append("a=candidate:").append(10 + i).append(" 1 udp 2130706431 203.0.113.10 ")
                    .append(6000 + i).append(" typ host\r\n");
        }

        String filtered = RemoteCandidateIntake.server().remoteDescription(offer.toString());

        assertFalse(filtered.contains("a1b2c3d4.local"), "a name would make the server look it up");
        assertFalse(filtered.contains(" tcp "), "NetherNet gathers no TCP");
        assertTrue(filtered.contains("2001:db8::1"));
        assertEquals(RemoteCandidateIntake.MAX_CANDIDATES, candidates(filtered));
        assertTrue(filtered.contains("m=application"));
    }

    @Test
    void removesBlankLinesAndUnreadableCandidates() {
        String filtered = RemoteCandidateIntake.server().remoteDescription(
                OFFER.replace("a=ice-ufrag", "\r\na=candidate:9 1 udp\r\na=ice-ufrag") + "\r\n");

        assertFalse(filtered.contains("\r\n\r\n"), "libwebrtc rejects a blank line anywhere");
        assertFalse(filtered.contains("a=candidate:9"));
        assertEquals(OFFER, filtered);
    }

    @Test
    void appliesNothingBehindRelaysButRemembersTheRelays() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.relayed();
        String filtered = intake.remoteDescription(OFFER.replace("a=ice-ufrag",
                "a=candidate:5 1 udp 16777215 192.0.2.9 3478 typ relay raddr 0.0.0.0 rport 0\r\na=ice-ufrag"));

        assertEquals(0, candidates(filtered));
        assertTrue(intake.isPeerRelay(new InetSocketAddress("192.0.2.9", 3478)));
        assertTrue(intake.trickled("candidate:1 1 udp 2122194687 203.0.113.7 54321 typ host").isEmpty());
    }

    // Names, on a client

    @Test
    void aClientLooksNamesUpAndHandsOnTheirLiteralsInsteadOfTheNames() {
        RemoteCandidateIntake intake = client(OFFER);
        String filtered = intake.remoteDescription("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=mid:0\r\n"
                + "a=candidate:1 1 udp 2130706431 node.cluster.local 5000 typ host\r\n"
                + "a=candidate:2 1 udp 2130706431 203.0.113.10 5001 typ host\r\n");
        Optional<RemoteCandidate> trickled = intake.trickled("candidate:3 1 udp 2130706431 relay.example 5002 typ relay"
                + " raddr 0.0.0.0 rport 0");

        assertEquals(0, filtered.lines().filter(line -> line.contains(".local")).count(), "the engine never sees a name");
        assertEquals(1, candidates(filtered), "the literal stays in the description");
        assertTrue(trickled.isEmpty(), "a name follows once looked up");
        assertEquals(List.of("node.cluster.local", "relay.example"), resolver.names());

        resolver.answer("relay.example", "2001:db8::7");
        resolver.answer("node.cluster.local", "::ffff:10.0.0.7");
        assertEquals(List.of(
                new RemoteCandidate("candidate:3 1 udp 2130706431 2001:db8::7 5002 typ relay raddr 0.0.0.0 rport 0", "0", 0),
                new RemoteCandidate("candidate:1 1 udp 2130706431 10.0.0.7 5000 typ host", "0", 0)), lookedUp);
        assertTrue(intake.isPeerRelay(new InetSocketAddress("2001:db8::7", 5002)), "a looked-up relay is noted");
    }

    @Test
    void aNameIsCheckedAtTheAddressItResolvesTo() throws Exception {
        RemoteCandidateIntake intake = client(OFFER);
        // As glibc reads them; the JDK refuses some of these spellings and the name is dropped then
        List<String> names = List.of("0xe00000fb", "0340.0.0.373", "bcast.example", "any.example", "mapped.example",
                "missing.example");
        for (String name : names) {
            intake.trickled("candidate:1 1 udp 2130706431 " + name + " 5000 typ host");
        }

        resolver.answer("0xe00000fb", "224.0.0.251");
        resolver.answer("0340.0.0.373", "224.0.0.251");
        resolver.answer("bcast.example", "255.255.255.255");
        resolver.answer("any.example", "::");
        resolver.answer("mapped.example", Inet6Address.getByAddress(null,
                new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 255, (byte) 255, (byte) 224, 0, 0, (byte) 251}, -1));
        resolver.fail("missing.example");

        assertEquals(names, resolver.names());
        assertEquals(List.of(), lookedUp);
    }

    /**
     * The system resolver reads these spellings itself, or refuses them, without asking DNS: the
     * JDK parses the decimal forms and refuses ambiguous ones such as {@code 0xe00000fb}.
     */
    @Test
    void theSystemResolverMeetsNumericSpellingsAsTheAddressesTheyStandFor() throws Exception {
        List<CompletableFuture<InetAddress>> lookups = new CopyOnWriteArrayList<>();
        RemoteCandidateIntake intake = RemoteCandidateIntake.client(OFFER, name -> {
            CompletableFuture<InetAddress> lookup = RemoteCandidateIntake.Resolver.SYSTEM.resolve(name)
                    .toCompletableFuture();
            lookups.add(lookup);
            return lookup;
        }, lookedUp::add);
        for (String name : List.of("3758096635", "224.251", "4294967295", "0", "0xe00000fb", "1.2.3")) {
            intake.trickled("candidate:1 1 udp 2130706431 " + name + " 5000 typ host");
        }
        for (CompletableFuture<InetAddress> lookup : lookups) {
            lookup.handle((address, failure) -> null).get(10, TimeUnit.SECONDS);
        }

        assertEquals(InetAddress.ofLiteral("224.0.0.251"), lookups.getFirst().get());
        // Follow-ups run once their lookup completed, on its thread
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (lookedUp.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(List.of(new RemoteCandidate("candidate:1 1 udp 2130706431 1.2.0.3 5000 typ host", "0", 0)),
                lookedUp, "only 1.2.3, which stands for 1.2.0.3, is usable");
    }

    @Test
    void aClientLooksUpAtMostTheCapOfNamesPerAttempt() {
        RemoteCandidateIntake intake = client(OFFER);
        StringBuilder answer = new StringBuilder("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n");
        for (int i = 0; i < 20; i++) {
            answer.append("a=candidate:").append(i).append(" 1 udp 1 answer").append(i).append(".example 5000 typ host\r\n");
        }
        intake.remoteDescription(answer.toString());
        for (int i = 0; i < 20; i++) {
            intake.trickled("candidate:" + i + " 1 udp 1 trickled" + i + ".example 5000 typ host");
        }

        assertEquals(RemoteCandidateIntake.MAX_LOOKUPS, resolver.names().size());
        assertEquals("trickled11.example", resolver.names().getLast(), "in the order the names came");
    }

    @Test
    void aNameHoldsItsPlaceUnderTheCapWhileLookedUpAndOnlyNamesAreLookedUp() {
        RemoteCandidateIntake intake = client(OFFER);
        intake.trickled("candidate:1 1 udp 1 first.example 5000 typ host");
        intake.trickled("candidate:3 1 tcp 1 tcp.example 9 typ host tcptype active");
        intake.trickled("candidate:4 1 udp 1  5000 typ host");
        int taken = 0;
        for (int i = 0; i < 40; i++) {
            if (intake.trickled("candidate:" + i + " 1 udp 1 203.0.113.7 " + (1000 + i) + " typ host").isPresent()) {
                taken++;
            }
        }
        intake.trickled("candidate:2 1 udp 1 late.example 5000 typ host");

        assertEquals(RemoteCandidateIntake.MAX_CANDIDATES - 1, taken);
        assertEquals(List.of("first.example"), resolver.names(), "no TCP, nothing past the cap, no empty name");
    }

    @Test
    void aNameKeepsTheSectionItWasTaggedWithWhenItArrived() {
        RemoteCandidateIntake intake = client(OFFER.replace("a=mid:0", "a=mid:offer"));
        intake.trickled("candidate:1 1 udp 1 early.example 5000 typ host");
        intake.remoteDescription("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:answer\r\n");

        resolver.answer("early.example", "203.0.113.7");
        assertEquals("offer", lookedUp.getFirst().sdpMid());
    }

    @Test
    void aServerLooksNothingUp() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=candidate:1 1 udp 2130706431 0xe00000fb 5000 typ host\r\n");

        assertTrue(intake.trickled("candidate:1 1 udp 2130706431 3758096635 5000 typ host").isEmpty());
    }

    // Trickled candidates

    @Test
    void tagsTrickledCandidatesWithTheOffersOwnMid() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(OFFER.replace("a=mid:0", "a=mid:data").replace("a=group:BUNDLE 0", "a=group:BUNDLE data"));

        assertEquals(Optional.of(new RemoteCandidate("candidate:7 1 udp 2122194687 203.0.113.7 54321 typ host",
                        "data", 0)),
                intake.trickled("a=candidate:7 1 udp 2122194687 203.0.113.7 54321 typ host\r\n"));
    }

    @Test
    void aClientTagsCandidatesTrickledAheadOfTheAnswerWithItsOffersMid() {
        RemoteCandidateIntake intake = client(OFFER.replace("a=mid:0", "a=mid:data"));

        assertEquals("data", intake.trickled("candidate:1 1 udp 1 203.0.113.7 1 typ host").orElseThrow().sdpMid());
        intake.remoteDescription("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n");
        assertEquals("data", intake.trickled("candidate:2 1 udp 1 203.0.113.7 2 typ host").orElseThrow().sdpMid(),
                "an answer without a mid keeps the offer's");
    }

    @Test
    void tagsTheFirstMediaSection() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription("v=0\r\na=mid:session\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=mid:first\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:second\r\n");

        assertEquals("first", intake.trickled("candidate:1 1 udp 1 203.0.113.7 1 typ host").orElseThrow().sdpMid());
    }

    @Test
    void anOfferWithoutAMidLeavesTheEngineToGoByIndex() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(OFFER.replace("a=mid:0\r\n", ""));

        RemoteCandidate candidate = intake.trickled("candidate:1 1 udp 1 203.0.113.7 1 typ host").orElseThrow();
        assertEquals("", candidate.sdpMid());
        assertEquals(0, candidate.sdpMLineIndex());
    }

    @Test
    void takesAtMostTheCapFromTrickling() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(OFFER);
        int taken = 0;
        for (int i = 0; i < 100; i++) {
            if (intake.trickled("candidate:" + i + " 1 udp 1 203.0.113.7 " + (1000 + i) + " typ host").isPresent()) {
                taken++;
            }
        }
        assertEquals(RemoteCandidateIntake.MAX_CANDIDATES, taken);
    }

    /** Java reads 008.008.008.008 as an address; the engine would take it for a name and look it up. */
    @Test
    void passesAddressesOnInCanonicalForm() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        String offer = intake.remoteDescription(OFFER.replace("192.168.1.76 55473 typ host", "008.008.008.008 55473 typ host"));

        assertTrue(offer.contains("a=candidate:1 1 udp 2122194687 8.8.8.8 55473 typ host generation 0 network-id 1"));
        assertFalse(offer.contains("008.008.008.008 55473"));
        assertEquals("candidate:5 1 udp 2130706431 8.8.4.4 5000 typ host",
                intake.trickled("candidate:5 1 udp 2130706431 008.008.004.004 5000 typ host").orElseThrow().sdp());
    }

    @Test
    void refusesNamesTcpAndUnreadableTrickledCandidates() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(OFFER);

        assertTrue(intake.trickled("candidate:1 1 udp 2130706431 a1b2c3d4.local 5000 typ host").isEmpty());
        assertTrue(intake.trickled("candidate:1 1 udp 2130706431 lookup.example 5000 typ host").isEmpty());
        assertTrue(intake.trickled("candidate:1 1 tcp 2130706431 203.0.113.10 9 typ host tcptype active").isEmpty());
        assertTrue(intake.trickled("candidate:1 1 udp").isEmpty());
        assertTrue(intake.trickled("a=end-of-candidates").isEmpty());
        assertTrue(intake.trickled("candidate:1 1 udp 2130706431 2001:db8::1 5000 typ host").isPresent());
    }

    // Unusable addresses

    private static final List<String> UNUSABLE = List.of("224.0.0.251", "239.255.255.250", "ff02::fb",
            "255.255.255.255", "0.0.0.0", "::", "::ffff:224.0.0.251", "::ffff:255.255.255.255", "::ffff:0.0.0.0");

    @Test
    void serverAndClientDropCandidatesAtMulticastBroadcastOrUnspecifiedAddresses() {
        StringBuilder offer = new StringBuilder("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:0\r\n");
        for (String address : UNUSABLE) {
            offer.append("a=candidate:1 1 udp 2122194687 ").append(address).append(" 5000 typ host\r\n");
        }
        for (RemoteCandidateIntake intake : List.of(RemoteCandidateIntake.server(), client(OFFER))) {
            assertEquals(0, candidates(intake.remoteDescription(offer.toString())));
            for (String address : UNUSABLE) {
                assertTrue(intake.trickled("candidate:2 1 udp 2122194687 " + address + " 5000 typ host").isEmpty(),
                        address);
            }
        }
    }

    @Test
    void loopbackAndLinkLocalCandidatesStay() {
        List<String> kept = List.of("127.0.0.1", "::1", "169.254.10.20", "fe80::1", "::ffff:169.254.10.20");
        for (RemoteCandidateIntake intake : List.of(RemoteCandidateIntake.server(), client(OFFER))) {
            intake.remoteDescription(OFFER);
            for (String address : kept) {
                assertTrue(intake.trickled("candidate:2 1 udp 2122194687 " + address + " 5000 typ host").isPresent(),
                        address);
            }
        }
    }

    @Test
    void aDroppedCandidateCountsForNothing() {
        // A reflexive candidate would mean the peer needs no guessing, and a relay would be noted
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(HOST_ONLY_OFFER.replace("a=mid:0",
                "a=candidate:4 1 udp 1678767103 0.0.0.0 55474 typ srflx raddr 0.0.0.0 rport 0\r\na=mid:0"));
        intake.trickled("candidate:5 1 udp 16777215 224.0.0.1 3478 typ relay raddr 0.0.0.0 rport 0");

        assertEquals(2, intake.inferredCandidates("v=0\r\n", PUBLIC_PEER).size());
        assertFalse(intake.isPeerRelay(new InetSocketAddress("224.0.0.1", 3478)));
    }

    // Relays

    /**
     * IP and port, not IP alone: a TURN server sends from the relayed transport address its
     * candidate names, so another port on that host is not the relay. Also IPv6 and an
     * IPv4-mapped spelling of a relay, as a dual-stack PROXY v2 relay may write it.
     */
    @Test
    void relaysMatchByAddressAndPort() throws Exception {
        RemoteCandidateIntake intake = RemoteCandidateIntake.relayed();
        intake.trickled("candidate:1 1 UDP 16777215 192.0.2.9 3478 typ relay raddr 0.0.0.0 rport 0");
        intake.trickled("candidate:2 1 UDP 16777215 2001:db8::1 50124 typ relay raddr :: rport 0");
        intake.trickled("candidate:3 1 UDP 1694498815 203.0.113.7 54321 typ srflx raddr 0.0.0.0 rport 0");

        assertTrue(intake.isPeerRelay(new InetSocketAddress("192.0.2.9", 3478)));
        assertTrue(intake.isPeerRelay(new InetSocketAddress(InetAddress.getByName("::ffff:192.0.2.9"), 3478)));
        assertTrue(intake.isPeerRelay(new InetSocketAddress("2001:db8:0:0:0:0:0:1", 50124)));
        assertFalse(intake.isPeerRelay(new InetSocketAddress("192.0.2.9", 3479)));
        assertFalse(intake.isPeerRelay(new InetSocketAddress("203.0.113.7", 54321)));
    }

    @Test
    void remembersNoRelayItCannotReadAnEndpointOutOf() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.trickled("candidate:1 1 udp 16777215 turn.example 3478 typ relay");
        intake.trickled("candidate:2 1 udp 16777215 192.0.2.9 0 typ relay");

        assertFalse(intake.isPeerRelay(new InetSocketAddress("192.0.2.9", 0)));
    }

    // Inferred candidates, ported from upstream's SdpUtilTest

    private static List<RemoteCandidate> inferred(String offer, InetSocketAddress from) {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(offer);
        return intake.inferredCandidates("v=0\r\n", from);
    }

    @Test
    void infersACandidateForEveryPortAPeerGathered() {
        List<RemoteCandidate> inferred = inferred(HOST_ONLY_OFFER, PUBLIC_PEER);

        // One per UDP port, at the address the offer arrived from, TCP left alone
        assertEquals(List.of(
                new RemoteCandidate("candidate:90000000 1 udp 1677721855 93.184.216.34 55473 typ srflx raddr 0.0.0.0 rport 0",
                        "0", 0),
                new RemoteCandidate("candidate:90000001 1 udp 1677721855 93.184.216.34 55472 typ srflx raddr 0.0.0.0 rport 0",
                        "0", 0)), inferred);
    }

    @Test
    void infersFromTrickledHostCandidatesToo() {
        // An Xbox or LAN offer carries no candidates and trickles them all
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:0\r\n");
        intake.trickled("candidate:1 1 udp 2122194687 192.168.1.76 55473 typ host");

        assertEquals(1, intake.inferredCandidates("v=0\r\n", new InetSocketAddress("2606:4700::7", 1)).size());
        assertTrue(intake.inferredCandidates("v=0\r\n", new InetSocketAddress("2606:4700::7", 1)).getFirst().sdp()
                .endsWith(" 2606:4700::7 55473 typ srflx raddr :: rport 0"));
    }

    @Test
    void guessesAtNoMorePortsThanAPeerCouldPlausiblyHold() {
        StringBuilder sdp = new StringBuilder("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n");
        for (int i = 0; i < 200; i++) {
            sdp.append("a=candidate:").append(i).append(" 1 udp 2122194687 192.168.1.76 ")
                    .append(50000 + i).append(" typ host\r\n");
        }

        // The peer decides how many candidates it sends, so it must not decide how many packets leave
        assertEquals(RemoteCandidateIntake.MAX_INFERRED_CANDIDATES, inferred(sdp.toString(), PUBLIC_PEER).size());
    }

    @Test
    void guessesOncePerPortHoweverOftenItAppears() {
        String offer = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=candidate:1 1 udp 2122194687 192.168.1.76 55473 typ host\r\n"
                + "a=candidate:2 1 udp 2122194687 10.7.0.2 55473 typ host\r\n";

        assertEquals(1, inferred(offer, PUBLIC_PEER).size());
    }

    @Test
    void infersNothingForAPeerThatAlreadyHasAReflexiveCandidate() {
        String offer = HOST_ONLY_OFFER.replace("a=mid:0",
                "a=candidate:4 1 udp 1678767103 198.51.100.4 55474 typ srflx raddr 0.0.0.0 rport 0\r\na=mid:0");

        assertTrue(inferred(offer, PUBLIC_PEER).isEmpty());
    }

    @Test
    void infersNothingWhenThePeerSignaledFromThisNetworkOrBehindCarrierNat() {
        for (InetSocketAddress from : List.of(new InetSocketAddress("192.168.1.5", 44321),
                new InetSocketAddress("127.0.0.1", 44321), new InetSocketAddress("100.108.10.95", 44321),
                new InetSocketAddress("fd7a:115c:a1e0::1", 44321), new InetSocketAddress("240.1.2.3", 0),
                InetSocketAddress.createUnresolved("example.test", 44321))) {
            assertTrue(inferred(HOST_ONLY_OFFER, from).isEmpty(), from.toString());
        }
    }

    @Test
    void infersNothingWhenThisHostIsReachable() {
        RemoteCandidateIntake intake = RemoteCandidateIntake.server();
        intake.remoteDescription(HOST_ONLY_OFFER);

        assertTrue(intake.inferredCandidates(
                "a=candidate:1 1 udp 2122194687 93.184.216.35 19135 typ host\r\n", PUBLIC_PEER).isEmpty());
    }

    @Test
    void infersNothingAsAClientOrBehindRelays() {
        for (RemoteCandidateIntake intake : List.of(client(OFFER), RemoteCandidateIntake.relayed())) {
            intake.remoteDescription(HOST_ONLY_OFFER);
            assertTrue(intake.inferredCandidates("v=0\r\n", PUBLIC_PEER).isEmpty());
        }
    }

    @Test
    void seesAHostCandidateAPeerElsewhereCouldReach() {
        assertTrue(RemoteCandidateIntake.hasPublicHostCandidate(
                "a=candidate:1 1 udp 2122194687 93.184.216.34 19135 typ host\r\n"));
        assertTrue(RemoteCandidateIntake.hasPublicHostCandidate(
                "a=candidate:1 1 udp 2122194687 2606:4700:4700::1111 19135 typ host\r\n"));
    }

    @Test
    void countsNoAddressThatOnlyWorksOnTheLocalNetworkOrIsNoInterface() {
        for (String line : List.of(
                "a=candidate:1 1 udp 2122194687 192.168.1.76 55473 typ host",
                "a=candidate:1 1 udp 2122194687 100.64.0.1 19135 typ host",
                "a=candidate:1 1 udp 2122194687 fd7a:115c:a1e0::bf3a:a60 55476 typ host",
                "a=candidate:1 1 udp 2122194687 0.0.0.0 19135 typ host",
                "a=candidate:1 1 udp 2122194687 fe80::1 19135 typ host",
                "a=candidate:1 1 udp 2122194687 c0ffee.local 19135 typ host",
                "a=candidate:1 1 udp 2122194687 203.0.113.9 19135 not-typ host",
                // The address is public but it is a hole in a NAT, not an address this host holds
                "a=candidate:13 1 UDP 1678767103 213.14.155.171 19135 typ srflx raddr 0.0.0.0 rport 0")) {
            assertFalse(RemoteCandidateIntake.hasPublicHostCandidate(line + "\r\n"), line);
        }
    }
}

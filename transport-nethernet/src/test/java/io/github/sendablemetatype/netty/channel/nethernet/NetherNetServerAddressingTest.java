package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressChangedEvent;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeIceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.RemoteSignal;
import io.github.sendablemetatype.netty.util.address.ProxyList;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferFixtures;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.ANSWER;
import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.OFFER;
import static org.junit.jupiter.api.Assertions.*;

/** What the endpoint does with addresses: candidate intake, what answers announce, what children report. */
@Timeout(15)
class NetherNetServerAddressingTest {
    /** An address {@code EndpointAddress} counts as public, which inferring and typed addresses need. */
    private static final InetSocketAddress PUBLIC_CLIENT = new InetSocketAddress("93.184.216.34", 44321);
    private static final String HOST_ONLY_OFFER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "a=mid:data\r\na=candidate:1 1 udp 2122260223 192.168.1.20 51234 typ host\r\n"
            + "a=max-message-size:262144\r\n";
    /** What the engine gathered on a host with a private interface address only. */
    private static final String GATHERED = "v=0\r\nm=application 5000 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "c=IN IP4 10.0.0.5\r\na=candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host\r\n"
            + "a=fingerprint:sha-256 " + OfferFixtures.DIGEST + "\r\na=mid:data\r\na=end-of-candidates\r\n";

    private final EndpointHarness harness = new EndpointHarness();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void onlyIpLiteralUdpCandidatesReachTheEngine() throws Exception {
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer("1", "v=0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:data\r\n"
                + "a=candidate:1 1 udp 2122260223 192.168.1.20 51234 typ host\r\n"
                + "a=candidate:2 1 udp 2122260223 client.example 51235 typ host\r\n"
                + "a=candidate:3 1 tcp 1518280447 192.168.1.20 9 typ host tcptype active\r\n"
                + "a=max-message-size:262144\r\n");
        FakePeer peer = harness.negotiating();

        String applied = peer.remoteOffers().getFirst();
        assertTrue(applied.contains("192.168.1.20 51234 typ host"));
        assertFalse(applied.contains("client.example"), "a name would make the server look it up");
        assertFalse(applied.contains(" tcp "));

        reply.signal(new RemoteSignal.Candidate("candidate:4 1 udp 2122260223 name.example 5000 typ host"));
        reply.signal(new RemoteSignal.Candidate("candidate:5 1 udp 2122260223 192.168.1.21 5000 typ host"));
        harness.sync();
        List<FakePeer.RemoteCandidate> trickled = peer.remoteCandidates();
        assertEquals(1, trickled.size());
        assertTrue(trickled.getFirst().candidate().contains("192.168.1.21 5000 typ host"));
        assertEquals("data", trickled.getFirst().mid(), "trickled candidates take the offer's own mid");
    }

    @Test
    void aFullAnswerAnnouncesTheAdvertisedAddresses() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(GATHERED);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES,
                List.of(new InetSocketAddress("203.0.113.7", 0)));
        harness.bind();

        String answer = harness.source(false).offer("1", OFFER).answer();

        assertTrue(answer.contains("a=candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host\r\n"));
        assertTrue(answer.contains(" 203.0.113.7 5000 typ srflx raddr 10.0.0.5 rport 5000\r\n"), answer);
        assertTrue(answer.contains("c=IN IP4 203.0.113.7\r\n"), "m= and c= follow what the answer announces");
        assertTrue(answer.contains("a=identity:"));
        assertTrue(answer.endsWith("a=end-of-candidates\r\n"));
        assertEquals(1, answer.split("a=end-of-candidates", -1).length - 1);
    }

    @Test
    void advertisedNamesAreLookedUpOffTheLoopAndAnnouncedOnceTheyResolve() throws Exception {
        CompletableFuture<List<InetSocketAddress>> lookup = new CompletableFuture<>();
        try (EndpointHarness names = new EndpointHarness(OfferCheck::newExecutor, endpoints -> {
            try {
                return lookup.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new UnknownHostException("The test never answered the lookup");
            }
        })) {
            names.engine.answerWith(ANSWER);
            names.engine.gatherWith(GATHERED);
            names.bootstrap.option(NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES, List.of(
                    new InetSocketAddress("203.0.113.7", 0), InetSocketAddress.createUnresolved("play.example", 0)));
            NetherNetServerChannel server = names.bind();
            FakeJoinSource source = names.source(false);
            assertEquals(List.of(new InetSocketAddress("203.0.113.7", 0)), server.advertisedAddresses());

            String before = source.offer("1", OFFER).answer();
            assertTrue(before.contains(" 203.0.113.7 5000 typ srflx"), "the literal is announced at once: " + before);
            assertFalse(before.contains("203.0.113.8"), before);

            List<InetSocketAddress> resolved =
                    List.of(new InetSocketAddress("203.0.113.7", 0), new InetSocketAddress("203.0.113.8", 0));
            lookup.complete(resolved);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            String after;
            long connectionId = 2;
            do {
                after = source.offer(Long.toString(connectionId++), OFFER).answer();
            } while (!after.contains("203.0.113.8") && System.nanoTime() < deadline);
            assertTrue(after.contains(" 203.0.113.8 5000 typ srflx"), after);
            assertTrue(after.contains(" 203.0.113.7 5000 typ srflx"), after);
            assertEquals(resolved, server.advertisedAddresses());

            server.close().sync();
            assertEquals(List.of(), server.advertisedAddresses(), "a closed endpoint announces nothing");
        }
    }

    @Test
    void nothingIsAnnouncedBeforeBind() {
        NetherNetServerChannel server = new NetherNetServerChannel();
        assertEquals(List.of(), server.advertisedAddresses());
        assertEquals(List.of(), server.discoveredAddresses());
    }

    @Test
    void anAdvertisedNameThatDoesNotResolveClosesTheEndpoint() throws Exception {
        try (EndpointHarness names = new EndpointHarness(OfferCheck::newExecutor, endpoints -> {
            throw new UnknownHostException("play.example");
        })) {
            names.bootstrap.option(NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES,
                    List.of(InetSocketAddress.createUnresolved("play.example", 0)));
            NetherNetServerChannel server = names.bind();
            assertTrue(server.closeFuture().await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void trickledCandidatesGetTheAdvertisedTranslations() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES,
                List.of(new InetSocketAddress("203.0.113.7", 0)));
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer("1", OFFER);
        reply.answer();
        FakePeer peer = harness.engine.lastPeer();

        peer.localCandidate("candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host", "data");
        peer.gatheringComplete(GATHERED);
        harness.sync();

        List<String> sent = reply.drain();
        assertEquals(2, sent.size(), sent.toString());
        assertEquals("candidate candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host", sent.get(0));
        assertTrue(sent.get(1).contains(" 203.0.113.7 5000 typ srflx raddr 10.0.0.5 rport 5000"), sent.get(1));
    }

    @Test
    void aFullAnswerAddsTheAddressThePlayerTyped() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(GATHERED);
        NetherNetServerChannel server = harness.bind();
        int icePort = ((InetSocketAddress) server.localAddress()).getPort();

        FakeJoinSource source = harness.source(false);
        String answer = source.offer("1", OFFER, new SourcedAddress(PUBLIC_CLIENT, AddressSource.SIGNALING, null),
                new Join.Addressed("8.8.8.8:19132", 19132, false, false, null)).answer();
        String proxied = source.offer("2", OFFER, new SourcedAddress(PUBLIC_CLIENT, AddressSource.SIGNALING, null),
                new Join.Addressed("8.8.8.8:19132", 19132, true, false, null)).answer();

        assertTrue(answer.contains(" 8.8.8.8 " + icePort + " typ srflx raddr 10.0.0.5 rport 5000\r\n"), answer);
        assertFalse(proxied.contains(" 8.8.8.8 "), "A trusted proxy's Host names the proxy: " + proxied);
    }

    @Test
    void guessedCandidatesAreCheckedForAClientThatOffersOnlyHostCandidates() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(GATHERED);
        harness.bind();

        harness.source(false).offer("1", HOST_ONLY_OFFER,
                new SourcedAddress(PUBLIC_CLIENT, AddressSource.SIGNALING, null), null).answer();
        harness.sync();

        List<FakePeer.RemoteCandidate> inferred = harness.engine.lastPeer().remoteCandidates();
        assertEquals(1, inferred.size());
        assertTrue(inferred.getFirst().candidate().contains(" 93.184.216.34 51234 typ srflx"),
                inferred.getFirst().candidate());
    }

    @Test
    void guessingCandidatesCanBeTurnedOff() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(GATHERED);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_INFER_PEER_CANDIDATES, false);
        harness.bind();

        harness.source(false).offer("1", HOST_ONLY_OFFER,
                new SourcedAddress(PUBLIC_CLIENT, AddressSource.SIGNALING, null), null).answer();
        harness.sync();

        assertEquals(List.of(), harness.engine.lastPeer().remoteCandidates());
    }

    @Test
    void theChildReportsThePairFromActivationAndEachMoveAfter() throws Exception {
        harness.bind();
        harness.source(true).offer("1", OFFER);
        NetherNetChildChannel child = harness.child();
        List<RemoteAddressChangedEvent> events = new CopyOnWriteArrayList<>();
        child.eventLoop().submit(() -> child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                if (event instanceof RemoteAddressChangedEvent changed) {
                    events.add(changed);
                }
            }
        })).sync();
        FakePeer peer = harness.negotiating();
        peer.completeAnswer(ANSWER);
        assertEquals(FakeJoinSource.CLIENT, child.remoteAddress());

        InetSocketAddress mapped = new InetSocketAddress("203.0.113.50", 6000);
        peer.path(new TransportPath(null, CandidateType.HOST, mapped, CandidateType.SERVER_REFLEXIVE));
        harness.sync();
        assertEquals(FakeJoinSource.CLIENT, child.remoteAddress(), "the signaled address until activation");

        harness.activate(peer);
        assertEquals(mapped, child.remoteAddress());
        assertEquals(AddressSource.ICE, child.reportedAddress().source());
        assertEquals(List.of(), events, "consumers read the address when the channel goes active");

        InetSocketAddress relay = new InetSocketAddress("192.0.2.77", 3478);
        peer.path(new TransportPath(null, CandidateType.HOST, relay, CandidateType.RELAY));
        harness.sync();
        assertEquals(relay, child.remoteAddress());
        assertEquals(1, events.size());
        assertEquals(mapped, events.getFirst().previous().address());
        assertEquals(new SourcedAddress(relay, AddressSource.TURN, null), events.getFirst().current());
    }

    @Test
    void proxyRelaysFrontTheSharedPortAndNoRemoteCandidateIsApplied() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES,
                new TrustedProxies(TrustedProxies.parseList(List.of("192.0.2.10")), ProxyList.NONE));
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer("1", HOST_ONLY_OFFER);
        FakePeer peer = harness.negotiating();

        assertEquals(List.of("192.0.2.10/32"), ((FakeIceEndpoint) peer.config().endpoint()).relays());
        assertFalse(peer.remoteOffers().getFirst().contains("a=candidate"),
                "behind relays players are reached only through them");
        reply.signal(new RemoteSignal.Candidate("candidate:5 1 udp 2122260223 192.168.1.21 5000 typ host"));
        harness.sync();
        assertEquals(List.of(), peer.remoteCandidates());
    }

    /** What the engine gathered behind a relay: a private host, its public mapping and a TURN allocation. */
    private static final String GATHERED_WITH_TURN = "v=0\r\nm=application 5000 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "c=IN IP4 203.0.113.50\r\na=candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host\r\n"
            + "a=candidate:2 1 udp 1686052607 203.0.113.50 5000 typ srflx raddr 10.0.0.5 rport 5000\r\n"
            + "a=candidate:3 1 udp 41885439 192.0.2.80 40000 typ relay raddr 203.0.113.50 rport 5000\r\n"
            + "a=fingerprint:sha-256 " + OfferFixtures.DIGEST + "\r\na=mid:data\r\na=end-of-candidates\r\n";

    private void bindBehindRelaysWithoutAdvertisedAddresses() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(GATHERED_WITH_TURN);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES,
                new TrustedProxies(TrustedProxies.parseList(List.of("192.0.2.10")), ProxyList.NONE));
        harness.bind();
    }

    @Test
    void behindRelaysWithoutAdvertisedAddressesAFullAnswerCarriesOnlyTurn() throws Exception {
        bindBehindRelaysWithoutAdvertisedAddresses();

        // The typed endpoint would be announced on a direct host
        String answer = harness.source(false).offer("1", OFFER,
                new SourcedAddress(PUBLIC_CLIENT, AddressSource.SIGNALING, null),
                new Join.Addressed("8.8.8.8:19132", 19132, false, false, null)).answer();

        assertEquals(1, answer.split("a=candidate:", -1).length - 1, answer);
        assertTrue(answer.contains("a=candidate:3 1 udp 41885439 192.0.2.80 40000 typ relay raddr 0.0.0.0 rport 0\r\n"),
                answer);
        for (String origin : List.of("10.0.0.5", "203.0.113.50", "8.8.8.8")) {
            assertFalse(answer.contains(origin), origin + " leaked: " + answer);
        }
    }

    @Test
    void behindRelaysWithoutAdvertisedAddressesTrickledCandidatesAreOnlyTurn() throws Exception {
        bindBehindRelaysWithoutAdvertisedAddresses();
        FakeJoinSource.Reply reply = harness.source(true).offer("1", OFFER);
        reply.answer();
        FakePeer peer = harness.engine.lastPeer();

        peer.localCandidate("candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host", "data");
        peer.localCandidate("candidate:2 1 udp 1686052607 203.0.113.50 5000 typ srflx raddr 10.0.0.5 rport 5000",
                "data");
        peer.localCandidate("candidate:3 1 udp 41885439 192.0.2.80 40000 typ relay raddr 203.0.113.50 rport 5000",
                "data");
        peer.gatheringComplete(GATHERED_WITH_TURN);
        harness.sync();

        assertEquals(List.of("candidate candidate:3 1 udp 41885439 192.0.2.80 40000 typ relay raddr 0.0.0.0 rport 0"),
                reply.drain().stream().filter(sent -> sent.startsWith("candidate ")).toList());
    }

    @Test
    void behindRelaysNoCandidateIsGuessedForTheClient() throws Exception {
        bindBehindRelaysWithoutAdvertisedAddresses();

        // A check to a guessed candidate would leave this host straight, past the relay
        harness.source(false).offer("1", HOST_ONLY_OFFER,
                new SourcedAddress(PUBLIC_CLIENT, AddressSource.SIGNALING, null), null).answer();
        harness.sync();

        assertEquals(List.of(), harness.engine.lastPeer().remoteCandidates());
    }

    @Test
    void proxyRelaysNeedTheSharedPort() {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES,
                        new TrustedProxies(TrustedProxies.parseList(List.of("192.0.2.10")), ProxyList.NONE))
                .option(NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT, false);
        ChannelFuture bind = harness.bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).awaitUninterruptibly();
        assertInstanceOf(IllegalStateException.class, bind.cause());
    }

    @Test
    void relaysNoneOfWhichCouldBeReadRefuseTheBind() {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES, new TrustedProxies(
                TrustedProxies.parseList(List.of("10.0.0.0/99", "192.0.2.300")), ProxyList.NONE));
        ChannelFuture bind = harness.bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).awaitUninterruptibly();

        // Bound without them, the endpoint would take players directly and announce this host
        assertInstanceOf(UnreadableRelaysException.class, bind.cause());
        assertEquals(List.of(), harness.engine.endpoints(), "refused before the ICE port opened");
    }

    @Test
    void aPartlyReadableRelayListBindsWithTheRelaysItRead() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES, new TrustedProxies(
                TrustedProxies.parseList(List.of("192.0.2.10", "10.0.0.0/99")), ProxyList.NONE));
        harness.bind();
        harness.source(true).offer("1", HOST_ONLY_OFFER);
        FakePeer peer = harness.negotiating();

        assertEquals(List.of("192.0.2.10/32"), harness.engine.endpoints().getFirst().relays());
        assertFalse(peer.remoteOffers().getFirst().contains("a=candidate"), "players are reached only through it");
    }

    @Test
    void anEmptyRelayListBindsWithoutRelays() throws Exception {
        // How a host runs without relays on purpose, also after a list none of whose relays could be read
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES, TrustedProxies.NONE);
        harness.bind();
        harness.source(true).offer("1", HOST_ONLY_OFFER);
        FakePeer peer = harness.negotiating();

        assertEquals(List.of(), harness.engine.endpoints().getFirst().relays());
        assertTrue(peer.remoteOffers().getFirst().contains("192.168.1.20 51234 typ host"), "players connect directly");
    }
}

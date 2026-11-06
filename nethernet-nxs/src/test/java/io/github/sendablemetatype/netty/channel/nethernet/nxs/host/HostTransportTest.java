package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionPrincipal;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfileSnapshot;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.StunServer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import io.github.sendablemetatype.netty.util.nethernet.IdentityPublicKey;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.host.HostFixture.CLIENT;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.host.HostFixture.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The NXS host side on an endpoint of the fake engine. */
@Timeout(30)
class HostTransportTest {
    private final HostFixture fixture;

    HostTransportTest() throws Exception {
        fixture = new HostFixture();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void theAdmissionKeysBecomeThePortsPrefilter() throws Exception {
        fixture.start(HostFixture.fixed());
        assertFalse(fixture.endpoint.holdsChecks(), "without keys no check may reach Java");

        fixture.installKey();
        assertEquals(Set.of("NXS1" + HostFixture.KEY_ID), fixture.endpoint.derivedKeys().keySet());
        assertTrue(fixture.endpoint.holdsChecks());

        fixture.transport.close().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertFalse(fixture.endpoint.holdsChecks());
        assertEquals(Set.of(), fixture.endpoint.derivedKeys().keySet(), "closing revokes the keys");
    }

    @Test
    void aHeldTicketIsTakenByAPeerWithItsCredentialsAndTheHostIdentity() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        StatelessAdmissionCodec.Credentials ticket = fixture.mint(30);

        HostFixture.Check check = fixture.hold(ticket.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        NetherNetChildChannel child = fixture.child();

        assertEquals(new IceCredentials(ticket.localUfrag(), ticket.icePwd()), peer.config().localCredentials());
        assertEquals(fixture.identity.dtls(), peer.config().identity());
        assertEquals(fixture.endpoint, peer.config().endpoint());
        await(() -> !peer.remoteOffers().isEmpty(), "the offer the token implies");
        String offer = peer.remoteOffers().getFirst();
        assertTrue(offer.contains("a=ice-ufrag:" + HostFixture.CLIENT_UFRAG + "\r\n"), offer);
        assertTrue(offer.contains("a=ice-pwd:" + HostFixture.CLIENT_PASSWORD + "\r\n"), offer);
        assertTrue(offer.contains("a=fingerprint:sha-256 " + "AB:".repeat(31) + "AB"), "the client's certificate is pinned");
        assertFalse(check.rejected.isDone());

        assertEquals(CLIENT, child.remoteAddress());
        assertEquals(AddressSource.NXS_ADMISSION, child.reportedAddress().source());
        AdmissionPrincipal principal = child.attr(AdmissionPrincipal.KEY).get();
        assertEquals(TicketId.ofToken(ticket.localUfrag()), principal.ticketId());
        assertEquals("42", principal.networkId());
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(child, fixture.clientKey.getPublic()),
                "the login is bound to the key the provider proved");

        List<TransportOutcome> outcomes = poll(1);
        assertEquals(OutcomeStage.ICE_SEEN, outcomes.getFirst().stage());
    }

    @Test
    void aTicketForAnotherKeyIsRefusedAndAReplayedOneToo() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();

        HostFixture.Check forged = fixture.hold("NXS1" + HostFixture.KEY_ID + "A".repeat(160), CLIENT);
        forged.rejected.get(5, TimeUnit.SECONDS);
        assertEquals(1, fixture.transport.ledgerStats().invalid());

        StatelessAdmissionCodec.Credentials ticket = fixture.mint(30);
        fixture.hold(ticket.localUfrag(), CLIENT);
        fixture.child();
        HostFixture.Check replay = fixture.hold(ticket.localUfrag(), new InetSocketAddress("203.0.113.66", 50001));
        replay.rejected.get(5, TimeUnit.SECONDS);
        assertEquals(1, fixture.transport.ledgerStats().replayRejected());
        assertEquals(1, fixture.engine.peers().size(), "neither got a peer");
    }

    @Test
    void ticketsTakeTheEndpointsBudgetAndARefusedOneIsNotConsumed() throws Exception {
        fixture.bootstrap.option(NetherChannelOption.NETHER_SERVER_MAX_SESSIONS, 1);
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        fixture.hold(fixture.mint(30).localUfrag(), CLIENT);
        fixture.child();

        StatelessAdmissionCodec.Credentials second = fixture.mint(30);
        InetSocketAddress elsewhere = new InetSocketAddress("203.0.113.67", 50002);
        fixture.hold(second.localUfrag(), elsewhere).rejected.get(5, TimeUnit.SECONDS);
        assertEquals(1, fixture.engine.peers().size(), "the endpoint's budget held for NXS too");
        await(() -> fixture.transport.ledgerStats().sessions() == 1, "the refused ticket's record to go");

        fixture.children.clear();
        fixture.engine.lastPeer().closed(io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause.REMOTE_CLOSED);
        await(() -> fixture.transport.ledgerStats().sessions() == 0, "the first session to end");
        fixture.hold(second.localUfrag(), elsewhere);
        fixture.child();
    }

    @Test
    void anActiveSessionReportsItsDataChannelsAndThePairItUses() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        StatelessAdmissionCodec.Credentials ticket = fixture.mint(30);
        fixture.hold(ticket.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        fixture.child();
        InetSocketAddress mapped = new InetSocketAddress("93.184.216.41", 61000);
        peer.path(new TransportPath(null, TransportPath.CandidateType.HOST, mapped,
                TransportPath.CandidateType.SERVER_REFLEXIVE));
        activate(peer);

        List<TransportOutcome> outcomes = poll(2);
        assertEquals(List.of(OutcomeStage.ICE_SEEN, OutcomeStage.DATA_CHANNELS_OPEN),
                outcomes.stream().map(TransportOutcome::stage).toList());
        assertEquals(mapped, outcomes.get(1).remote());
        assertEquals(TicketId.ofToken(ticket.localUfrag()), outcomes.get(1).ticketId());
    }

    @Test
    void aSessionWhoseLoginIsNeverDecidedClosesAtItsTicketsDeadline() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        fixture.hold(fixture.mint(2).localUfrag(), CLIENT);
        FakePeer undecidedPeer = fixture.lastPeer();
        NetherNetChildChannel undecided = fixture.child();
        activate(undecidedPeer);

        fixture.hold(fixture.mint(2).localUfrag(), new InetSocketAddress("203.0.113.68", 50003));
        FakePeer decidedPeer = fixture.lastPeer();
        NetherNetChildChannel decided = fixture.child();
        activate(decidedPeer);
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(decided, fixture.clientKey.getPublic()));

        assertTrue(undecided.closeFuture().await(5, TimeUnit.SECONDS), "an application must decide the login");
        Thread.sleep(300);
        assertTrue(decided.isOpen(), "a decided login outlives its ticket");
    }

    @Test
    void theProfileNamesTheIdentityTheKeyAndTheBoundPort() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        HostProfileSnapshot snapshot = fixture.transport.captureHostProfile().toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertEquals(fixture.identity.fingerprint(), snapshot.profile().dtlsFingerprint());
        assertEquals(HostFixture.KEY_ID, snapshot.profile().admissionKeyId());
        assertEquals(fixture.transport.incarnation(), snapshot.profile().incarnation());
        assertEquals(5000, snapshot.profile().sctpPort());
        assertEquals(List.of(HostCandidate.host(new InetSocketAddress("127.0.0.1", fixture.endpoint.port()))),
                snapshot.profile().candidates());
        snapshot.requireCurrent();

        fixture.transport.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertFalse(isCurrent(snapshot.guard()), "a draining endpoint is not published");
    }

    @Test
    void anAssistedJoinIsAnsweredWithOneFreshCandidateAndTheServerIdentity() throws Exception {
        String hostAnswer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\n"
                + "a=ice-ufrag:NXS1K001assisted\r\na=ice-pwd:assistedpassword0123456789\r\n"
                + "a=fingerprint:" + fixture.identity.fingerprint().value() + "\r\na=setup:active\r\na=mid:0\r\n"
                + "a=sctp-port:5000\r\na=max-message-size:262144\r\n";
        fixture.engine.answerWith(hostAnswer);
        fixture.engine.gatherWith(hostAnswer + "a=candidate:1 1 udp 2122260223 127.0.0.1 40000 typ host\r\n"
                + "a=end-of-candidates\r\n");
        fixture.start(new HostTransport.Settings(HostTransport.Publication.MAINTAINED, List.of(), false, true, false,
                TicketLedger.Limits.defaults()));
        fixture.installKey();
        InetSocketAddress stun = new InetSocketAddress("192.0.2.7", 3478);
        fixture.transport.configureStunServers(List.of(new StunServer("192.0.2.7", 3478))).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        InetSocketAddress mapped = new InetSocketAddress("93.184.216.40", fixture.endpoint.port());
        fixture.endpoint.mapStun(stun, mapped);

        AssistedJoin join = assistedJoin();
        AssistedAnswer answer = fixture.transport.assistedJoin(join, System.nanoTime() + TimeUnit.SECONDS.toNanos(10),
                OwnershipGuard.ALWAYS).toCompletableFuture().get(10, TimeUnit.SECONDS);

        FakePeer peer = fixture.lastPeer();
        assertEquals(new IceCredentials(join.localUfrag(), join.localPassword()), peer.config().localCredentials());
        assertEquals(fixture.identity.dtls(), peer.config().identity());
        assertTrue(answer.asserted(), "clients since 26.40 refuse an answer without the server's identity");
        List<String> candidates = answer.sdp().lines().filter(line -> line.startsWith("a=candidate:")).toList();
        assertEquals(List.of("a=candidate:1 1 UDP 2130706431 93.184.216.40 " + fixture.endpoint.port() + " typ srflx"),
                candidates, "one fresh mapping of the gameplay port, gathered for this join");
        assertEquals(List.of(stun), fixture.endpoint.stunMonitors());

        NetherNetChildChannel child = fixture.child();
        assertEquals(join.id(), child.attr(AdmissionPrincipal.KEY).get().ticketId().value());
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(child, fixture.clientKey.getPublic()),
                "the login is bound to the key the provider verified");
    }

    @Test
    void anAssistedJoinForAnotherIncarnationIsRefused() throws Exception {
        fixture.start(new HostTransport.Settings(HostTransport.Publication.MAINTAINED, List.of(), false, true, false,
                TicketLedger.Limits.defaults()));
        fixture.installKey();
        AssistedJoin join = assistedJoin("0".repeat(32));

        var answer = fixture.transport.assistedJoin(join, System.nanoTime() + TimeUnit.SECONDS.toNanos(10),
                OwnershipGuard.ALWAYS).toCompletableFuture();
        assertTrue(answer.handle((value, failure) -> failure != null).get(5, TimeUnit.SECONDS));
        assertTrue(fixture.engine.peers().isEmpty());
    }

    private AssistedJoin assistedJoin() {
        return assistedJoin(fixture.transport.incarnation());
    }

    private AssistedJoin assistedJoin(String incarnation) {
        String offer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\n"
                + "a=ice-ufrag:clnt\r\na=ice-pwd:clientpassword0123456789\r\n"
                + "a=fingerprint:sha-256 " + "AB:".repeat(31) + "AB\r\na=setup:actpass\r\na=mid:0\r\n"
                + "a=sctp-port:5000\r\na=max-message-size:262144\r\n"
                + "a=candidate:1 1 udp 2122260223 198.51.100.9 50000 typ host\r\n";
        return new AssistedJoin("0123456789abcdef0123456789abcdef", "instance", 1, incarnation, HostFixture.KEY_ID,
                fixture.identity.fingerprint(), System.currentTimeMillis() + 20_000, "42",
                Base64.getEncoder().encodeToString(IdentityPublicKey.canonical(fixture.clientKey.getPublic())),
                "NXS1K001assisted", "assistedpassword0123456789", offer);
    }

    private static boolean isCurrent(OwnershipGuard guard) {
        try {
            guard.requireCurrent();
            return true;
        } catch (IllegalStateException replaced) {
            return false;
        }
    }

    /** The client opens both data channels and the reliable one opens, which activates the child. */
    private void activate(FakePeer peer) throws InterruptedException {
        FakeDataLink reliable = peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        peer.remoteChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        peer.open(reliable);
        fixture.sync();
    }

    private List<TransportOutcome> poll(int count) throws InterruptedException {
        List<TransportOutcome> polled = new ArrayList<>();
        await(() -> {
            polled.addAll(fixture.transport.pollOutcomes(256));
            return polled.size() >= count;
        }, count + " outcomes");
        return polled;
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.host.HostFixture.CLIENT;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.host.HostFixture.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The NXS host side on an endpoint of the fake engine. */
@Timeout(30)
class HostTransportTest {
    /** The provider's STUN server assisted joins gather their answer candidate through. */
    private static final InetSocketAddress STUN = new InetSocketAddress("192.0.2.7", 3478);
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
    void anInstallationThatRacesCloseInstallsNothing() throws Exception {
        fixture.start(HostFixture.fixed());
        CountDownLatch copying = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        // Copying the snapshot is where an installation stalls here, as the client's thread could anywhere
        List<AdmissionKey> keys = new AbstractList<>() {
            @Override
            public AdmissionKey get(int index) {
                return new AdmissionKey(HostFixture.KEY_ID, HostFixture.SECRET);
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public Object[] toArray() {
                copying.countDown();
                try {
                    closed.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                return super.toArray();
            }
        };
        CompletableFuture<CompletionStage<Void>> installing =
                CompletableFuture.supplyAsync(() -> fixture.transport.installAdmissionKeys(keys));
        assertTrue(copying.await(5, TimeUnit.SECONDS));
        fixture.transport.close().toCompletableFuture().get(5, TimeUnit.SECONDS);
        closed.countDown();

        CompletableFuture<Void> installed = installing.get(5, TimeUnit.SECONDS).toCompletableFuture();
        assertTrue(installed.handle((ignored, failure) -> failure != null).get(5, TimeUnit.SECONDS),
                "a closed transport takes no keys");
        assertEquals(Set.of(), fixture.transport.installedKeyIds(), "no key material outlives close");
    }

    @Test
    void aHeldTicketIsTakenByAPeerWithItsCredentialsAndTheHostCertificate() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        StatelessAdmissionCodec.Credentials ticket = fixture.mint(30);

        HostFixture.Check check = fixture.hold(ticket.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        NetherNetChildChannel child = fixture.child();

        assertEquals(new IceCredentials(ticket.localUfrag(), ticket.icePwd()), peer.config().localCredentials());
        assertEquals(fixture.certificate.dtls(), peer.config().identity());
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
        assertEquals(1, fixture.answerers().size(), "neither got a peer");
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
        assertEquals(1, fixture.answerers().size(), "the endpoint's budget held for NXS too");
        await(() -> fixture.transport.ledgerStats().sessions() == 1, "the refused ticket's record to go");

        fixture.children.clear();
        fixture.lastPeer().closed(io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause.REMOTE_CLOSED);
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
    void aChildThatActivatesRightBeforeItsHandshakeDeadlineIsNotTimedOut() throws Exception {
        fixture.start(new HostTransport.Settings(HostTransport.Publication.FIXED, List.of(), true, false, false,
                new TicketLedger.Limits(8192, 1000)));
        fixture.installKey();
        fixture.hold(fixture.mint(30).localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        NetherNetChildChannel child = fixture.child();
        long pastDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1100);
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(child, fixture.clientKey.getPublic()));
        FakeDataLink reliable = peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        FakeDataLink unreliable = peer.remoteChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        AtomicBoolean activated = new AtomicBoolean();
        child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                activated.set(true);
                ctx.fireChannelActive();
            }
        });

        // The child activates while the loop is held past the deadline, so the first maintenance tick that sees it
        // active is also the first one past the deadline
        child.eventLoop().submit(() -> {
            peer.open(reliable);
            peer.open(unreliable);
            TimeUnit.NANOSECONDS.sleep(pastDeadline - System.nanoTime());
            return null;
        }).sync();
        fixture.sync();
        Thread.sleep(300);
        assertTrue(activated.get(), "the child connected before the deadline");
        assertTrue(child.isOpen(), "a child that connected in time is no stalled handshake");
    }

    @Test
    void removingNxsClosesTheChildrenWhoseLoginIsUndecidedAndKeepsThePlayers() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        fixture.hold(fixture.mint(30).localUfrag(), CLIENT);
        FakePeer undecidedPeer = fixture.lastPeer();
        NetherNetChildChannel undecided = fixture.child();
        activate(undecidedPeer);

        fixture.hold(fixture.mint(30).localUfrag(), new InetSocketAddress("203.0.113.68", 50003));
        FakePeer playerPeer = fixture.lastPeer();
        NetherNetChildChannel player = fixture.child();
        activate(playerPeer);
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(player, fixture.clientKey.getPublic()));

        assertTrue(fixture.server.removeSource(fixture.transport.source()));
        assertTrue(undecided.closeFuture().await(5, TimeUnit.SECONDS),
                "nothing would decide the login or close the child once NXS is gone");
        Thread.sleep(300);
        assertTrue(player.isOpen(), "a player whose login was accepted stays");
    }

    @Test
    void theProfileNamesTheIdentityTheKeyAndTheBoundPort() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        HostProfileSnapshot snapshot = fixture.transport.captureHostProfile().toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertEquals(fixture.fingerprint(), snapshot.profile().dtlsFingerprint());
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
        fixture.start(new HostTransport.Settings(HostTransport.Publication.MAINTAINED, List.of(), false, true, false,
                TicketLedger.Limits.defaults()));
        fixture.installKey();
        AssistedJoin join = assistedJoin();
        answerAssistedWith(join.localUfrag(), join.localPassword());

        AssistedAnswer answer = fixture.transport.assistedJoin(join, System.nanoTime() + TimeUnit.SECONDS.toNanos(10),
                OwnershipGuard.ALWAYS).toCompletableFuture().get(10, TimeUnit.SECONDS);

        FakePeer peer = fixture.lastPeer();
        assertEquals(new IceCredentials(join.localUfrag(), join.localPassword()), peer.config().localCredentials());
        assertEquals(fixture.certificate.dtls(), peer.config().identity());
        assertTrue(answer.asserted(), "clients since 26.40 refuse an answer without the server's identity");
        List<String> candidates = answer.sdp().lines().filter(line -> line.startsWith("a=candidate:")).toList();
        assertEquals(List.of("a=candidate:1 1 UDP 2130706431 93.184.216.40 " + fixture.endpoint.port() + " typ srflx"),
                candidates, "one fresh mapping of the gameplay port, gathered for this join");
        assertEquals(List.of(STUN), fixture.endpoint.stunMonitors());

        NetherNetChildChannel child = fixture.child();
        assertEquals(join.id(), child.attr(AdmissionPrincipal.KEY).get().ticketId().value());
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(child, fixture.clientKey.getPublic()),
                "the login is bound to the key the provider verified");
    }

    @Test
    void aTokenAnAssistedJoinUsedIsNotAdmittedAgainStatelessly() throws Exception {
        fixture.start(new HostTransport.Settings(HostTransport.Publication.MAINTAINED, List.of(), false, true, false,
                TicketLedger.Limits.defaults()));
        fixture.installKey();
        // The provider hands the host a real token as the assisted ufrag, which the client learns from the answer
        StatelessAdmissionCodec.Credentials ticket = fixture.mint(30);
        answerAssistedWith(ticket.localUfrag(), ticket.icePwd());
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1500);
        fixture.transport.assistedJoin(assistedJoin(fixture.transport.incarnation(), ticket.localUfrag(),
                ticket.icePwd()), deadline, OwnershipGuard.ALWAYS).toCompletableFuture().get(5, TimeUnit.SECONDS);
        FakePeer peer = fixture.lastPeer();
        fixture.child();
        peer.closed(CloseCause.REMOTE_CLOSED);
        await(() -> fixture.transport.ledgerStats().sessions() == 0, "the assisted session to end");
        // Past the join's own deadline, well before the token's
        TimeUnit.NANOSECONDS.sleep(Math.max(0, deadline - System.nanoTime()) + TimeUnit.MILLISECONDS.toNanos(300));

        HostFixture.Check replay = fixture.hold(ticket.localUfrag(), new InetSocketAddress("203.0.113.70", 50010));
        replay.rejected.get(5, TimeUnit.SECONDS);
        assertEquals(1, fixture.transport.ledgerStats().replayRejected());
        assertEquals(1, fixture.answerers().size(), "the token got no second peer");
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
        assertTrue(fixture.answerers().isEmpty());
    }

    private AssistedJoin assistedJoin() {
        return assistedJoin(fixture.transport.incarnation());
    }

    private AssistedJoin assistedJoin(String incarnation) {
        return assistedJoin(incarnation, "NXS1K001assisted", "assistedpassword0123456789");
    }

    /** Lets assisted peers answer with these credentials, through a STUN mapping of the gameplay port. */
    private void answerAssistedWith(String ufrag, String password) throws Exception {
        String hostAnswer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\n"
                + "a=ice-ufrag:" + ufrag + "\r\na=ice-pwd:" + password + "\r\n"
                + "a=fingerprint:" + fixture.fingerprint().value() + "\r\na=setup:active\r\na=mid:0\r\n"
                + "a=sctp-port:5000\r\na=max-message-size:262144\r\n";
        fixture.engine.answerWith(hostAnswer);
        fixture.engine.gatherWith(hostAnswer + "a=candidate:1 1 udp 2122260223 127.0.0.1 40000 typ host\r\n"
                + "a=end-of-candidates\r\n");
        fixture.transport.configureStunServers(List.of(new StunServer(STUN.getHostString(), STUN.getPort())))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        fixture.endpoint.mapStun(STUN, new InetSocketAddress("93.184.216.40", fixture.endpoint.port()));
    }

    private AssistedJoin assistedJoin(String incarnation, String localUfrag, String localPassword) {
        String offer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\n"
                + "a=ice-ufrag:clnt\r\na=ice-pwd:clientpassword0123456789\r\n"
                + "a=fingerprint:sha-256 " + "AB:".repeat(31) + "AB\r\na=setup:actpass\r\na=mid:0\r\n"
                + "a=sctp-port:5000\r\na=max-message-size:262144\r\n"
                + "a=candidate:1 1 udp 2122260223 198.51.100.9 50000 typ host\r\n";
        return new AssistedJoin("0123456789abcdef0123456789abcdef", "instance", 1, incarnation, HostFixture.KEY_ID,
                fixture.fingerprint(), System.currentTimeMillis() + 20_000, "42",
                Base64.getEncoder().encodeToString(IdentityPublicKey.canonical(fixture.clientKey.getPublic())),
                localUfrag, localPassword, offer);
    }

    private static boolean isCurrent(OwnershipGuard guard) {
        try {
            guard.requireCurrent();
            return true;
        } catch (IllegalStateException replaced) {
            return false;
        }
    }

    /** The client opens both data channels and both open, which activates the child. */
    private void activate(FakePeer peer) throws InterruptedException {
        FakeDataLink reliable = peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        FakeDataLink unreliable = peer.remoteChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        peer.open(reliable);
        peer.open(unreliable);
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

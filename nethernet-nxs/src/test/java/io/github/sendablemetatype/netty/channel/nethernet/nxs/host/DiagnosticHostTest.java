package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Claims;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Context;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Credentials;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Key;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticChannels;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticExchange;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.host.DiagnosticResult.Outcome;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfileSnapshot;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.StunServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.host.HostFixture.CLIENT;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.host.HostFixture.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NXS connectivity checks on an endpoint of the fake engine: the test plays the provider, which installs the policy and
 * issues permits, and the prober, whose checks and frames it hands to the host's peer.
 */
@Timeout(30)
class DiagnosticHostTest {
    private static final String PROBER_PASSWORD = "proberpassword01234567";
    private static final String PROBER_FINGERPRINT_HEX = "ab".repeat(32);
    private static final InetSocketAddress STUN = new InetSocketAddress("192.0.2.7", 3478);
    /** An assisted prober's public source. */
    private static final InetSocketAddress PUBLIC_PROBER = new InetSocketAddress("93.184.216.77", 50000);

    private final HostFixture fixture;
    private Context context;
    private Key key;
    private InetSocketAddress target;
    private long revision;

    DiagnosticHostTest() throws Exception {
        fixture = new HostFixture();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void aDirectCheckAnswersThePingOnAPeerOfItsOwnAndIsNoPlayer() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);

        HostFixture.Check check = fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        assertEquals(new IceCredentials(permit.credentials.localUfrag(), permit.credentials.icePwd()),
                peer.config().localCredentials());
        assertEquals(fixture.certificate.dtls(), peer.config().identity());
        assertEquals(fixture.endpoint, peer.config().endpoint());
        await(() -> !peer.remoteOffers().isEmpty(), "the offer the permit stands for");
        String offer = peer.remoteOffers().getFirst();
        assertTrue(offer.contains("a=ice-ufrag:" + HostFixture.CLIENT_UFRAG + "\r\n"), offer);
        assertTrue(offer.contains("a=ice-pwd:" + PROBER_PASSWORD + "\r\n"), offer);
        assertTrue(offer.contains("a=candidate:1 1 UDP 2130706431 198.51.100.9 50000 typ host\r\n"),
                "the check's source is the one candidate: " + offer);
        assertFalse(check.rejected.isDone());

        FakeDataLink reliable = connect(peer, CLIENT);
        byte[] nonce = nonce();
        peer.message(reliable, DiagnosticExchange.encode(permit.claims.attemptIdHex(), DiagnosticExchange.PING, 0,
                nonce));
        await(() -> reliable.sendCount() == 1, "the PONG");
        reliable.accept(0);
        assertArrayEquals(DiagnosticExchange.encode(permit.claims.attemptIdHex(), DiagnosticExchange.PONG, 0, nonce),
                reliable.sentBytes().getFirst());

        // The prober closes once it checked the echo
        peer.linkClosed(reliable, CloseCause.REMOTE_CLOSED);
        DiagnosticResult result = result();
        assertEquals(Outcome.PONG_SENT, result.outcome());
        assertTrue(result.success());
        assertEquals(permit.claims.attemptIdHex(), result.attemptId());
        assertEquals(permit.claims.offerDigestHex(), result.offerDigestHex());
        assertEquals(new InetSocketAddress("127.0.0.1", target.getPort()), result.selectedLocal());
        assertEquals(CLIENT, result.selectedRemote());
        assertEquals(List.of(1, 56, 1, 56), List.of(result.sentFrames(), result.sentBytes(), result.receivedFrames(),
                result.receivedBytes()));
        assertTrue(peer.terminated());
        assertTrue(fixture.children.isEmpty(), "a check is never a player");
        assertTrue(fixture.transport.pollOutcomes(256).isEmpty(), "and reports no player outcome");
    }

    @Test
    void aHostThatLingersClosesASecondAfterThePong() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        FakeDataLink reliable = connect(peer, CLIENT);
        peer.message(reliable, ping(permit));
        await(() -> reliable.sendCount() == 1, "the PONG");
        reliable.accept(0);

        assertEquals(Outcome.PONG_SENT, result().outcome(), "a prober that never closes is not waited for");
        assertTrue(peer.terminated());
    }

    @Test
    void aPongIsSentOnlyOnceTheEngineAcceptedIt() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        FakeDataLink reliable = connect(peer, CLIENT);
        peer.message(reliable, ping(permit));
        await(() -> reliable.sendCount() == 1, "the PONG");

        // Still out past the time the host lingers after a PONG, then refused
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(DiagnosticHost.LINGER_NANOS) + 300);
        assertTrue(fixture.transport.pollDiagnosticResults(1).isEmpty(), "a PONG the engine has not accepted");
        reliable.reject(0, new IOException("The engine refused the PONG"));
        assertEquals(Outcome.TRANSPORT_FAILED, result().outcome());
        assertTrue(peer.terminated());
    }

    @Test
    void withoutAPolicyADiagnosticTicketCreatesNoPeer() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        captureTarget();
        context = new Context("https://provider.example", "instance", fixture.transport.incarnation(), 1);
        key = new Key(HostFixture.KEY_ID, HostFixture.SECRET, 0, System.currentTimeMillis() + 600_000);
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);

        fixture.hold(permit.credentials.localUfrag(), CLIENT).rejected.get(5, TimeUnit.SECONDS);
        assertTrue(fixture.answerers().isEmpty());
        assertTrue(fixture.children.isEmpty());
    }

    @Test
    void anAssistedPermitArrivingAsACheckCreatesNoPeer() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.ASSISTED_PROFILE, PUBLIC_PROBER);

        fixture.hold(permit.credentials.localUfrag(), CLIENT).rejected.get(5, TimeUnit.SECONDS);
        assertTrue(fixture.answerers().isEmpty(), "only the provider's WebSocket may start an assisted check");
    }

    @Test
    void anAttemptRunsOnce() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        fixture.lastPeer().closed(CloseCause.ICE_FAILED);
        assertEquals(Outcome.TRANSPORT_FAILED, result().outcome());

        // The same permit again, and a fresh permit for the same attempt, from another source
        InetSocketAddress elsewhere = new InetSocketAddress("203.0.113.80", 50020);
        fixture.hold(permit.credentials.localUfrag(), elsewhere).rejected.get(5, TimeUnit.SECONDS);
        Credentials reissued = DiagnosticAdmissionCodec.issue(context, key, permit.claims, HostFixture.CLIENT_UFRAG,
                permit.offer, key.retireAfter(), DiagnosticAdmissionCodec.Clock.system());
        fixture.hold(reissued.localUfrag(), elsewhere).rejected.get(5, TimeUnit.SECONDS);
        assertEquals(1, fixture.answerers().size());
    }

    @Test
    void atMostFourChecksRunAtOnce() throws Exception {
        directHost();
        for (int i = 0; i < DiagnosticHost.MAX_ACTIVE; i++) {
            InetSocketAddress source = new InetSocketAddress("198.51.100." + (20 + i), 50000);
            fixture.hold(permit(DiagnosticAdmissionCodec.PROFILE, source).credentials.localUfrag(), source);
            peer(i + 1);
        }
        InetSocketAddress fifth = new InetSocketAddress("198.51.100.30", 50000);
        fixture.hold(permit(DiagnosticAdmissionCodec.PROFILE, fifth).credentials.localUfrag(), fifth)
                .rejected.get(5, TimeUnit.SECONDS);
        assertEquals(DiagnosticHost.MAX_ACTIVE, fixture.answerers().size());
    }

    @Test
    void withdrawingThePolicyEndsTheCheck() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();

        fixture.transport.disableDiagnostics().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(Outcome.WITHDRAWN, result().outcome());
        assertTrue(peer.terminated());
    }

    @Test
    void aPathOffThePermitsEndpointsSendsNoPong() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        FakeDataLink reliable = connect(peer, new InetSocketAddress("198.51.100.99", 50000));
        peer.message(reliable, ping(permit));

        assertEquals(Outcome.INVALID_PATH, result().outcome());
        assertEquals(0, reliable.sendCount());
    }

    @Test
    void aWrongPingOrAnExtraChannelFailsTheCheck() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        FakeDataLink reliable = connect(peer, CLIENT);
        peer.message(reliable, DiagnosticExchange.encode("ff".repeat(16), DiagnosticExchange.PING, 0, nonce()));
        assertEquals(Outcome.INVALID_PING, result().outcome());
        assertEquals(0, reliable.sendCount());

        InetSocketAddress source = new InetSocketAddress("198.51.100.40", 50000);
        Permit second = permit(DiagnosticAdmissionCodec.PROFILE, source);
        fixture.hold(second.credentials.localUfrag(), source);
        FakePeer other = peer(2);
        other.open(other.remoteChannel(ChannelParameters.reliable("Other")));
        assertEquals(Outcome.INVALID_PROTOCOL, result().outcome());
    }

    @Test
    void aPingSentAsTextFailsTheCheck() throws Exception {
        directHost();
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();
        FakeDataLink reliable = connect(peer, CLIENT);
        peer.text(reliable, ping(permit));

        assertEquals(Outcome.INVALID_PROTOCOL, result().outcome());
        assertEquals(0, reliable.sendCount(), "no PONG answers a text message");
    }

    @Test
    void anAssistedCheckIsAnsweredWithOneCandidateAndNoIdentity() throws Exception {
        assistedHost();
        Permit permit = permit(DiagnosticAdmissionCodec.ASSISTED_PROFILE, PUBLIC_PROBER);
        AssistedJoin join = assistedJoin(permit, PUBLIC_PROBER);

        AssistedAnswer answer = fixture.transport.assistedJoin(join, System.nanoTime() + TimeUnit.SECONDS.toNanos(20),
                OwnershipGuard.ALWAYS).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertFalse(answer.asserted(), "the diagnostic profile forbids an identity assertion");
        assertEquals(List.of("a=candidate:1 1 UDP 2130706431 93.184.216.40 " + fixture.endpoint.port() + " typ srflx"),
                answer.sdp().lines().filter(line -> line.startsWith("a=candidate:")).toList());
        FakePeer peer = fixture.lastPeer();
        assertEquals(new IceCredentials(join.localUfrag(), join.localPassword()), peer.config().localCredentials());
        assertTrue(peer.remoteOffers().getFirst().contains("93.184.216.77 50000"), "a public offer keeps its candidate");

        // A check for the join's ufrag held before its peer is ready is left to that peer
        HostFixture.Check early = fixture.hold(join.localUfrag(), PUBLIC_PROBER);
        fixture.sync();
        assertFalse(early.rejected.isDone());

        FakeDataLink reliable = connect(peer, PUBLIC_PROBER);
        peer.message(reliable, ping(permit));
        await(() -> reliable.sendCount() == 1, "the PONG");
        reliable.accept(0);
        peer.closed(CloseCause.REMOTE_CLOSED);
        DiagnosticResult result = result();
        assertEquals(Outcome.PONG_SENT, result.outcome());
        assertTrue(result.target().assisted());
        assertTrue(fixture.children.isEmpty());
    }

    @Test
    void anAssistedCheckWhoseAuthorityLapsesEnds() throws Exception {
        assistedHost();
        Permit permit = permit(DiagnosticAdmissionCodec.ASSISTED_PROFILE, PUBLIC_PROBER);
        boolean[] current = {true};
        OwnershipGuard guard = () -> {
            if (!current[0]) {
                throw new IllegalStateException("The provider socket was replaced");
            }
        };
        fixture.transport.assistedJoin(assistedJoin(permit, PUBLIC_PROBER), System.nanoTime()
                + TimeUnit.SECONDS.toNanos(20), guard).toCompletableFuture().get(10, TimeUnit.SECONDS);

        current[0] = false;
        assertEquals(Outcome.WITHDRAWN, result().outcome());
    }

    @Test
    void anAssistedCheckForAnotherHostOrADirectPermitIsRefused() throws Exception {
        assistedHost();
        Permit direct = permit(DiagnosticAdmissionCodec.PROFILE, PUBLIC_PROBER);
        Permit assisted = permit(DiagnosticAdmissionCodec.ASSISTED_PROFILE, PUBLIC_PROBER);
        AssistedJoin valid = assistedJoin(assisted, PUBLIC_PROBER);
        AssistedJoin otherHost = new AssistedJoin(valid.id(), "other-instance", valid.generation(),
                valid.incarnation(), valid.keyId(), valid.hostFingerprint(), valid.expiresAt(), "0", null,
                valid.localUfrag(), valid.localPassword(), valid.offer());
        for (AssistedJoin join : List.of(assistedJoin(direct, PUBLIC_PROBER), otherHost)) {
            ExecutionException refused = assertThrows(ExecutionException.class, () -> fixture.transport
                    .assistedJoin(join, System.nanoTime() + TimeUnit.SECONDS.toNanos(20), OwnershipGuard.ALWAYS)
                    .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalArgumentException.class, refused.getCause());
        }
        assertTrue(fixture.answerers().isEmpty());
    }

    @Test
    void anAssistedCheckWithNothingToAnswerWithCreatesNoPeer() throws Exception {
        fixture.start(new HostTransport.Settings(HostTransport.Publication.MAINTAINED, List.of(), false, true, false,
                TicketLedger.Limits.defaults()));
        fixture.installKey();
        revision = fixture.transport.captureHostProfile().toCompletableFuture().get(5, TimeUnit.SECONDS)
                .candidateRevision();
        target = new InetSocketAddress("93.184.216.40", fixture.endpoint.port());
        install(Set.of(DiagnosticHostPolicy.Endpoint.assisted(4, revision)));
        Permit permit = permit(DiagnosticAdmissionCodec.ASSISTED_PROFILE, PUBLIC_PROBER);

        // No public host candidate and no provider STUN server to learn a mapping through
        ExecutionException refused = assertThrows(ExecutionException.class, () -> fixture.transport
                .assistedJoin(assistedJoin(permit, PUBLIC_PROBER), System.nanoTime() + TimeUnit.SECONDS.toNanos(20),
                        OwnershipGuard.ALWAYS).toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, refused.getCause());
        assertTrue(fixture.answerers().isEmpty());
    }

    @Test
    void aPolicyMustDescribeTheHostAsItStands() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        captureTarget();
        Context own = new Context("https://provider.example", "instance", fixture.transport.incarnation(), 1);
        Key policyKey = new Key(HostFixture.KEY_ID, HostFixture.SECRET, 0, System.currentTimeMillis() + 600_000);
        long expiresAt = System.currentTimeMillis() + 120_000;
        var published = endpoint(target, revision);
        for (var policy : List.of(
                new DiagnosticHostPolicy(new Context(own.providerOrigin(), own.hostId(), "00".repeat(16), 1),
                        List.of(policyKey), Set.of(published), expiresAt),
                new DiagnosticHostPolicy(own, List.of(policyKey), Set.of(endpoint(target, revision + 1)), expiresAt),
                new DiagnosticHostPolicy(own, List.of(policyKey),
                        Set.of(endpoint(new InetSocketAddress("127.0.0.2", target.getPort()), revision)), expiresAt),
                new DiagnosticHostPolicy(own, List.of(policyKey), Set.of(DiagnosticHostPolicy.Endpoint.assisted(4,
                        revision)), expiresAt))) {
            ExecutionException refused = assertThrows(ExecutionException.class, () -> fixture.transport
                    .configureDiagnostics(policy, System.nanoTime() + TimeUnit.SECONDS.toNanos(60))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalArgumentException.class, refused.getCause());
        }
        assertThrows(ExecutionException.class, () -> fixture.transport.configureDiagnostics(
                new DiagnosticHostPolicy(own, List.of(policyKey), Set.of(published), expiresAt),
                System.nanoTime() + TimeUnit.MINUTES.toNanos(6)).toCompletableFuture().get(5, TimeUnit.SECONDS),
                "a heartbeat's policy lasts five minutes at most");
        fixture.transport.configureDiagnostics(new DiagnosticHostPolicy(own, List.of(policyKey), Set.of(published),
                expiresAt), System.nanoTime() + TimeUnit.SECONDS.toNanos(60)).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
    }

    @Test
    void closingTheTransportEndsItsChecksAndWaitsForTheirPeers() throws Exception {
        directHost();
        fixture.engine.holdPeerClose(true);
        Permit permit = permit(DiagnosticAdmissionCodec.PROFILE, CLIENT);
        fixture.hold(permit.credentials.localUfrag(), CLIENT);
        FakePeer peer = fixture.lastPeer();

        CompletableFuture<Void> closed = fixture.transport.close().toCompletableFuture();
        await(peer::closeCalled, "the check's peer to close");
        fixture.sync();
        assertFalse(closed.isDone(), "the peer is still tearing down");
        peer.finishClose();
        closed.get(5, TimeUnit.SECONDS);
        assertEquals(Outcome.HOST_CLOSED, result().outcome());
    }

    // ---- Helpers ----

    /** A host serving direct checks of its bound endpoint under a policy the test installed. */
    private void directHost() throws Exception {
        fixture.start(HostFixture.fixed());
        fixture.installKey();
        captureTarget();
        install(Set.of(endpoint(target, revision)));
    }

    /** A host serving assisted checks, whose answers name a STUN mapping of the gameplay port. */
    private void assistedHost() throws Exception {
        fixture.start(new HostTransport.Settings(HostTransport.Publication.MAINTAINED, List.of(), false, true, false,
                TicketLedger.Limits.defaults()));
        fixture.installKey();
        fixture.transport.configureStunServers(List.of(new StunServer(STUN.getHostString(), STUN.getPort())))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        fixture.endpoint.mapStun(STUN, new InetSocketAddress("93.184.216.40", fixture.endpoint.port()));
        HostProfileSnapshot snapshot = fixture.transport.captureHostProfile().toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        revision = snapshot.candidateRevision();
        // Only a direct permit, which this host refuses, names a target
        target = new InetSocketAddress("93.184.216.40", fixture.endpoint.port());
        install(Set.of(DiagnosticHostPolicy.Endpoint.assisted(4, revision)));
    }

    private void captureTarget() throws Exception {
        HostProfileSnapshot snapshot = fixture.transport.captureHostProfile().toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        HostCandidate probe = snapshot.probeCandidates().getFirst();
        target = probe.endpoint();
        revision = snapshot.candidateRevision();
    }

    private void install(Set<DiagnosticHostPolicy.Endpoint> endpoints) throws Exception {
        context = new Context("https://provider.example", "instance", fixture.transport.incarnation(), 1);
        key = new Key(HostFixture.KEY_ID, HostFixture.SECRET, 0, System.currentTimeMillis() + 600_000);
        fixture.transport.configureDiagnostics(new DiagnosticHostPolicy(context, List.of(key), endpoints,
                System.currentTimeMillis() + 120_000), System.nanoTime() + TimeUnit.SECONDS.toNanos(120))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static DiagnosticHostPolicy.Endpoint endpoint(InetSocketAddress endpoint, long revision) {
        return new DiagnosticHostPolicy.Endpoint(4, DiagnosticAdmissionCodec.address(4,
                endpoint.getAddress().getHostAddress()), endpoint.getPort(), revision);
    }

    /** A permit for one attempt, as the provider issues it for the prober's exact offer. */
    private record Permit(Claims claims, byte[] offer, Credentials credentials) {
    }

    private Permit permit(int profile, InetSocketAddress prober) throws Exception {
        String offer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\n"
                + "a=setup:actpass\r\na=ice-ufrag:" + HostFixture.CLIENT_UFRAG + "\r\na=ice-pwd:" + PROBER_PASSWORD
                + "\r\na=fingerprint:sha-256 " + "AB:".repeat(31) + "AB\r\na=sctp-port:5000\r\n"
                + "a=max-message-size:262144\r\na=candidate:1 1 UDP 2130706431 "
                + prober.getAddress().getHostAddress() + " " + prober.getPort() + " typ host\r\na=end-of-candidates\r\n";
        byte[] bytes = offer.getBytes(StandardCharsets.UTF_8);
        boolean assisted = profile == DiagnosticAdmissionCodec.ASSISTED_PROFILE;
        Claims claims = new Claims((System.currentTimeMillis() / 1000 + 30) * 1000, PROBER_FINGERPRINT_HEX,
                PROBER_PASSWORD, HexFormat.of().formatHex(random(16)),
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), revision, 4,
                assisted ? "00".repeat(16) : DiagnosticAdmissionCodec.address(4, target.getAddress().getHostAddress()),
                assisted ? 0 : target.getPort(), profile);
        Credentials credentials = DiagnosticAdmissionCodec.issue(context, key, claims, HostFixture.CLIENT_UFRAG, bytes,
                key.retireAfter(), DiagnosticAdmissionCodec.Clock.system());
        String answer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\n"
                + "a=setup:active\r\na=ice-ufrag:" + credentials.localUfrag() + "\r\na=ice-pwd:" + credentials.icePwd()
                + "\r\na=fingerprint:" + fixture.fingerprint().value() + "\r\na=sctp-port:5000\r\n"
                + "a=max-message-size:262144\r\n";
        // The host's answer as libwebrtc gives it: the permit's credentials and the host certificate
        fixture.engine.answerWith(answer);
        return new Permit(claims, bytes, credentials);
    }

    private AssistedJoin assistedJoin(Permit permit, InetSocketAddress prober) {
        return new AssistedJoin(permit.claims.attemptIdHex(), context.hostId(), context.generation(),
                context.incarnation(), key.keyId(), fixture.fingerprint(), permit.claims.expiresAt(), "0", null,
                permit.credentials.localUfrag(), permit.credentials.icePwd(),
                new String(permit.offer, StandardCharsets.UTF_8));
    }

    /** The prober's side of the connection: the pair, the connection up and both channels open. */
    private FakeDataLink connect(FakePeer peer, InetSocketAddress remote) throws Exception {
        await(() -> !peer.remoteOffers().isEmpty(), "the host to apply the offer");
        peer.path(new TransportPath(new InetSocketAddress("127.0.0.1", fixture.endpoint.port()),
                TransportPath.CandidateType.HOST, remote, TransportPath.CandidateType.PEER_REFLEXIVE));
        peer.state(PeerState.CONNECTED);
        FakeDataLink reliable = peer.remoteChannel(DiagnosticChannels.RELIABLE);
        FakeDataLink unreliable = peer.remoteChannel(DiagnosticChannels.UNRELIABLE);
        peer.open(reliable);
        peer.open(unreliable);
        fixture.sync();
        return reliable;
    }

    private static byte[] ping(Permit permit) {
        return DiagnosticExchange.encode(permit.claims.attemptIdHex(), DiagnosticExchange.PING, 0, nonce());
    }

    private static byte[] nonce() {
        return random(32);
    }

    private static byte[] random(int bytes) {
        byte[] value = new byte[bytes];
        new SecureRandom().nextBytes(value);
        return value;
    }

    /** The host's {@code number}th peer, once it exists; each answers with the latest permit's credentials. */
    private FakePeer peer(int number) throws InterruptedException {
        await(() -> fixture.answerers().size() >= number, "peer " + number);
        return fixture.answerers().get(number - 1);
    }

    /** The next result the host reports. */
    private DiagnosticResult result() throws InterruptedException {
        List<DiagnosticResult> results = new ArrayList<>();
        await(() -> {
            results.addAll(fixture.transport.pollDiagnosticResults(1));
            return !results.isEmpty();
        }, "a diagnostic result");
        return results.getFirst();
    }
}

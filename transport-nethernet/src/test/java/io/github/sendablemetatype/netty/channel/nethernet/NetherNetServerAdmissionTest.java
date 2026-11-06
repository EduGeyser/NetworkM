package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherServerThrottle;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerThrottle;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.OfferPolicy;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.github.sendablemetatype.netty.util.nethernet.TrustedIssuer;
import io.netty.channel.EventLoop;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.ANSWER;
import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.OFFER;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

/** The endpoint's one admission stage: identity, throttle and budget, the same for every source. */
@Timeout(15)
class NetherNetServerAdmissionTest {
    private final ManualCheckExecutor checks = new ManualCheckExecutor();
    private EndpointHarness harness = new EndpointHarness();
    private final List<Integer> refusedStatuses = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    private void reportRefusals() {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_METRICS, new NetherServerMetrics() {
            @Override
            public void joinRefused(int httpStatus) {
                refusedStatuses.add(httpStatus);
            }
        });
    }

    // Identity.

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anOfferWithoutAnAssertionIsRefusedByDefaultOnEveryPath(boolean trickles) throws Exception {
        reportRefusals();
        harness.bind();
        FakeJoinSource source = new FakeJoinSource(trickles);
        harness.server.addSource(source);

        FakeJoinSource.Reply reply = source.offer(1, OFFER);

        reply.assertRefused(JoinRefusal.IDENTITY_MISSING);
        harness.sync();
        assertTrue(harness.engine.peers().isEmpty());
        assertEquals(1, reply.finished.get());
        assertEquals(List.of(401), refusedStatuses);
        assertEquals(0, harness.server.gate().pending());
    }

    @Test
    void aFailedProofIsRefused() throws Exception {
        harness.bind();
        FakeJoinSource source = new FakeJoinSource(true);
        harness.server.addSource(source);
        source.offer(1, retailOffer().replace(DIGEST, OTHER_DIGEST)).assertRefused(JoinRefusal.IDENTITY_INVALID);
        assertTrue(harness.engine.peers().isEmpty());
    }

    @Test
    void provenKeysAreAdmittedByDefaultAndBindTheLogin() throws Exception {
        List<String> accepted = new CopyOnWriteArrayList<>();
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_METRICS, new NetherServerMetrics() {
            @Override
            public void connectionAccepted(String networkId, boolean verifiedIdentity) {
                accepted.add(networkId + " " + verifiedIdentity);
            }
        });
        harness.bind();
        FakeJoinSource source = new FakeJoinSource(true);
        harness.server.addSource(source);

        source.offer(1, educationOffer());
        NetherNetChildChannel education = harness.child();
        assertEquals(OfferCheckResult.Level.KEY_PROVEN, education.getOfferCheck().level());
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(education, CLIENT_KEY.getPublic()));

        source.offer(2, retailOffer());
        assertEquals(OfferCheckResult.Level.ATTESTED, harness.child().getOfferCheck().level());
        assertEquals(List.of("remote-1 false", "remote-2 true"), accepted);
    }

    @Test
    void eachSourceHasItsOwnPolicy() throws Exception {
        harness.bind();
        FakeJoinSource strict = new FakeJoinSource(true);
        harness.server.addSource(strict, OfferPolicy.REQUIRE_ATTESTED);
        FakeJoinSource lenient = harness.source(true);

        strict.offer(1, educationOffer()).assertRefused(JoinRefusal.IDENTITY_INVALID);
        lenient.offer(2, OFFER);
        NetherNetChildChannel child = harness.child();
        assertEquals(OfferCheckResult.Level.NO_ASSERTION, child.getOfferCheck().level());
        assertEquals(TransportIdentityBinding.State.NONE, TransportIdentityBinding.state(child),
                "a child that proved no key still carries a binding");
    }

    @Test
    void anUnreachableTrustSourceRefusesOnlyWhereAttestationIsRequired() throws Exception {
        // Port 1 refuses the connection, so the key set fetch fails at once
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of(
                TrustedIssuer.jwks(URI.create("https://127.0.0.1:1/keys"), TrustedIssuer.MINECRAFT_ISSUER,
                        TrustedIssuer.MINECRAFT_AUDIENCE))));
        harness.bind();
        FakeJoinSource lenient = new FakeJoinSource(false);
        harness.server.addSource(lenient);
        FakeJoinSource strict = new FakeJoinSource(false);
        harness.server.addSource(strict, OfferPolicy.REQUIRE_ATTESTED);

        lenient.offer(1, retailOffer());
        assertEquals(OfferCheckResult.Level.KEY_PROVEN, harness.child().getOfferCheck().level(),
                "the proven key is enough by default");
        strict.offer(2, retailOffer()).assertRefused(JoinRefusal.TRUST_SOURCE_UNREACHABLE);
    }

    @Test
    void theTrustedIssuersKeysAreFetchedAtBind() throws Exception {
        try (ServerSocket keyServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            keyServer.setSoTimeout(5000);
            harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of(
                    TrustedIssuer.jwks(URI.create("https://127.0.0.1:" + keyServer.getLocalPort() + "/keys"),
                            TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE))));
            harness.bind();
            try (Socket fetch = keyServer.accept()) {
                assertNotNull(fetch, "the bind fetched the key set before any join");
            }
        }
    }

    @Test
    void anUnusableOfferIsRefusedBeforeItsCheck() throws Exception {
        useManualChecks();
        harness.bind();
        harness.source(true).offer(1, "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=max-message-size:1\r\n")
                .assertRefused(JoinRefusal.INTERNAL);
        assertTrue(checks.tasks.isEmpty());
        assertTrue(harness.engine.peers().isEmpty());
    }

    // The check's scheduling.

    @Test
    void aCheckPastItsDeadlineIsRefusedAndAdmitsNothingLater() throws Exception {
        useManualChecks();
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS, 50);
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(1, educationOffer());

        reply.assertRefused(JoinRefusal.NO_ANSWER_IN_TIME);
        assertTrue(((Future<?>) checks.tasks.peek()).isCancelled(), "the check is withdrawn");
        checks.runNext();
        harness.sync();
        assertTrue(harness.engine.peers().isEmpty());
        assertEquals(0, harness.server.gate().sessions());
    }

    @Test
    void aCheckThatFinishesPastItsDeadlineAdmitsNothingWhenTheLoopRanLate() throws Exception {
        useManualChecks();
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS, 100);
        harness.bind();
        FakeJoinSource source = harness.source(true);
        EventLoop loop = harness.server.eventLoop();
        CountDownLatch resume = new CountDownLatch(1);
        CompletableFuture<FakeJoinSource.Reply> reply = new CompletableFuture<>();
        try {
            // The join's admission runs first, then the loop stalls past the check's deadline
            loop.submit(() -> {
                reply.complete(source.offer(1, OFFER));
                loop.execute(() -> {
                    try {
                        resume.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }).sync();
            checks.awaitQueued();
            Thread.sleep(250);
            checks.runNext();
        } finally {
            resume.countDown();
        }

        reply.get().assertRefused(JoinRefusal.NO_ANSWER_IN_TIME);
        harness.sync();
        assertTrue(harness.engine.peers().isEmpty());
        assertEquals(0, harness.server.gate().sessions());
    }

    @Test
    void aFullCheckQueueRefusesTheJoinForCapacity() throws Exception {
        useManualChecks();
        harness.bind();
        FakeJoinSource source = harness.source(true);
        source.offer(1, educationOffer());
        source.offer(2, educationOffer()).assertRefused(JoinRefusal.CAPACITY);
        assertEquals(1, checks.tasks.size());

        checks.runNext();
        assertEquals("1", harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get());
    }

    @Test
    void aJoinAbandonedDuringItsCheckCreatesNothing() throws Exception {
        useManualChecks();
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(false).offer(1, educationOffer());
        harness.sync();
        assertEquals(1, harness.server.gate().pending());

        reply.abandoned.complete(null);
        harness.sync();
        checks.runNext();
        harness.sync();

        assertTrue(harness.engine.peers().isEmpty());
        assertEquals(List.of(), reply.drain(), "a client that left gets nothing");
        assertEquals(1, reply.finished.get());
        assertEquals(0, harness.server.gate().sessions());
    }

    @Test
    void closingTheEndpointRefusesJoinsStillBeingCheckedAndGivesBackWhatTheyHold() throws Exception {
        useManualChecks();
        RecordingThrottle throttle = new RecordingThrottle();
        throttle.allow = true;
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_THROTTLE, throttle);
        harness.bind();
        FakeJoinSource.Reply reply = harness.source(true).offer(1, OFFER);
        Future<?> queued = checks.awaitQueued();
        AdmissionGate gate = harness.server.gate();

        harness.server.close().sync();

        reply.assertRefused(JoinRefusal.CAPACITY);
        assertEquals(1, reply.finished.get());
        assertTrue(queued.isCancelled(), "the check is withdrawn");
        assertEquals(List.of(FakeJoinSource.CLIENT), throttle.closed, "a throttle another endpoint reuses keeps no count");
        assertEquals(0, gate.pending());
        assertEquals(0, gate.sessions());
    }

    @Test
    void removingASourceRefusesItsJoinsStillBeingCheckedAtOnce() throws Exception {
        useManualChecks();
        RecordingThrottle throttle = new RecordingThrottle();
        throttle.allow = true;
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_THROTTLE, throttle);
        harness.bind();
        FakeJoinSource source = harness.source(true);
        FakeJoinSource.Reply reply = source.offer(1, OFFER);
        Future<?> queued = checks.awaitQueued();

        assertTrue(harness.server.removeSource(source));

        reply.assertRefused(JoinRefusal.CAPACITY);
        assertTrue(queued.isCancelled());
        assertEquals(List.of(FakeJoinSource.CLIENT), throttle.closed);
        assertEquals(0, harness.server.gate().pending());
        checks.runNext();
        harness.sync();
        assertTrue(harness.engine.peers().isEmpty());
        assertEquals(1, reply.finished.get());
    }

    // The shared budget.

    @Test
    void thePendingBudgetIsSharedByEverySourceAndAdmitsAnswersIt() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS, 1);
        harness.bind();
        FakeJoinSource xbox = harness.source(true);
        FakeJoinSource http = harness.source(false);
        SourcedAddress anyone = new SourcedAddress(new InetSocketAddress("192.0.2.1", 1), AddressSource.SIGNALING, null);
        assertTrue(http.sink.admits(anyone));

        xbox.offer(1, OFFER);
        FakePeer peer = harness.negotiating();
        assertFalse(http.sink.admits(anyone));
        assertFalse(xbox.sink.admits(anyone));
        http.offer(2, OFFER).assertRefused(JoinRefusal.CAPACITY);

        peer.completeAnswer(ANSWER);
        harness.activate(peer);
        assertTrue(http.sink.admits(anyone), "an active connection is no longer pending");
        http.offer(3, OFFER);
        harness.child();
        assertEquals("3", harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get());
    }

    @Test
    void aSessionHoldsItsBudgetUntilItsTerminationCompletes() throws Exception {
        harness.engine.holdPeerClose(true);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_MAX_SESSIONS, 1);
        harness.bind();
        FakeJoinSource source = harness.source(true);
        source.offer(1, OFFER);
        NetherNetChildChannel child = harness.child();
        FakePeer peer = harness.negotiating();

        child.close().sync();
        harness.sync();
        assertEquals(1, harness.server.gate().sessions(), "the peer's teardown still runs");
        source.offer(2, OFFER).assertRefused(JoinRefusal.CAPACITY);

        peer.finishClose();
        harness.sync();
        assertEquals(0, harness.server.gate().sessions());
        source.offer(3, OFFER);
        assertEquals("3", harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get());
    }

    @Test
    void aClosedHandshakeStaysPendingUntilItsTerminationCompletes() throws Exception {
        harness.engine.holdPeerClose(true);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS, 1);
        harness.bind();
        FakeJoinSource source = harness.source(true);
        source.offer(1, OFFER);
        NetherNetChildChannel child = harness.child();
        FakePeer peer = harness.negotiating();

        child.close().sync();
        harness.sync();
        assertEquals(1, harness.server.gate().pending(), "the peer that never went active still tears down");
        source.offer(2, OFFER).assertRefused(JoinRefusal.CAPACITY);

        peer.finishClose();
        harness.sync();
        assertEquals(0, harness.server.gate().pending());
        source.offer(3, OFFER);
        assertEquals("3", harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get());
    }

    // The throttle.

    @Test
    void aThrottledJoinIsRefusedAndAnAcceptedOneIsReleasedAtTermination() throws Exception {
        RecordingThrottle throttle = new RecordingThrottle();
        List<InetSocketAddress> throttled = new CopyOnWriteArrayList<>();
        harness.engine.holdPeerClose(true);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_THROTTLE, throttle)
                .option(NetherChannelOption.NETHER_SERVER_METRICS, new NetherServerMetrics() {
                    @Override
                    public void joinThrottled(InetSocketAddress address) {
                        throttled.add(address);
                    }
                });
        harness.bind();
        FakeJoinSource source = harness.source(false);

        source.offer(1, OFFER).assertRefused(JoinRefusal.CAPACITY);
        assertEquals(List.of(FakeJoinSource.CLIENT), throttled);
        assertTrue(harness.engine.peers().isEmpty(), "a refused join creates no peer");

        throttle.allow = true;
        source.offer(2, OFFER);
        NetherNetChildChannel child = harness.child();
        FakePeer peer = harness.negotiating();
        child.close().sync();
        harness.sync();
        assertEquals(List.of(), throttle.closed, "released only when the teardown completed");
        peer.finishClose();
        harness.sync();
        assertEquals(List.of(FakeJoinSource.CLIENT), throttle.closed);
    }

    @Test
    void joinsFromAnUnknownAddressAreCheckedButNotThrottled() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_THROTTLE, new RecordingThrottle());
        harness.bind();
        FakeJoinSource xbox = new FakeJoinSource(true);
        harness.server.addSource(xbox);

        xbox.offer(1, OFFER, SourcedAddress.unknown(), null).assertRefused(JoinRefusal.IDENTITY_MISSING);
        assertTrue(xbox.sink.admits(SourcedAddress.unknown()));
        xbox.offer(2, educationOffer(), SourcedAddress.unknown(), null);
        assertEquals("2", harness.child().attr(NetherNetChildChannel.CONNECTION_ID).get());
    }

    @Test
    void theDefaultThrottleCountsPerAddressAndAdmitsAnswersIt() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_THROTTLE,
                new DefaultNetherServerThrottle(1, Duration.ofSeconds(60), 30));
        harness.bind();
        FakeJoinSource source = harness.source(false);
        SourcedAddress client = new SourcedAddress(FakeJoinSource.CLIENT, AddressSource.SIGNALING, null);
        SourcedAddress other = new SourcedAddress(new InetSocketAddress("192.0.2.1", 1), AddressSource.SIGNALING, null);

        source.offer(1, OFFER);
        harness.child();
        assertFalse(source.sink.admits(client));
        assertTrue(source.sink.admits(other));
        source.offer(2, OFFER).assertRefused(JoinRefusal.CAPACITY);
    }

    private void useManualChecks() {
        harness.close();
        harness = new EndpointHarness(() -> checks);
    }

    /** Refuses every join until told to allow them, and records the releases. */
    private static final class RecordingThrottle implements NetherServerThrottle {
        volatile boolean allow;
        final List<InetSocketAddress> closed = new CopyOnWriteArrayList<>();

        @Override
        public boolean accept(InetSocketAddress address) {
            return allow;
        }

        @Override
        public void closed(InetSocketAddress address) {
            closed.add(address);
        }
    }
}

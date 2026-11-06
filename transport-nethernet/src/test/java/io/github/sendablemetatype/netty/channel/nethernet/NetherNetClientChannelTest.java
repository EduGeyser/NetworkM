package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherNetAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.ice.ManualResolver;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidateIntake;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetClientSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Signal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingFailure;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.OfferFixtures;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.github.sendablemetatype.netty.util.nethernet.ServerTrust;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.security.KeyPair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NetherNetClientChannelTest {
    private static final String OFFER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:0\r\n";
    private static final String ANSWER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "a=max-message-size:262144\r\n";

    @Test
    void theDataChannelsUseTheReferenceParameters() {
        try (Harness h = new Harness()) {
            h.connect();
            h.signaling.connections.getFirst().complete(List.of());
            h.pump();
            FakePeer peer = h.engine.lastPeer();
            assertEquals(PeerConfig.Role.OFFERER, peer.config().role());
            assertEquals(List.of(
                    ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL),
                    ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL)),
                    peer.links().stream().map(FakeDataLink::parameters).toList());
        }
    }

    @Test
    void theOfferGoesOutBeforeTrickledCandidatesAndTheAnswerAndCandidatesReachThePeer() {
        try (Harness h = new Harness()) {
            h.connect();
            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            peer.localCandidate("candidate:1", "0");
            h.pump();
            String id = h.connectionId();
            assertEquals(List.of("CONNECTREQUEST " + id + " " + OFFER, "CANDIDATEADD " + id + " candidate:1"),
                    h.signaling.sent);

            // Ahead of the answer, a candidate takes the offer's mid, which the answer mirrors
            String remote = "candidate:9 1 udp 2122194687 203.0.113.10 5000 typ host";
            h.signal(NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD, id, remote);
            h.signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, id, ANSWER);
            h.pump();
            assertEquals(List.of(new FakePeer.RemoteCandidate(remote, "0")), peer.remoteCandidates());
            assertEquals(List.of(ANSWER), peer.remoteAnswers());
            assertEquals(256 * 262143, h.channel.maxMessageSize());
        }
    }

    /**
     * The server decides which addresses the client checks, so its candidates go through the
     * client's intake: no unusable addresses, no TCP, at most the cap from the answer and as many
     * again from trickling, per attempt.
     */
    @Test
    void eachAttemptAppliesOnlyTheServerCandidatesItsIntakeTakes() {
        List<String> offered = List.of(
                "candidate:1 1 udp 2122194687 203.0.113.10 5000 typ host",
                "candidate:3 1 tcp 2122194687 203.0.113.10 9 typ host tcptype passive",
                "candidate:4 1 udp 2122194687 224.0.0.251 5002 typ host",
                "candidate:5 1 udp 2122194687 255.255.255.255 5003 typ host",
                "candidate:6 1 udp 2122194687 0.0.0.0 5004 typ host");
        try (Harness h = new Harness()) {
            h.connect();
            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            h.pump();
            String id = h.connectionId();

            StringBuilder answer = new StringBuilder(ANSWER).append("a=mid:0\r\n");
            offered.forEach(candidate -> answer.append("a=").append(candidate).append("\r\n"));
            h.signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, id, answer.toString());
            offered.forEach(candidate -> h.signal(NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD, id, candidate));
            h.pump();

            assertEquals(List.of(ANSWER + "a=mid:0\r\na=" + offered.getFirst() + "\r\n"), peer.remoteAnswers());
            assertEquals(List.of(new FakePeer.RemoteCandidate(offered.getFirst(), "0")), peer.remoteCandidates());

            trickleMany(h, id, 40);
            assertEquals(RemoteCandidateIntake.MAX_CANDIDATES, peer.remoteCandidates().size());

            // The next attempt takes its own share
            h.advance(100);
            FakePeer retry = h.start();
            retry.completeOffer(OFFER);
            h.pump();
            trickleMany(h, h.connectionId(), 40);
            assertEquals(RemoteCandidateIntake.MAX_CANDIDATES, retry.remoteCandidates().size());
        }
    }

    /**
     * A server may name its host. The client looks each name up off the event loop and hands the
     * engine the address, after the answer and only if the address rules take it; the engine never
     * sees a name.
     */
    @Test
    void namedServerCandidatesReachThePeerAsTheirAddressesOnceLookedUp() {
        try (Harness h = new Harness()) {
            h.connect();
            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            h.pump();
            String id = h.connectionId();

            h.signal(NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD, id,
                    "candidate:1 1 udp 2122194687 early.example 5000 typ host");
            h.signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, id, ANSWER + "a=mid:0\r\n"
                    + "a=candidate:2 1 udp 2122194687 node.cluster.local 5001 typ host\r\n"
                    + "a=candidate:3 1 udp 2122194687 203.0.113.10 5002 typ host\r\n"
                    + "a=candidate:4 1 udp 2122194687 0xe00000fb 5003 typ host\r\n");
            h.pump();
            assertEquals(List.of(ANSWER + "a=mid:0\r\na=candidate:3 1 udp 2122194687 203.0.113.10 5002 typ host\r\n"),
                    peer.remoteAnswers());
            assertEquals(List.of(), peer.remoteCandidates(), "nothing until the lookups are done");

            h.resolver.answer("node.cluster.local", "10.0.0.7");
            h.resolver.answer("0xe00000fb", "224.0.0.251");
            h.resolver.fail("early.example");
            assertEquals(List.of(), peer.remoteCandidates(), "the results come back through the event loop");
            h.pump();

            assertEquals(List.of(new FakePeer.RemoteCandidate("candidate:2 1 udp 2122194687 10.0.0.7 5001 typ host", "0")),
                    peer.remoteCandidates());
        }
    }

    @Test
    void aLookupForARetiredAttemptAppliesNothing() {
        try (Harness h = new Harness()) {
            h.connect();
            FakePeer first = h.start();
            first.completeOffer(OFFER);
            h.pump();
            h.signal(NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD, h.connectionId(),
                    "candidate:1 1 udp 2122194687 slow.example 5000 typ host");
            h.pump();

            h.advance(100);
            FakePeer retry = h.start();
            retry.completeOffer(OFFER);
            h.pump();
            h.resolver.answer("slow.example", "203.0.113.10");
            h.pump();

            assertNotSame(first, retry);
            assertEquals(List.of(), first.remoteCandidates());
            assertEquals(List.of(), retry.remoteCandidates());
        }
    }

    private static void trickleMany(Harness h, String id, int count) {
        for (int i = 0; i < count; i++) {
            h.signal(NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD, id,
                    "candidate:" + (100 + i) + " 1 udp 2122194687 198.51.100.7 " + (6000 + i) + " typ host");
        }
        h.pump();
    }

    @Test
    void signalingThatDoesNotTrickleGetsTheGatheredOfferAndNoCandidates() {
        try (Harness h = new Harness()) {
            h.signaling.trickles = false;
            h.connect();
            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            peer.localCandidate("candidate:1", "0");
            h.pump();
            assertTrue(h.signaling.sent.isEmpty(), "the offer waits for gathering");

            String gathered = OFFER + "a=candidate:1\r\na=end-of-candidates\r\n";
            peer.gatheringComplete(gathered);
            h.pump();
            assertEquals(List.of("CONNECTREQUEST " + h.connectionId() + " " + gathered), h.signaling.sent);
        }
    }

    @Test
    void anAttemptMakesRoomForTheSignalingsAnswerWaitUntilTheAnswerArrives() {
        try (Harness h = new Harness()) {
            h.signaling.trickles = false;
            h.signaling.answerTimeout = Duration.ofMillis(1000);
            ChannelFuture connect = h.connect();
            h.advance(1099);
            assertEquals(1, h.signaling.connections.size(), "no retry while the signaling connects");

            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            peer.gatheringComplete(OFFER);
            h.pump();
            h.advance(1099);
            assertEquals(1, h.signaling.connections.size(), "no retry while the answer can still arrive");

            h.signaling.signalHandlers.getLast().onSignal(
                    new Signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, h.connectionId(), ANSWER));
            h.pump();
            h.advance(99);
            assertFalse(connect.isDone());
            h.advance(1);
            assertEquals("The handshake timed out; gave up after 1 attempt",
                    assertInstanceOf(ConnectException.class, connect.cause()).getMessage(),
                    "once answered, the handshake timeout applies");
            assertEquals(1, h.signaling.connections.size(), "signaling that does not trickle gets one attempt");
        }
    }

    @Test
    void aConnectMakesTheConfiguredAttemptsTheFirstIncluded() {
        try (Harness h = new Harness()) {
            AtomicInteger retries = new AtomicInteger();
            h.channel.config().setOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() {
                @Override
                public void handshakeRetry() {
                    retries.incrementAndGet();
                }
            });
            assertEquals(3, h.channel.config().getOption(NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS),
                    "the default");
            ChannelFuture connect = h.connect();
            h.advance(100);
            h.advance(100);
            assertEquals(3, h.signaling.connections.size());
            assertFalse(connect.isDone());
            h.advance(100);
            assertEquals("The handshake timed out; gave up after 3 attempts",
                    assertInstanceOf(ConnectException.class, connect.cause()).getMessage());
            assertEquals(3, h.signaling.connections.size());
            assertEquals(2, retries.get());
        }
    }

    @Test
    void aPeerFailingOverSignalingThatDoesNotTrickleEndsTheOnlyAttempt() {
        try (Harness h = new Harness()) {
            h.signaling.trickles = false;
            ChannelFuture connect = h.connect();
            h.start().closed(CloseCause.ICE_FAILED);
            h.pump();
            assertEquals("The peer ended during the handshake (ICE_FAILED); gave up after 1 attempt",
                    assertInstanceOf(ConnectException.class, connect.cause()).getMessage());
            assertEquals(1, h.signaling.connections.size());
            assertFalse(h.channel.isOpen());
        }
    }

    @Test
    void aSignalingFailureEndsTheConnectWithItsReasonAndFailure() {
        try (Harness h = new Harness()) {
            List<NetherConnectionFailure> failures = new ArrayList<>();
            h.channel.config().setOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() {
                @Override
                public void connectionFailed(NetherConnectionFailure reason) {
                    failures.add(reason);
                }
            });
            ChannelFuture connect = h.connect();
            h.start().completeOffer(OFFER);
            h.pump();

            h.signaling.signalHandlers.getLast().onFailure(h.connectionId(),
                    new SignalingFailure(NetherConnectionFailure.CONNECT_ERROR, 16, "HTTP 401"));
            h.pump();

            ConnectException failure = assertInstanceOf(ConnectException.class, connect.cause());
            assertTrue(failure.getMessage().endsWith(": HTTP 401"), failure.getMessage());
            assertEquals(List.of(NetherConnectionFailure.CONNECT_ERROR), failures);
            assertFalse(h.channel.isOpen());
            h.advance(1000);
            assertEquals(1, h.signaling.connections.size(), "no retry after the server refused");
        }
    }

    @Test
    void theOfferCarriesTheClientIdentityAsAProvenKey() throws Exception {
        try (Harness h = new Harness()) {
            KeyPair key = OfferFixtures.CLIENT_KEY;
            h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_IDENTITY, ClientIdentity.of(key,
                    OfferFixtures.educationToken(OfferFixtures.educationClaims(key.getPublic()), key), ""));
            h.connect();
            h.start().completeOffer(OfferFixtures.offer(null, OfferFixtures.DIGEST));
            h.pump();

            String offer = h.signaling.sent.getFirst().split(" ", 3)[2];
            OfferCheckResult result = new OfferCheck(List.of()).check(offer);
            assertEquals(OfferCheckResult.Level.KEY_PROVEN, result.level());
        }
    }

    @Test
    void aPinnedServerTrustRefusesAnAnswerWithoutTheIdentityAndMakesNoOtherAttempt() {
        try (Harness h = new Harness()) {
            List<NetherConnectionFailure> failures = new ArrayList<>();
            h.channel.config().setOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() {
                @Override
                public void connectionFailed(NetherConnectionFailure reason) {
                    failures.add(reason);
                }
            });
            h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_SERVER_TRUST,
                    ServerTrust.pinnedTo(ServerIdentity.ephemeral("self").publicKey()));
            ChannelFuture connect = h.connect();
            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            h.pump();

            h.signaling.signalHandlers.getLast().onSignal(
                    new Signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, h.connectionId(), ANSWER));
            h.pump();
            ConnectException failure = assertInstanceOf(ConnectException.class, connect.cause());
            assertEquals("The server's answer carries no identity", failure.getMessage());
            assertEquals(ServerTrust.Refusal.MISSING,
                    assertInstanceOf(ServerTrust.Refused.class, failure.getCause()).refusal());
            assertEquals(List.of(NetherConnectionFailure.SERVER_IDENTITY_REFUSED), failures);
            assertTrue(peer.remoteAnswers().isEmpty(), "a refused answer never reaches the engine");
            assertFalse(h.channel.isOpen());
            h.advance(1000);
            assertEquals(1, h.signaling.connections.size());
        }
    }

    @Test
    void aPinnedServerTrustPassesTheAnswerOfThePinnedServer() throws Exception {
        try (Harness h = new Harness()) {
            ServerIdentity server = ServerIdentity.ephemeral("self");
            h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_SERVER_TRUST,
                    ServerTrust.pinnedTo(server.publicKey()));
            ChannelFuture connect = h.connect();
            FakePeer peer = h.start();
            peer.completeOffer(OFFER);
            h.pump();

            String answer = server.augmentAnswer(OfferFixtures.offer(null, OfferFixtures.DIGEST));
            h.signaling.signalHandlers.getLast().onSignal(
                    new Signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, h.connectionId(), answer));
            h.pump();
            assertEquals(List.of(answer), peer.remoteAnswers());
            assertFalse(connect.isDone());
            assertTrue(h.channel.isOpen());
        }
    }

    @Test
    void aDuplicateConnectCannotReplaceTheOriginalPromiseOrTarget() {
        try (Harness h = new Harness()) {
            ChannelFuture original = h.connect();
            SocketAddress target = h.channel.remoteAddress();
            ChannelFuture duplicate = h.channel.connect(new NetherNetAddress("other-target"));
            h.pump();
            assertInstanceOf(ConnectionPendingException.class, duplicate.cause());
            assertFalse(original.isDone());
            assertSame(target, h.channel.remoteAddress());
            assertEquals(1, h.signaling.connections.size());
            h.channel.close();
            h.pump();
            assertInstanceOf(ClosedChannelException.class, original.cause());
        }
    }

    @Test
    void anOldSignalingFailureCannotCloseTheRetryOrCancelItsTimeout() {
        try (Harness h = new Harness()) {
            ChannelFuture original = h.connect();
            h.advance(100);
            assertEquals(2, h.signaling.connections.size());
            h.signaling.connections.get(0).completeExceptionally(new IllegalStateException("Old attempt failed"));
            h.pump();
            assertTrue(h.channel.isOpen());
            assertFalse(original.isDone());
            h.advance(99);
            assertEquals(2, h.signaling.connections.size());
            h.advance(1);
            assertEquals(3, h.signaling.connections.size());
            h.advance(100);
            assertInstanceOf(ConnectException.class, original.cause());
            assertFalse(h.channel.isOpen());
        }
    }

    @Test
    void anOldSignalingSuccessCannotCreateThePeerOfTheRetry() {
        try (Harness h = new Harness()) {
            ChannelFuture original = h.connect();
            h.advance(100);
            h.signaling.connections.get(0).complete(List.of());
            h.pump();
            assertTrue(h.engine.peers().isEmpty());
            assertFalse(original.isDone());
            h.signaling.connections.get(1).complete(List.of());
            h.pump();
            assertEquals(1, h.engine.peers().size(), "the current attempt still creates its peer");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aLinkageFailureFailsTheCurrentHandshakeImmediately(boolean missingClass) {
        try (Harness h = new Harness()) {
            LinkageError failure = missingClass ? new NoClassDefFoundError("Native binding unavailable")
                    : new UnsatisfiedLinkError("Native library unavailable");
            h.engine.failCreation(failure);
            ChannelFuture connect = h.connect();
            h.signaling.connections.getFirst().complete(List.of());
            h.pump();
            assertInstanceOf(ConnectException.class, connect.cause());
            assertSame(failure, connect.cause().getCause());
            assertFalse(h.channel.isOpen());
            assertEquals(1, h.signaling.closes);
            h.advance(1000);
            assertEquals(1, h.signaling.connections.size(), "linkage failures must not wait for retries");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aSignalingSuccessQueuedBeforeOrCompletedAfterCloseCannotCreateAPeer(boolean queuedBeforeClose) {
        try (Harness h = new Harness()) {
            ChannelFuture original = h.connect();
            if (queuedBeforeClose) {
                h.signaling.connections.getFirst().complete(List.of());
            }
            h.channel.close();
            if (!queuedBeforeClose) {
                h.signaling.connections.getFirst().complete(List.of());
            }
            h.pump();
            assertTrue(h.engine.peers().isEmpty());
            assertInstanceOf(ClosedChannelException.class, original.cause());
            assertEquals(1, h.signaling.closes);
        }
    }

    @Test
    void notFoundCallbacksBelongToTheAttemptThatRegisteredThem() {
        try (Harness h = new Harness()) {
            ChannelFuture original = h.connect();
            h.advance(100);
            h.signaling.notFoundHandlers.get(0).onNotFound("Old attempt not found");
            h.pump();
            assertTrue(h.channel.isOpen());
            assertFalse(original.isDone());
            h.signaling.notFoundHandlers.get(1).onNotFound("Current attempt not found");
            h.pump();
            assertInstanceOf(ConnectException.class, original.cause());
            assertFalse(h.channel.isOpen());
        }
    }

    @Test
    void aSynchronousSignalingFailureCompletesTheConnectPromise() {
        try (Harness h = new Harness()) {
            IllegalStateException failure = new IllegalStateException("Signaling rejected connect");
            h.signaling.connectFailure = failure;
            ChannelFuture connect = h.connect();
            assertInstanceOf(ConnectException.class, connect.cause());
            assertSame(failure, connect.cause().getCause());
            assertFalse(h.channel.isOpen());
            assertEquals(1, h.signaling.closes);
        }
    }

    @Test
    void aRetryCleanupFailureDoesNotLeaveTheConnectPromisePending() {
        try (Harness h = new Harness()) {
            ChannelFuture connect = h.connect();
            IllegalStateException failure = new IllegalStateException("Failed to unregister old attempt");
            h.signaling.removeFailure = failure;
            h.advance(100);
            assertInstanceOf(ConnectException.class, connect.cause());
            assertSame(failure, connect.cause().getCause());
            assertFalse(h.channel.isOpen());
        }
    }

    @Test
    void messagesBeforeAndDuringOpenArePreserved() {
        try (Harness h = new Harness()) {
            ChannelFuture connect = h.connect();
            FakePeer peer = h.start();
            FakeDataLink reliable = peer.links().getFirst();
            peer.message(reliable, (byte) 0, (byte) 42);
            h.pump();
            assertTrue(h.events.isEmpty());
            peer.open(reliable);
            peer.message(reliable, (byte) 0, (byte) 43);
            h.pump();
            assertEquals(List.of("active", "data:42", "data:43"), h.events);
            assertTrue(connect.isSuccess());
            assertInstanceOf(AlreadyConnectedException.class,
                    h.channel.connect(new NetherNetAddress("other-target")).cause());
            h.signaling.connections.getFirst().completeExceptionally(new IllegalStateException("Late signaling result"));
            h.signaling.notFoundHandlers.getFirst().onNotFound("Late not-found result");
            h.pump();
            assertTrue(h.channel.isActive());
        }
    }

    @Test
    void aRetryDiscardsEarlyMessagesAndIgnoresTheOldPeer() {
        try (Harness h = new Harness()) {
            // Keeps the old peer's events flowing after its close, as a slow teardown would.
            h.engine.holdPeerClose(true);
            h.connect();
            FakePeer old = h.start();
            FakeDataLink oldReliable = old.links().getFirst();
            old.message(oldReliable, (byte) 0, (byte) 42);
            assertTrue(h.allocator.metric().usedHeapMemory() > 0);
            h.advance(100);
            assertEquals(0, h.allocator.metric().usedHeapMemory());
            assertTrue(old.closeCalled(), "the retry closes the old attempt's peer");
            old.open(oldReliable);
            h.pump();
            assertTrue(h.events.isEmpty(), "the old attempt cannot activate the channel");

            FakePeer current = h.start();
            FakeDataLink reliable = current.links().getFirst();
            current.message(reliable, (byte) 0, (byte) 44);
            current.open(reliable);
            h.pump();
            assertEquals(List.of("active", "data:44"), h.events);
            assertEquals(0, h.allocator.metric().usedHeapMemory());
        }
    }

    @Test
    void closeDiscardsEarlyMessagesAndIgnoresALaterOpen() {
        try (Harness h = new Harness()) {
            h.engine.holdPeerClose(true);
            h.connect();
            FakePeer peer = h.start();
            FakeDataLink reliable = peer.links().getFirst();
            peer.message(reliable, (byte) 0, (byte) 42);
            h.channel.close();
            peer.open(reliable);
            h.pump();
            assertFalse(h.channel.isActive());
            assertTrue(h.events.isEmpty());
            assertEquals(0, h.allocator.metric().usedHeapMemory());
            assertTrue(peer.closeCalled());
        }
    }

    @Test
    void anOwnedEngineIsCreatedOnlyWhenAPeerIsNeededAndClosedWithTheChannel() throws Exception {
        EmbeddedChannel clock = new EmbeddedChannel();
        FakeSignaling signaling = new FakeSignaling();
        AtomicInteger creations = new AtomicInteger();
        FakeEngine engine = new FakeEngine();
        NetherNetClientChannel channel = new NetherNetClientChannel(() -> {
            creations.incrementAndGet();
            return engine;
        }, signaling);
        try {
            clock.eventLoop().register(channel).sync();
            channel.connect(new NetherNetAddress("target"));
            clock.runPendingTasks();
            assertEquals(0, creations.get());
            signaling.connections.getFirst().complete(List.of());
            clock.runPendingTasks();
            assertEquals(1, creations.get());
            channel.close().sync();
            assertEquals(1, engine.closeCalls());
            assertEquals(1, signaling.closes);
        } finally {
            channel.close().syncUninterruptibly();
            clock.finishAndReleaseAll();
        }
    }

    @Test
    void aSharedEngineIsNotClosedWithTheChannel() {
        try (Harness h = new Harness()) {
            h.connect();
            h.start();
            h.channel.close();
            h.pump();
            assertEquals(0, h.engine.closeCalls());
            assertTrue(h.engine.lastPeer().closeCalled());
        }
    }

    @Test
    void invalidTimeoutAndAttemptValuesAreRejectedWhileOneAttemptWorks() {
        try (Harness h = new Harness()) {
            assertThrows(IllegalArgumentException.class,
                    () -> h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 0));
            assertThrows(IllegalArgumentException.class,
                    () -> h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, -1));
            assertThrows(IllegalArgumentException.class,
                    () -> h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS, 0));
            h.channel.config().setOption(NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS, 1);
            ChannelFuture connect = h.connect();
            h.advance(100);
            assertInstanceOf(ConnectException.class, connect.cause());
            assertEquals(1, h.signaling.connections.size());
        }
    }

    @Test
    void unreliableMessagesWaitForActivationAndApplicationReads() {
        try (Harness h = new Harness()) {
            h.channel.config().setAutoRead(false);
            ChannelFuture connect = h.connect();
            FakePeer peer = h.start();
            peer.message(peer.links().get(1), (byte) 0, (byte) 42);
            h.pump();
            assertFalse(connect.isDone());
            assertTrue(h.events.isEmpty());

            peer.open(peer.links().getFirst());
            h.pump();
            assertTrue(connect.isSuccess());
            assertEquals(List.of("active"), h.events);

            h.channel.read();
            h.pump();
            assertEquals(List.of("active", "unreliable:42"), h.events);
            assertEquals(0, h.allocator.metric().usedHeapMemory());
        }
    }

    @Test
    void anInvalidAnswerLimitFailsAtOnceButAStaleAnswerCannotFailARetry() {
        try (Harness h = new Harness()) {
            ChannelFuture connect = h.connect();
            h.start().completeOffer(OFFER);
            h.pump();
            String oldId = h.connectionId();
            h.advance(100);
            String sdp = "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=max-message-size:1\r\n";
            h.signaling.signalHandlers.get(0).onSignal(
                    new Signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, oldId, sdp));
            h.pump();
            assertFalse(connect.isDone());
            h.start().completeOffer(OFFER);
            h.pump();
            String currentId = h.connectionId();
            assertNotEquals(oldId, currentId);
            h.signaling.signalHandlers.get(1).onSignal(
                    new Signal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE, currentId, sdp));
            h.pump();
            assertInstanceOf(ConnectException.class, connect.cause());
            assertInstanceOf(IllegalArgumentException.class, connect.cause().getCause());
            assertFalse(h.channel.isOpen());
            h.advance(1000);
            assertEquals(2, h.signaling.connections.size());
            assertEquals(1, h.signaling.closes);
        }
    }

    @Test
    void aPeerFailingDuringTheHandshakeRetries() {
        try (Harness h = new Harness()) {
            ChannelFuture connect = h.connect();
            FakePeer peer = h.start();
            peer.closed(CloseCause.ICE_FAILED);
            h.pump();
            assertEquals(2, h.signaling.connections.size());
            assertFalse(connect.isDone());
            assertTrue(peer.closeCalled());
        }
    }

    @Test
    void aPeerFailingAfterConnectEndsTheConnectionWithConnectionLost() {
        try (Harness h = new Harness()) {
            h.connect();
            FakePeer peer = h.start();
            peer.open(peer.links().getFirst());
            h.pump();
            assertTrue(h.channel.isActive());
            peer.closed(CloseCause.ICE_FAILED);
            h.pump();
            assertEquals(List.of("active", "CONNECTION_LOST", "inactive"), h.events);
            assertFalse(h.channel.isOpen());
            assertEquals(1, h.signaling.connections.size(), "no retry once connected");
        }
    }

    @Test
    void aClientReportsWhereItDialedAndWhereEachAttemptConnected() {
        try (Harness h = new Harness()) {
            assertSame(TransportMetrics.NONE, h.channel.transportMetrics().toCompletableFuture().join(),
                    "no peer before the connect");
            InetSocketAddress server = new InetSocketAddress("192.0.2.10", 19132);
            h.channel.connect(server);
            h.pump();
            assertEquals(new SourcedAddress(server, AddressSource.SIGNALING, null), h.channel.signaledAddress());

            FakePeer peer = h.start();
            InetSocketAddress media = new InetSocketAddress("192.0.2.11", 50000);
            TransportPath path = new TransportPath(null, TransportPath.CandidateType.HOST, media,
                    TransportPath.CandidateType.SERVER_REFLEXIVE);
            peer.path(path);
            h.pump();
            assertEquals(Optional.of(path), h.channel.selectedPath());
            assertEquals(new SourcedAddress(media, AddressSource.ICE, null), h.channel.reportedAddress());
            assertEquals(media, h.channel.remoteAddress());

            // The attempt times out; the next one has neither a pair nor, until its signaling connects, a peer.
            h.advance(100);
            assertEquals(2, h.signaling.connections.size());
            assertEquals(Optional.empty(), h.channel.selectedPath());
            assertEquals(Optional.empty(), h.channel.mediaAddress());
            assertEquals(server, h.channel.remoteAddress());
            assertSame(TransportMetrics.NONE, h.channel.transportMetrics().toCompletableFuture().join());
        }
    }

    /**
     * The engine thread found the attempt current, but the attempt is retried before the event
     * loop takes the path in: the loop checks the attempt again, so the next one stays empty.
     */
    @Test
    void aPathTheRetiredAttemptReportedReachesNothingOfTheNextAttempt() {
        try (Harness h = new Harness()) {
            InetSocketAddress server = new InetSocketAddress("192.0.2.10", 19132);
            h.channel.connect(server);
            h.pump();
            FakePeer peer = h.start();

            peer.path(new TransportPath(null, TransportPath.CandidateType.HOST,
                    new InetSocketAddress("192.0.2.11", 50000), TransportPath.CandidateType.SERVER_REFLEXIVE));
            h.advance(100);

            assertEquals(2, h.signaling.connections.size(), "the attempt was retried");
            assertEquals(Optional.empty(), h.channel.selectedPath());
            assertEquals(Optional.empty(), h.channel.mediaAddress());
            assertEquals(new SourcedAddress(server, AddressSource.SIGNALING, null), h.channel.reportedAddress());
            assertEquals(server, h.channel.remoteAddress());
        }
    }

    @Test
    void aClientThatDialedANetworkIdKeepsItAndReportsWhereTheServerWas() {
        try (Harness h = new Harness()) {
            h.connect();
            assertEquals(AddressSource.UNKNOWN, h.channel.signaledAddress().source());
            FakePeer peer = h.start();
            TransportMetrics metrics = new TransportMetrics(Optional.of(Duration.ofMillis(9)), Optional.empty());
            peer.setMetrics(metrics);
            assertSame(metrics, h.channel.transportMetrics().toCompletableFuture().join());

            InetSocketAddress relay = new InetSocketAddress("198.51.100.4", 3478);
            peer.path(new TransportPath(null, TransportPath.CandidateType.HOST, relay,
                    TransportPath.CandidateType.RELAY));
            h.pump();
            assertEquals(new SourcedAddress(relay, AddressSource.TURN, null), h.channel.reportedAddress());
            assertEquals(new NetherNetAddress("target"), h.channel.remoteAddress());
        }
    }

    private static final class Harness implements AutoCloseable {
        private final EmbeddedChannel clock = new EmbeddedChannel();
        private final FakeSignaling signaling = new FakeSignaling();
        private final FakeEngine engine = new FakeEngine();
        private final ManualResolver resolver = new ManualResolver();
        private final NetherNetClientChannel channel = new NetherNetClientChannel(engine, signaling, resolver);
        private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        private final List<String> events = new ArrayList<>();

        private Harness() {
            clock.freezeTime();
            channel.config().setAllocator(allocator);
            channel.config().setOption(NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 100);
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    events.add("active");
                }

                @Override
                public void channelInactive(ChannelHandlerContext ctx) {
                    events.add("inactive");
                }

                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                    events.add(String.valueOf(event));
                }

                @Override
                public void channelRead(ChannelHandlerContext ctx, Object message) {
                    try {
                        if (message instanceof NetherNetPacket packet) {
                            events.add("unreliable:" + packet.content().readUnsignedByte());
                        } else {
                            events.add("data:" + ((ByteBuf) message).readUnsignedByte());
                        }
                    } finally {
                        ReferenceCountUtil.release(message);
                    }
                }
            });
            clock.eventLoop().register(channel).syncUninterruptibly();
            pump();
        }

        private ChannelFuture connect() {
            ChannelFuture connect = channel.connect(new NetherNetAddress("target"));
            pump();
            return connect;
        }

        /** Completes the current attempt's signaling, which creates its peer. */
        private FakePeer start() {
            signaling.connections.getLast().complete(List.of());
            pump();
            return engine.lastPeer();
        }

        /** A signal from the server, as the newest attempt's handler receives it. */
        private void signal(String type, String id, String data) {
            signaling.signalHandlers.getLast().onSignal(new Signal(type, id, data));
        }

        /** The connection ID of the newest connect request. */
        private String connectionId() {
            String request = signaling.sent.stream().filter(signal -> signal.startsWith("CONNECTREQUEST"))
                    .reduce((first, second) -> second).orElseThrow();
            return request.split(" ", 3)[1];
        }

        private void pump() {
            clock.runPendingTasks();
            clock.checkException();
        }

        private void advance(long millis) {
            clock.advanceTimeBy(millis, TimeUnit.MILLISECONDS);
            clock.runScheduledPendingTasks();
            pump();
        }

        @Override
        public void close() {
            channel.close().syncUninterruptibly();
            clock.runPendingTasks();
            clock.finishAndReleaseAll();
        }
    }

    /** Every channel closes its signaling, so channels from one factory must not share one. */
    @Test
    void channelsFromOneFactoryEachGetTheirOwnSignaling() {
        List<FakeSignaling> created = new ArrayList<>();
        var factory = NetherNetChannelFactory.client(new FakeEngine(), () -> {
            FakeSignaling signaling = new FakeSignaling();
            created.add(signaling);
            return signaling;
        });

        factory.newChannel();
        factory.newChannel();

        assertEquals(2, created.size());
        assertNotSame(created.get(0), created.get(1));
    }

    private static final class FakeSignaling implements NetherNetClientSignaling {
        private final List<CompletableFuture<List<IceServer>>> connections = new ArrayList<>();
        private final List<NotFoundHandler> notFoundHandlers = new ArrayList<>();
        private final List<SignalHandler> signalHandlers = new ArrayList<>();
        private final List<String> sent = new ArrayList<>();
        private RuntimeException connectFailure;
        private RuntimeException removeFailure;
        private int closes;
        private boolean trickles = true;
        private Duration answerTimeout = Duration.ZERO;

        @Override
        public CompletableFuture<List<IceServer>> connect(SocketAddress remoteAddress) {
            if (connectFailure != null) {
                throw connectFailure;
            }
            CompletableFuture<List<IceServer>> connection = new CompletableFuture<>();
            connections.add(connection);
            return connection;
        }

        @Override public void setNotFoundHandler(NotFoundHandler handler) { notFoundHandlers.add(handler); }
        @Override public void sendSignal(String targetNetworkId, String data) { sent.add(data); }
        @Override public void setSignalHandler(String connectionId, String remoteNetworkId, SignalHandler handler) { signalHandlers.add(handler); }
        @Override
        public void removeSignalHandler(String connectionId) {
            RuntimeException failure = removeFailure;
            removeFailure = null;
            if (failure != null) {
                throw failure;
            }
        }
        @Override public boolean trickles() { return trickles; }
        @Override public Duration answerTimeout() { return answerTimeout; }
        @Override public String getLocalNetworkId() { return "local"; }
        @Override public void close() { closes++; }
    }
}

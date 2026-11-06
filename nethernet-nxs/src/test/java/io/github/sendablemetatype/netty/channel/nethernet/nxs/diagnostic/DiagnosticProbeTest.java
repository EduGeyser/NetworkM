package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeIceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Context;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Credentials;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Key;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The prober on the fake engine: the test completes the probe peer's offer and gathering with the peer's own
 * credentials, and plays the provider's signaling.
 */
@Timeout(20)
class DiagnosticProbeTest {
    private static final Context CONTEXT = new Context("https://provider.example", "probe-host", "cd".repeat(16), 1);
    private static final Key KEY = new Key("D001", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", 0,
            System.currentTimeMillis() + 600_000);
    private static final String HOST_FINGERPRINT = "ef".repeat(32);
    private static final InetSocketAddress TARGET = new InetSocketAddress("93.184.216.40", 19132);
    private static final InetSocketAddress BIND = new InetSocketAddress("10.0.0.5", 40000);

    private final FakeEngine engine = new FakeEngine();
    private final DefaultEventExecutor executor = new DefaultEventExecutor();
    private final AtomicBoolean authorized = new AtomicBoolean(true);

    @AfterEach
    void tearDown() {
        executor.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void anAnswerThatIsNotTheHostsSendsNoCheckAndCleansUp() throws Exception {
        List<String> modes = List.of("malformed", "pin", "oversize", "withdraw", "elsewhere");
        for (int i = 0; i < modes.size(); i++) {
            String mode = modes.get(i);
            authorized.set(true);
            DiagnosticProbe.Job job = job(20);
            DiagnosticProbe probe = probe(job);
            CompletionStage<DiagnosticProbe.Result> running = probe.run(request -> {
                String answer = answer(request, job, mode.equals("pin") ? "00".repeat(32) : HOST_FINGERPRINT,
                        mode.equals("elsewhere") ? new InetSocketAddress("93.184.216.41", 19132) : TARGET);
                if (mode.equals("malformed")) {
                    answer = answer.replace("a=mid:0", "a=mid:1");
                } else if (mode.equals("oversize")) {
                    answer = "x".repeat(DiagnosticSdp.MAX_BYTES + 1);
                } else if (mode.equals("withdraw")) {
                    authorized.set(false);
                }
                return CompletableFuture.completedFuture(answer);
            });
            FakePeer peer = gather(i + 1);

            DiagnosticProbe.Result result = running.toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(mode.equals("withdraw") ? DiagnosticProbe.Reason.WITHDRAWN : DiagnosticProbe.Reason.ANSWER,
                    result.reason(), mode);
            assertFalse(result.answerVerified(), mode);
            assertNull(result.attemptedRemote(), mode);
            assertTrue(result.cleanupComplete(), mode);
            assertTrue(peer.remoteAnswers().isEmpty(), "no answer reached the engine: " + mode);
            assertTrue(peer.terminated(), mode);
            assertTrue(engine.endpoints().getLast().closed(), "the probe's endpoint closes with it");
        }
    }

    @Test
    void theOfferIsTheGatheredOneWithThePermitsClaims() throws Exception {
        DiagnosticProbe.Job job = job(20);
        CompletableFuture<DiagnosticProbe.Request> requested = new CompletableFuture<>();
        DiagnosticProbe probe = probe(job);
        probe.run(request -> {
            requested.complete(request);
            return new CompletableFuture<>();
        });
        FakePeer peer = gather();
        DiagnosticProbe.Request request = requested.get(5, TimeUnit.SECONDS);

        IceCredentials credentials = peer.config().localCredentials();
        assertEquals(credentials.ufrag(), request.ufrag());
        assertEquals(credentials.password(), request.claims().clientIcePwd());
        assertEquals(job.attemptIdHex(), request.claims().attemptIdHex());
        assertEquals(job.expiresAt(), request.claims().expiresAt());
        assertEquals(DiagnosticAdmissionCodec.PROFILE, request.claims().profile());
        assertEquals(PeerConfig.Role.OFFERER, peer.config().role());
        assertEquals(BIND.getPort(), peer.config().endpoint().port());
        assertEquals(List.of(BIND.getAddress()), peer.config().candidates().addresses());
        DiagnosticSdp.offer(request.offer(), request.claims(), request.ufrag());
        probe.cancel();
    }

    @Test
    void cancellingWhileTheSignalingIsOutClosesThePeerAndDropsTheAnswer() throws Exception {
        DiagnosticProbe probe = probe(job(20));
        CompletableFuture<String> answer = new CompletableFuture<>();
        CompletableFuture<Void> asked = new CompletableFuture<>();
        CompletionStage<DiagnosticProbe.Result> running = probe.run(request -> {
            asked.complete(null);
            return answer;
        });
        FakePeer peer = gather();
        asked.get(5, TimeUnit.SECONDS);

        probe.cancel();
        DiagnosticProbe.Result result = running.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(DiagnosticProbe.Reason.CANCELLED, result.reason());
        assertTrue(answer.isCancelled());
        assertTrue(peer.terminated());
        probe.termination().toCompletableFuture().get(1, TimeUnit.SECONDS);
        assertThrows(IllegalStateException.class, () -> probe.run(request -> answer), "a probe runs once");
    }

    @Test
    void signalingThatNeverAnswersEndsAtTheJobsDeadline() throws Exception {
        DiagnosticProbe probe = probe(job(2));
        CompletableFuture<String> answer = new CompletableFuture<>();
        CompletionStage<DiagnosticProbe.Result> running = probe.run(request -> answer);
        gather();

        assertEquals(DiagnosticProbe.Reason.EXPIRED, running.toCompletableFuture().get(5, TimeUnit.SECONDS).reason());
        assertTrue(answer.isCancelled());
    }

    @Test
    void aVerifiedAnswerThatNeverConnectsIsATransportFailure() throws Exception {
        DiagnosticProbe.Job job = job(2);
        DiagnosticProbe probe = probe(job);
        CompletionStage<DiagnosticProbe.Result> running = probe.run(request ->
                CompletableFuture.completedFuture(answer(request, job, HOST_FINGERPRINT, TARGET)));
        FakePeer peer = gather();
        HostFixtureWait.until(() -> !peer.remoteAnswers().isEmpty(), "the answer to reach the engine");

        DiagnosticProbe.Result result = running.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(DiagnosticProbe.Reason.TRANSPORT, result.reason());
        assertTrue(result.answerVerified());
        assertEquals(TARGET, result.attemptedRemote());
        assertFalse(result.transportEstablished());
    }

    @Test
    void aDirectProbeBehindNatChecksFromTheMappingTheHostSaw() throws Exception {
        // Bound to a private address, the probe reaches the public target through NAT; libwebrtc then reports the
        // mapping the host's check responses named as the pair's local peer-reflexive candidate
        InetSocketAddress mapping = new InetSocketAddress("93.184.216.99", 61000);
        DiagnosticProbe.Result result = probeOver(new TransportPath(mapping, CandidateType.PEER_REFLEXIVE, TARGET,
                CandidateType.HOST), 1);
        assertEquals(DiagnosticProbe.Reason.COMPLETE, result.reason());
        assertEquals(mapping, result.selectedLocal());
        assertEquals(TARGET, result.selectedRemote());

        // A mapping that is no destination, and a host address the probe was not bound to, are not its endpoint
        for (TransportPath path : List.of(
                new TransportPath(new InetSocketAddress("10.0.0.77", 61000), CandidateType.PEER_REFLEXIVE, TARGET,
                        CandidateType.HOST),
                new TransportPath(new InetSocketAddress("10.0.0.6", 40000), CandidateType.HOST, TARGET,
                        CandidateType.HOST))) {
            assertEquals(DiagnosticProbe.Reason.SELECTED_PATH, probeOver(path, engine.peers().size() + 1).reason(),
                    path.toString());
        }
    }

    @Test
    void aPongSentAsTextFailsTheProbe() throws Exception {
        DiagnosticProbe.Result result = probeOver(new TransportPath(BIND, CandidateType.HOST, TARGET,
                CandidateType.HOST), 1, true);
        assertEquals(DiagnosticProbe.Reason.PROTOCOL, result.reason());
        assertFalse(result.pingVerified());
    }

    @Test
    void cancellingOrExpiringWhileTheEndpointOpensWaitsForItToClose() throws Exception {
        engine.holdEndpointOpen(true);
        engine.holdEndpointClose(true);
        for (boolean cancel : List.of(true, false)) {
            DiagnosticProbe probe = probe(job(cancel ? 20 : 2));
            CompletableFuture<DiagnosticProbe.Result> running = probe.run(request -> new CompletableFuture<>())
                    .toCompletableFuture();
            int opened = cancel ? 1 : 2;
            HostFixtureWait.until(() -> engine.endpoints().size() == opened, "the probe to open its endpoint");
            FakeIceEndpoint endpoint = engine.endpoints().getLast();
            if (cancel) {
                probe.cancel();
                executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            } else {
                Thread.sleep(2_500);
            }
            CompletableFuture<Void> terminated = probe.termination().toCompletableFuture();
            assertFalse(running.isDone(), "the endpoint is still opening");
            assertFalse(terminated.isDone(), "the endpoint is still opening");

            endpoint.finishOpen();
            HostFixtureWait.until(endpoint::closed, "the late endpoint to close");
            assertFalse(terminated.isDone(), "the endpoint is still closing");
            assertTrue(engine.peers().isEmpty(), "no peer is created on a late endpoint");

            endpoint.finishClose();
            DiagnosticProbe.Result result = running.get(5, TimeUnit.SECONDS);
            assertEquals(cancel ? DiagnosticProbe.Reason.CANCELLED : DiagnosticProbe.Reason.EXPIRED, result.reason());
            assertTrue(result.cleanupComplete());
            terminated.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void onlyDestinationsTheJobMayUseAreProbed() {
        for (String address : List.of("10.0.0.9", "127.0.0.1", "192.0.2.1", "203.0.113.7")) {
            DiagnosticProbe.Job job = job(20, new InetSocketAddress(address, 19132));
            assertThrows(IllegalArgumentException.class, () -> probe(job), address);
        }
        DiagnosticProbe.Job job = job(20, new InetSocketAddress("10.0.0.9", 19132));
        DiagnosticProbe.builder(engine, executor, job, BIND, authorized::get).localDevelopment(true).build();
        for (InetSocketAddress bind : List.of(new InetSocketAddress("0.0.0.0", 40000),
                new InetSocketAddress("10.0.0.5", 0), new InetSocketAddress("2001:db8::5", 40000),
                InetSocketAddress.createUnresolved("probe.example", 40000))) {
            assertThrows(IllegalArgumentException.class,
                    () -> DiagnosticProbe.builder(engine, executor, job(20), bind, authorized::get).build(),
                    bind.toString());
        }
        assertThrows(IllegalArgumentException.class, () -> DiagnosticProbe.builder(engine, executor, job(20), BIND,
                authorized::get).stunServer(new InetSocketAddress("93.184.216.1", 3478)).build(),
                "only an assisted probe asks a STUN server");
    }

    // ---- Helpers ----

    private DiagnosticProbe probe(DiagnosticProbe.Job job) {
        BooleanSupplier authority = authorized::get;
        return DiagnosticProbe.builder(engine, executor, job, BIND, authority).build();
    }

    private static DiagnosticProbe.Job job(int seconds) {
        return job(seconds, TARGET);
    }

    private static DiagnosticProbe.Job job(int seconds, InetSocketAddress target) {
        byte[] attempt = new byte[16];
        new SecureRandom().nextBytes(attempt);
        return new DiagnosticProbe.Job(CONTEXT, HexFormat.of().formatHex(attempt),
                new DiagnosticHostPolicy.Endpoint(4, DiagnosticAdmissionCodec.address(4,
                        target.getAddress().getHostAddress()), target.getPort(), 7), HOST_FINGERPRINT,
                (System.currentTimeMillis() / 1000 + seconds) * 1000);
    }

    private FakePeer gather() throws InterruptedException {
        return gather(1);
    }

    /** Completes the {@code number}th probe peer's offer and gathering as the engine would, with its credentials. */
    private FakePeer gather(int number) throws InterruptedException {
        HostFixtureWait.until(() -> engine.peers().size() >= number && engine.peer(number - 1).links().size() == 2,
                "the probe's peer and its two channels");
        FakePeer peer = engine.peer(number - 1);
        IceCredentials credentials = peer.config().localCredentials();
        String offer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\n"
                + "a=setup:actpass\r\na=ice-ufrag:" + credentials.ufrag() + "\r\na=ice-pwd:" + credentials.password()
                + "\r\na=fingerprint:sha-256 " + "AB:".repeat(31) + "AB\r\na=sctp-port:5000\r\n"
                + "a=max-message-size:262144\r\n";
        peer.completeOffer(offer);
        peer.gatheringComplete(offer + "a=candidate:1 1 udp 2122260223 10.0.0.5 40000 typ host generation 0\r\n"
                + "a=end-of-candidates\r\n");
        return peer;
    }

    /**
     * Runs a direct probe whose {@code number}th peer connects over this pair, and answers its PING as the host would
     * if it sends one, or with the same PONG as a text message.
     */
    private DiagnosticProbe.Result probeOver(TransportPath path, int number) throws Exception {
        return probeOver(path, number, false);
    }

    private DiagnosticProbe.Result probeOver(TransportPath path, int number, boolean textPong) throws Exception {
        DiagnosticProbe.Job job = job(20);
        CompletableFuture<DiagnosticProbe.Result> running = probe(job).run(request ->
                CompletableFuture.completedFuture(answer(request, job, HOST_FINGERPRINT, TARGET))).toCompletableFuture();
        FakePeer peer = gather(number);
        HostFixtureWait.until(() -> !peer.remoteAnswers().isEmpty(), "the answer to reach the engine");
        peer.path(path);
        peer.state(PeerState.CONNECTED);
        FakeDataLink reliable = peer.links().getFirst();
        peer.open(reliable);
        peer.open(peer.links().getLast());
        HostFixtureWait.until(() -> running.isDone() || reliable.sendCount() == 1, "the PING or the result");
        if (!running.isDone()) {
            reliable.accept(0);
            byte[] ping = reliable.sentBytes().getFirst();
            byte[] pong = DiagnosticExchange.encode(job.attemptIdHex(), DiagnosticExchange.PONG, 0,
                    Arrays.copyOfRange(ping, 24, 56));
            if (textPong) {
                peer.text(reliable, pong);
            } else {
                peer.message(reliable, pong);
            }
        }
        return running.get(5, TimeUnit.SECONDS);
    }

    /** The host's answer as a provider returns it: a permit for the request, the target as its one candidate. */
    private static String answer(DiagnosticProbe.Request request, DiagnosticProbe.Job job, String fingerprint,
                                 InetSocketAddress candidate) {
        Credentials credentials = DiagnosticAdmissionCodec.issue(CONTEXT, KEY, request.claims(), request.ufrag(),
                request.offer(), KEY.retireAfter(), DiagnosticAdmissionCodec.Clock.system());
        return "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\n"
                + "a=setup:active\r\na=ice-ufrag:" + credentials.localUfrag() + "\r\na=ice-pwd:"
                + credentials.icePwd() + "\r\na=fingerprint:sha-256 "
                + HexFormat.ofDelimiter(":").withUpperCase().formatHex(HexFormat.of().parseHex(fingerprint))
                + "\r\na=sctp-port:5000\r\na=max-message-size:262144\r\na=candidate:1 1 UDP 2130706431 "
                + candidate.getAddress().getHostAddress() + " " + candidate.getPort()
                + " typ host\r\na=end-of-candidates\r\n";
    }

    /** Waits for a condition, as the host fixture does. */
    private static final class HostFixtureWait {
        static void until(BooleanSupplier condition, String what) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!condition.getAsBoolean()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("Timed out waiting: " + what);
                }
                Thread.sleep(10);
            }
        }
    }
}

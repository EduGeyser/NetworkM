package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.WebRtcEngine;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Context;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Credentials;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Key;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticProbe;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.host.DiagnosticResult.Outcome;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfileSnapshot;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.github.sendablemetatype.netty.util.nethernet.DtlsCertificate;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NXS connectivity checks on real libwebrtc: the prober on a peer of its own checks an endpoint's shared ICE port, the
 * test plays the provider between them, and no player child appears. Everything runs on this machine's own network
 * addresses, which libwebrtc gathers on (it ignores loopback interfaces), so host and prober run in local development.
 */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
@Timeout(90)
class DiagnosticNativeTest {
    private static final long WAIT_SECONDS = 30;
    private static final String KEY_ID = "K001";
    private static final String SECRET = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final DefaultEventExecutor prober = new DefaultEventExecutor();
    private final NetherNetEngine engine = WebRtcEngine.builder().factories(2).build();
    private final BlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    private final Key key = new Key(KEY_ID, SECRET, 0, System.currentTimeMillis() + 600_000);
    @TempDir
    Path directory;
    private NetherNetServerChannel server;
    private HostTransport transport;
    private DtlsCertificate certificate;
    private Context context;
    private HostProfileSnapshot snapshot;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        engine.close().toCompletableFuture().get(20, TimeUnit.SECONDS);
        prober.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aDirectCheckEchoesThePingOnTheSharedPort() throws Exception {
        host(HostTransport.Publication.FIXED, false);
        InetSocketAddress target = ipv4(snapshot.probeCandidates());
        DiagnosticHostPolicy.Endpoint endpoint = endpoint(target);
        install(Set.of(endpoint));
        DiagnosticProbe.Job job = job(endpoint, DtlsFingerprint.of(certificate.sha256()), 30);
        InetSocketAddress bind = freePort(target);

        DiagnosticProbe probe = probe(job, bind);
        DiagnosticProbe.Result result = probe.run(request -> CompletableFuture.completedFuture(
                directAnswer(request, target))).toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertEquals(DiagnosticProbe.Reason.COMPLETE, result.reason(), result.toString());
        assertTrue(result.pingVerified());
        assertEquals(bind, result.selectedLocal());
        assertEquals(target, result.selectedRemote());
        assertEquals(List.of(1, 56, 1, 56), List.of(result.sentFrames(), result.sentBytes(), result.receivedFrames(),
                result.receivedBytes()));
        DiagnosticResult observed = hostResult();
        assertEquals(Outcome.PONG_SENT, observed.outcome(), observed.toString());
        assertEquals(result.offerDigestHex(), observed.offerDigestHex());
        assertEquals(bind, observed.selectedRemote());
        assertTrue(children.isEmpty(), "a check is never a player");
    }

    @Test
    void anAssistedCheckIsAnsweredOverTheProvidersSocket() throws Exception {
        host(HostTransport.Publication.MAINTAINED, true);
        long revision = snapshot.candidateRevision();
        assertTrue(snapshot.assistedFamilies().contains(AddressFamily.IPV4), snapshot.toString());
        DiagnosticHostPolicy.Endpoint endpoint = DiagnosticHostPolicy.Endpoint.assisted(4, revision);
        install(Set.of(endpoint));
        DtlsFingerprint fingerprint = DtlsFingerprint.of(certificate.sha256());
        DiagnosticProbe.Job job = job(endpoint, fingerprint, 30);
        InetSocketAddress bind = freePort(ipv4(snapshot.profile().candidates()));
        List<AssistedAnswer> answers = new ArrayList<>();

        DiagnosticProbe.Result result = probe(job, bind).run(request -> {
            // The provider's part: a permit for the exact offer, pushed to the host as an assisted join
            Credentials permit = DiagnosticAdmissionCodec.issue(context, key, request.claims(), request.ufrag(),
                    request.offer(), key.retireAfter(), DiagnosticAdmissionCodec.Clock.system());
            AssistedJoin join = new AssistedJoin(job.attemptIdHex(), context.hostId(), context.generation(),
                    context.incarnation(), KEY_ID, fingerprint, job.expiresAt(), "0", null, permit.localUfrag(),
                    permit.icePwd(), new String(request.offer(), StandardCharsets.UTF_8));
            return transport.assistedJoin(join, System.nanoTime() + TimeUnit.SECONDS.toNanos(20),
                    OwnershipGuard.ALWAYS).thenApply(answer -> {
                answers.add(answer);
                return answer.sdp();
            });
        }).toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertEquals(DiagnosticProbe.Reason.COMPLETE, result.reason(), result.toString());
        AssistedAnswer answer = answers.getFirst();
        assertFalse(answer.asserted());
        assertEquals(1, answer.sdp().lines().filter(line -> line.startsWith("a=candidate:")).count());
        assertEquals(((InetSocketAddress) server.localAddress()).getPort(), result.selectedRemote().getPort(),
                "the check reached the shared port");
        DiagnosticResult observed = hostResult();
        assertEquals(Outcome.PONG_SENT, observed.outcome(), observed.toString());
        assertTrue(observed.target().assisted());
        assertTrue(children.isEmpty(), "a check is never a player");
    }

    @Test
    void aHostWithAnotherCertificateFailsTheProbe() throws Exception {
        host(HostTransport.Publication.FIXED, false);
        InetSocketAddress target = ipv4(snapshot.probeCandidates());
        DiagnosticHostPolicy.Endpoint endpoint = endpoint(target);
        install(Set.of(endpoint));
        // The provider and the prober expect a certificate the host does not present
        DtlsFingerprint expected = DtlsFingerprint.of(DtlsCertificate.loadOrCreate(directory.resolve("other.pem"))
                .sha256());
        DiagnosticProbe.Job job = job(endpoint, expected, 6);

        DiagnosticProbe.Result result = probe(job, freePort(target)).run(request -> CompletableFuture
                .completedFuture(answer(request, expected, target))).toCompletableFuture()
                .get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertEquals(DiagnosticProbe.Reason.TRANSPORT, result.reason(), result.toString());
        assertTrue(result.answerVerified());
        assertFalse(result.pingVerified());
        assertTrue(children.isEmpty());
    }

    // ---- Helpers ----

    /** An endpoint bound to every IPv4 address, with NXS on it in local development and the key installed. */
    private void host(HostTransport.Publication publication, boolean assisted) throws Exception {
        certificate = DtlsCertificate.loadOrCreate(directory.resolve("dtls-certificate.pem"));
        server = (NetherNetServerChannel) new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel child) {
                        children.add((NetherNetChildChannel) child);
                    }
                })
                .bind(new InetSocketAddress("0.0.0.0", 0)).sync().channel();
        transport = new HostTransport(new HostTransport.Settings(publication, List.of(), true, assisted, false,
                TicketLedger.Limits.defaults()), certificate, server.eventLoop());
        server.addSource(transport.source());
        transport.started().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        transport.installAdmissionKeys(List.of(new AdmissionKey(KEY_ID, SECRET))).toCompletableFuture()
                .get(WAIT_SECONDS, TimeUnit.SECONDS);
        snapshot = transport.captureHostProfile().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        context = new Context("https://provider.example", "instance", transport.incarnation(), 1);
    }

    private void install(Set<DiagnosticHostPolicy.Endpoint> endpoints) throws Exception {
        transport.configureDiagnostics(new DiagnosticHostPolicy(context, List.of(key), endpoints,
                System.currentTimeMillis() + 120_000), System.nanoTime() + TimeUnit.SECONDS.toNanos(120))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private DiagnosticHostPolicy.Endpoint endpoint(InetSocketAddress target) {
        return new DiagnosticHostPolicy.Endpoint(4, DiagnosticAdmissionCodec.address(4,
                target.getAddress().getHostAddress()), target.getPort(), snapshot.candidateRevision());
    }

    private DiagnosticProbe.Job job(DiagnosticHostPolicy.Endpoint target, DtlsFingerprint host, int seconds) {
        byte[] attempt = new byte[16];
        new SecureRandom().nextBytes(attempt);
        return new DiagnosticProbe.Job(context, HexFormat.of().formatHex(attempt), target, host.hex(),
                (System.currentTimeMillis() / 1000 + seconds) * 1000);
    }

    private DiagnosticProbe probe(DiagnosticProbe.Job job, InetSocketAddress bind) {
        return DiagnosticProbe.builder(engine, prober, job, bind, () -> true).localDevelopment(true).build();
    }

    private String directAnswer(DiagnosticProbe.Request request, InetSocketAddress target) {
        return answer(request, DtlsFingerprint.of(certificate.sha256()), target);
    }

    /** The host's answer as the provider builds it from the profile: the permit's credentials, the target. */
    private String answer(DiagnosticProbe.Request request, DtlsFingerprint fingerprint, InetSocketAddress target) {
        Credentials permit = DiagnosticAdmissionCodec.issue(context, key, request.claims(), request.ufrag(),
                request.offer(), key.retireAfter(), DiagnosticAdmissionCodec.Clock.system());
        return "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\n"
                + "a=setup:active\r\na=ice-ufrag:" + permit.localUfrag() + "\r\na=ice-pwd:" + permit.icePwd()
                + "\r\na=fingerprint:" + fingerprint.value() + "\r\na=sctp-port:5000\r\n"
                + "a=max-message-size:262144\r\na=candidate:1 1 UDP 2130706431 "
                + target.getAddress().getHostAddress() + " " + target.getPort() + " typ host\r\na=end-of-candidates\r\n";
    }

    private static InetSocketAddress ipv4(List<HostCandidate> candidates) {
        return candidates.stream().filter(candidate -> candidate.family() == AddressFamily.IPV4).findFirst()
                .orElseThrow(() -> new AssertionError("The host gathered no IPv4 address: " + candidates))
                .endpoint();
    }

    /** A free UDP port on the address the host listens on, for the prober. */
    private static InetSocketAddress freePort(InetSocketAddress on) throws Exception {
        try (DatagramSocket socket = new DatagramSocket(new InetSocketAddress(on.getAddress(), 0))) {
            return new InetSocketAddress(on.getAddress(), socket.getLocalPort());
        }
    }

    /** The host's result for the one check it admitted. */
    private DiagnosticResult hostResult() throws InterruptedException {
        List<DiagnosticResult> results = new ArrayList<>();
        HostFixture.await(() -> {
            results.addAll(transport.pollDiagnosticResults(1));
            return !results.isEmpty();
        }, "the host's diagnostic result");
        return results.getFirst();
    }
}

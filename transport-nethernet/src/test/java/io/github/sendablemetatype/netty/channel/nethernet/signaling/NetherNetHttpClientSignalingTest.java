package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import com.sun.net.httpserver.HttpServer;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetHttpClientSignaling.Probe;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.ClientInfo;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.HttpClientSettings;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.HttpClientSettings.Scheme;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusRequest;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.pkitesting.X509Bundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The HTTP signaling client against this library's server and against servers that are not NetherNet (E-8). */
@Timeout(30)
class NetherNetHttpClientSignalingTest {
    private static final String OFFER = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n";
    private static final String ANSWER = "v=0\r\no=- 3 4 IN IP4 127.0.0.1\r\n";
    private static final NetherNetServerStatus STATUS = NetherNetServerStatus.builder().name("Server").players(3).build();
    private static final long CONNECTION_ID = 77;

    private static X509Bundle authority;
    private static SslContext serverContext;
    private static SslContext ipLessServerContext;

    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final RecordingSink sink = new RecordingSink();
    private final List<StatusRequest> statusRequests = new CopyOnWriteArrayList<>();
    private final BlockingQueue<String> signals = new LinkedBlockingQueue<>();
    private NetherNetHttpSignaling server;
    private HttpServer foreign;
    private InetSocketAddress address;

    @BeforeAll
    static void certificates() throws Exception {
        authority = TestCertificates.authority();
        serverContext = TestCertificates.serverContext(TestCertificates.server(authority));
        // Names a host but not the address the client dials
        X509Bundle named = new io.netty.pkitesting.CertificateBuilder()
                .subject("CN=play.example.test")
                .addSanDnsName("play.example.test")
                .addExtendedKeyUsageServerAuth()
                .ecp256()
                .buildIssuedBy(authority);
        ipLessServerContext = TestCertificates.serverContext(named);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (foreign != null) {
            foreign.stop(0);
        }
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private void startServer(SslContext tls) throws Exception {
        server = NetherNetHttpSignaling.builder(group)
                .tls(tls == null ? null : name -> tls)
                .statusProvider(request -> {
                    statusRequests.add(request);
                    return StatusAnswer.serve(STATUS);
                })
                .build();
        server.bind(new InetSocketAddress("127.0.0.1", 0));
        server.start(sink);
        address = server.boundAddress();
    }

    /** A plain HTTP server answering every request with this status and body. */
    private void startForeign(int status, String body, String location) throws IOException {
        foreign = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        foreign.createContext("/", exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            if (location != null) {
                exchange.getResponseHeaders().add("Location", location);
            }
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        foreign.start();
        address = foreign.getAddress();
    }

    private static HttpClientSettings trusting() throws Exception {
        return HttpClientSettings.DEFAULT.withTrust(authority.toTrustManagerFactory());
    }

    private Probe probe(HttpClientSettings settings) throws Exception {
        return NetherNetHttpClientSignaling.probe(address, settings, group, "123").get(15, TimeUnit.SECONDS);
    }

    private ConnectException probeFailure(HttpClientSettings settings) {
        ExecutionException failure = assertThrows(ExecutionException.class, () -> probe(settings));
        return assertInstanceOf(ConnectException.class, failure.getCause());
    }

    private NetherNetHttpClientSignaling connected(HttpClientSettings settings) throws Exception {
        NetherNetHttpClientSignaling client = new NetherNetHttpClientSignaling(settings, group, "client-1");
        client.setSignalHandler(CONNECTION_ID, signals::add);
        client.connect(address).get(15, TimeUnit.SECONDS);
        return client;
    }

    private String nextSignal() throws InterruptedException {
        String signal = signals.poll(15, TimeUnit.SECONDS);
        assertTrue(signal != null, "no signal arrived");
        return signal;
    }

    @Test
    void readsTheStatusOverHttps() throws Exception {
        startServer(serverContext);

        Probe probe = probe(trusting().withClientInfo(new ClientInfo("1.26.62", 2225, 8)));

        assertEquals(Scheme.HTTPS, probe.scheme());
        assertEquals(STATUS.toJson(), probe.status().toJson());
        StatusRequest request = statusRequests.getFirst();
        assertEquals(new ClientInfo("1.26.62", 2225, 8), request.client());
        assertEquals("123", request.networkId());
        assertEquals("127.0.0.1:" + address.getPort(), request.host(), "Host carries what the client reached");
    }

    @Test
    void fallsBackToPlainHttpWhenTlsIsRefused() throws Exception {
        startServer(null);

        Probe probe = probe(trusting());

        assertEquals(Scheme.HTTP, probe.scheme());
        assertEquals(STATUS.toJson(), probe.status().toJson());
    }

    @Test
    void untrustedCertificateIsFinal() throws Exception {
        startServer(serverContext);

        ConnectException failure = probeFailure(HttpClientSettings.DEFAULT);

        assertTrue(failure.getMessage().contains("could not be reached over HTTPS"), failure.getMessage());
        assertTrue(statusRequests.isEmpty(), "no plain HTTP request after a rejected certificate");
    }

    @Test
    void certificateForAnotherNameIsFinal() throws Exception {
        startServer(ipLessServerContext);

        probeFailure(trusting());

        assertTrue(statusRequests.isEmpty(), "the certificate is checked against the address dialed");
    }

    @Test
    void nothingListeningFailsWithoutAPlainAttempt() throws Exception {
        try (ServerSocket closed = new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            address = new InetSocketAddress("127.0.0.1", closed.getLocalPort());
        }

        ConnectException failure = probeFailure(trusting());

        assertTrue(failure.getMessage().contains("connection refused"), failure.getMessage());
    }

    @Test
    void httpsSchemeNeverFallsBack() throws Exception {
        startServer(null);

        probeFailure(trusting().withScheme(Scheme.HTTPS));

        assertTrue(statusRequests.isEmpty());
    }

    @Test
    void emptyStatusStillServesNetherNet() throws Exception {
        startForeign(200, "", null);

        Probe probe = probe(trusting().withScheme(Scheme.HTTP));

        assertEquals(Scheme.HTTP, probe.scheme());
        assertNull(probe.status(), "vanilla before 1.26.50 answers with no body");
    }

    @Test
    void webPageIsNotNetherNet() throws Exception {
        startForeign(200, "<!doctype html><title>Welcome</title>", null);

        ConnectException failure = probeFailure(trusting().withScheme(Scheme.HTTP));

        assertTrue(failure.getMessage().contains("something other than a NetherNet status"), failure.getMessage());
    }

    @Test
    void declineIsNotNetherNet() throws Exception {
        startForeign(404, "", null);

        ConnectException failure = probeFailure(trusting().withScheme(Scheme.HTTP));

        assertTrue(failure.getMessage().contains("does not serve NetherNet"), failure.getMessage());
    }

    @Test
    void redirectsAreNotFollowed() throws Exception {
        startForeign(302, "", "http://198.51.100.1/v1/join");

        ConnectException failure = probeFailure(trusting().withScheme(Scheme.HTTP));

        assertTrue(failure.getMessage().contains("HTTP 302"), failure.getMessage());
    }

    @Test
    void offerIsPostedOnceAndItsAnswerArrives() throws Exception {
        sink.onJoin = join -> join.reply().answer(ANSWER);
        startServer(serverContext);
        NetherNetHttpClientSignaling client = connected(trusting());

        client.sendSignal("0", NetherNetConstants.buildSignalConnectRequest(CONNECTION_ID, OFFER));
        client.sendSignal("0", NetherNetConstants.buildSignalCandidateAdd(CONNECTION_ID, "candidate:1 1 udp 1 1.2.3.4 5 typ host"));

        assertEquals(NetherNetConstants.buildSignalConnectResponse(CONNECTION_ID, ANSWER), nextSignal());
        Join join = sink.next();
        assertEquals("client-1", join.remoteNetworkId());
        assertEquals(OFFER, join.offer());
        assertTrue(sink.joins.isEmpty(), "candidates go nowhere: the offer holds them all");
    }

    @Test
    void refusedOfferArrivesAsAConnectError() throws Exception {
        sink.onJoin = join -> join.reply().refuse(JoinRefusal.CAPACITY);
        startServer(null);
        NetherNetHttpClientSignaling client = connected(trusting());

        client.sendSignal("0", NetherNetConstants.buildSignalConnectRequest(CONNECTION_ID, OFFER));

        assertEquals(NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR + " " + CONNECTION_ID + " "
                + JoinRefusal.CAPACITY.errorCode(), nextSignal());
    }

    @Test
    void successWithoutAnSdpIsARefusal() throws Exception {
        // How vanilla BDS refuses an offer without identity: 200, body 37
        startForeign(200, "37", null);
        NetherNetHttpClientSignaling client = connected(trusting().withScheme(Scheme.HTTP));

        client.sendSignal("0", NetherNetConstants.buildSignalConnectRequest(CONNECTION_ID, OFFER));

        assertTrue(nextSignal().startsWith(NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR + " " + CONNECTION_ID));
    }

    @Test
    void answerWaitIsConfigurable() throws Exception {
        startServer(null);
        NetherNetHttpClientSignaling client = connected(trusting().withAnswerTimeout(Duration.ofMillis(300)));

        client.sendSignal("0", NetherNetConstants.buildSignalConnectRequest(CONNECTION_ID, OFFER));
        Join join = sink.next();

        assertEquals(NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR + " " + CONNECTION_ID + " "
                + JoinRefusal.NO_ANSWER_IN_TIME.errorCode(), nextSignal());
        // The client hung up, which the server reports as an abandoned join
        join.reply().abandoned().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void closedClientCannotConnect() {
        NetherNetHttpClientSignaling client = new NetherNetHttpClientSignaling(HttpClientSettings.DEFAULT, group);
        client.close();

        assertThrows(ExecutionException.class, () -> client.connect(new InetSocketAddress("127.0.0.1", 1)).get());
    }

    @Test
    void serverErrorTextIsCutAndReducedToPrintableAscii() {
        String reason = NetherNetHttpClientSignaling.reason("bad\u0000\nthingé" + "x".repeat(300));

        assertTrue(reason.startsWith(": bad  thing x"), reason);
        assertEquals(202, reason.length());
        assertEquals("", NetherNetHttpClientSignaling.reason("  "));
    }
}

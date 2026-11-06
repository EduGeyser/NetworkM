package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetHttpSignaling;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.ANSWER;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * HTTP signaling feeding a real endpoint on the fake engine, over real TCP: what a client sees
 * for each outcome of the endpoint's admission stage, and what the endpoint does when the HTTP
 * client goes away.
 */
@Timeout(20)
class NetherNetHttpJoinTest {
    private static final String HOST_CANDIDATE = "a=candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host";
    private final ManualCheckExecutor checks = new ManualCheckExecutor();
    private EndpointHarness harness;
    private NetherNetHttpSignaling http;
    private MultiThreadIoEventLoopGroup httpLoop;
    private final List<Integer> refused = new CopyOnWriteArrayList<>();
    private final NetherServerMetrics metrics = new NetherServerMetrics() {
        @Override
        public void joinRefused(int httpStatus) {
            refused.add(httpStatus);
        }
    };

    @AfterEach
    void tearDown() {
        harness.close();
        if (httpLoop != null) {
            httpLoop.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    /** Binds an endpoint and an HTTP source on it, checking identities on the given executor. */
    private void start(boolean manualChecks) throws Exception {
        harness = manualChecks ? new EndpointHarness(() -> checks) : new EndpointHarness();
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(ANSWER + HOST_CANDIDATE + "\r\n");
    }

    private void bind() throws Exception {
        harness.bind();
        http = NetherNetHttpSignaling.builder(harness.group).build();
        http.bind(new InetSocketAddress("127.0.0.1", 0));
        harness.server.addSource(http);
    }

    @Test
    void anAdmittedOfferIsAnsweredInFullAndItsChildBindsTheLogin() throws Exception {
        start(false);
        bind();

        String response = response(post(retailOffer()));

        assertTrue(response.startsWith("HTTP/1.1 200 OK\r\n"), response);
        String lower = response.toLowerCase();
        assertTrue(lower.contains("content-type: application/sdp\r\n"), response);
        assertTrue(lower.contains("connection: close\r\n"), response);
        assertTrue(response.contains("a=identity:"), "the endpoint's identity signs the answer: " + response);
        assertTrue(response.endsWith(HOST_CANDIDATE + "\r\na=end-of-candidates\r\n"),
                "the answer waits for gathering and carries every candidate: " + response);
        NetherNetChildChannel child = harness.child();
        assertEquals(OfferCheckResult.Level.ATTESTED, child.getOfferCheck().level());
        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(child, CLIENT_KEY.getPublic()));
    }

    @Test
    void anOfferWithoutAProvenKeyIsRefusedWith401AndCreatesNoPeer() throws Exception {
        start(false);
        bind();

        assertStatus(401, response(post(EndpointHarness.OFFER)));
        assertStatus(401, response(post(retailOffer().replace(DIGEST, OTHER_DIGEST))));
        assertTrue(harness.engine.peers().isEmpty());
        awaitTrue(() -> harness.server.gate().pending() == 0, "the budget is given back");
    }

    @Test
    void aFullCheckQueueRefusesWith503() throws Exception {
        start(true);
        bind();
        Socket first = post(educationOffer());
        checks.awaitQueued();

        // Another client: a second offer from the same one would be a duplicate
        assertStatus(503, response(post("other", educationOffer())));

        checks.runNext();
        assertStatus(200, response(first));
        assertEquals(OfferCheckResult.Level.KEY_PROVEN, harness.child().getOfferCheck().level());
    }

    @Test
    void aCheckPastItsDeadlineIsRefusedWith504AndAdmitsNothingLater() throws Exception {
        start(true);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS, 50);
        bind();

        assertStatus(504, response(post(educationOffer())));

        Future<?> check = checks.awaitQueued();
        assertTrue(check.isCancelled(), "the check is withdrawn");
        checks.runNext();
        harness.sync();
        assertTrue(harness.engine.peers().isEmpty());
        assertEquals(0, harness.server.gate().pending());
    }

    @Test
    void aClientThatLeavesDuringTheCheckCancelsTheJoin() throws Exception {
        start(true);
        bind();
        Socket client = post(educationOffer());
        Future<?> check = checks.awaitQueued();
        assertEquals(1, harness.server.gate().pending());

        client.close();

        awaitTrue(check::isCancelled, "the check is withdrawn");
        awaitTrue(() -> harness.server.gate().pending() == 0, "the budget is given back");
        checks.runNext();
        harness.sync();
        assertTrue(harness.engine.peers().isEmpty());
    }

    @Test
    void removingTheHttpSourceDuringACheckRefusesTheJoinWith503() throws Exception {
        start(true);
        bind();
        Socket client = post(educationOffer());
        Future<?> check = checks.awaitQueued();

        assertTrue(harness.server.removeSource(http));

        assertStatus(503, response(client));
        awaitTrue(check::isCancelled, "the check is withdrawn");
        awaitTrue(() -> harness.server.gate().pending() == 0, "the budget is given back");
        assertTrue(harness.engine.peers().isEmpty());
    }

    @Test
    void closingTheEndpointDuringACheckRefusesTheJoinWith503AndStopsTheChecks() throws Exception {
        start(true);
        bind();
        Socket client = post(educationOffer());
        checks.awaitQueued();

        harness.server.close().sync();

        assertStatus(503, response(client));
        assertTrue(checks.isShutdown());
        awaitTrue(() -> harness.server.gate().pending() == 0, "the budget is given back");
        assertTrue(harness.engine.peers().isEmpty());
    }

    @Test
    void everyHttpSourceOfAnEndpointHoldsANetworkIdUntilItsJoinIsAnswered() throws Exception {
        start(true);
        bind();
        NetherNetHttpSignaling other = httpSource();

        Socket first = post(http, "42", educationOffer());
        checks.awaitQueued();
        assertStatus(409, response(post(other, "42", educationOffer())));
        assertTrue(harness.engine.peers().isEmpty(), "the duplicate gets no peer");

        checks.runNext();
        assertStatus(200, response(first));
        NetherNetChildChannel answered = harness.child();
        assertFalse(answered.isActive());
        Socket again = post(other, "42", educationOffer());
        checks.awaitQueued();
        checks.runNext();
        assertStatus(200, response(again));
        assertTrue(answered.isOpen(), "the first join is untouched");
    }

    @Test
    void removingAnHttpSourceFreesTheNetworkIdOfTheJoinItAbandons() throws Exception {
        harness = new EndpointHarness();
        bind();
        Socket first = post(http, "42", retailOffer());
        NetherNetChildChannel pending = harness.child();
        harness.negotiating();
        // Only later peers answer: the first join stays without one.
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(ANSWER + HOST_CANDIDATE + "\r\n");

        assertTrue(harness.server.removeSource(http));
        response(first);
        awaitTrue(() -> !pending.isOpen(), "the abandoned join closes");

        NetherNetHttpSignaling replacement = httpSource();
        assertStatus(200, response(post(replacement, "42", retailOffer())));
    }

    // One count per refusal: whichever settles the HTTP request first reports it.

    @Test
    void anAdmissionRefusalAfterTheHttpDeadlineIsNotCountedAgain() throws Exception {
        harness = new EndpointHarness(() -> checks);
        bindWithOwnLoop(Duration.ofMillis(300));
        Socket client = post(EndpointHarness.OFFER);
        checks.awaitQueued();

        CountDownLatch release = blockEndpointLoop();
        try {
            // The check's result waits behind the blocked loop, and the deadline passes meanwhile
            checks.runNext();
            assertStatus(504, response(client));
        } finally {
            release.countDown();
        }
        syncBothLoops();

        assertEquals(List.of(504), refused);
    }

    @Test
    void aNegotiationRefusalAfterTheHttpDeadlineIsNotCountedAgain() throws Exception {
        harness = new EndpointHarness();
        bindWithOwnLoop(Duration.ofSeconds(1));
        Socket client = post(retailOffer());
        FakePeer peer = harness.negotiating();

        CountDownLatch release = blockEndpointLoop();
        try {
            peer.failAnswer(new IllegalStateException("the engine could not answer"));
            assertStatus(504, response(client));
        } finally {
            release.countDown();
        }
        syncBothLoops();

        assertEquals(List.of(504), refused);
    }

    /** Binds the endpoint and an HTTP source on an I/O loop of its own, both reporting to {@link #metrics}. */
    private void bindWithOwnLoop(Duration negotiationTimeout) throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_METRICS, metrics);
        harness.bind();
        httpLoop = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        http = NetherNetHttpSignaling.builder(httpLoop).metrics(metrics).negotiationTimeout(negotiationTimeout).build();
        http.bind(new InetSocketAddress("127.0.0.1", 0));
        harness.server.addSource(http);
    }

    /** Holds the endpoint's event loop, which its children share, until the latch is released. */
    private CountDownLatch blockEndpointLoop() throws InterruptedException {
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        harness.server.eventLoop().execute(() -> {
            blocked.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blocked.await();
        return release;
    }

    private void syncBothLoops() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            harness.sync();
            httpLoop.next().submit(() -> { }).sync();
        }
    }

    /** Another HTTP source on the bound endpoint. */
    private NetherNetHttpSignaling httpSource() throws Exception {
        NetherNetHttpSignaling source = NetherNetHttpSignaling.builder(harness.group).build();
        source.bind(new InetSocketAddress("127.0.0.1", 0));
        harness.server.addSource(source);
        return source;
    }

    /** Sends an offer on a connection of its own and returns the connection, unread. */
    private Socket post(String offer) throws IOException {
        return post("client", offer);
    }

    private Socket post(String networkId, String offer) throws IOException {
        return post(http, networkId, offer);
    }

    private static Socket post(NetherNetHttpSignaling source, String networkId, String offer) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(10_000);
        socket.connect(source.boundAddress());
        byte[] body = offer.getBytes(StandardCharsets.UTF_8);
        String head = "POST /v1/join/" + networkId + " HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/sdp\r\n"
                + "Content-Length: " + body.length + "\r\n\r\n";
        OutputStream out = socket.getOutputStream();
        out.write(head.getBytes(StandardCharsets.US_ASCII));
        out.write(body);
        out.flush();
        return socket;
    }

    /** Everything the server sends until it closes the connection. */
    private static String response(Socket socket) throws IOException {
        try (socket) {
            return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void assertStatus(int status, String response) {
        assertTrue(response.startsWith("HTTP/1.1 " + status + " "), response);
    }

    private static void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail(message);
            }
            Thread.sleep(5);
        }
    }
}

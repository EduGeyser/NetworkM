package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetHttpSignaling;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
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
    private final ManualCheckExecutor checks = new ManualCheckExecutor();
    private EndpointHarness harness;
    private NetherNetHttpSignaling http;

    @AfterEach
    void tearDown() {
        harness.close();
    }

    /** Binds an endpoint and an HTTP source on it, checking identities on the given executor. */
    private void start(boolean manualChecks) throws Exception {
        harness = manualChecks ? new EndpointHarness(() -> checks) : new EndpointHarness();
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(ANSWER);
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
        assertTrue(response.endsWith("a=end-of-candidates\r\n"), response);
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

        assertStatus(503, response(post(educationOffer())));

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
    void removingTheHttpSourceDuringACheckEndsTheRequestAndCancelsTheJoin() throws Exception {
        start(true);
        bind();
        Socket client = post(educationOffer());
        Future<?> check = checks.awaitQueued();

        assertTrue(harness.server.removeSource(http));

        assertEquals("", response(client), "the connection closes unanswered");
        awaitTrue(check::isCancelled, "the check is withdrawn");
        awaitTrue(() -> harness.server.gate().pending() == 0, "the budget is given back");
        assertTrue(harness.engine.peers().isEmpty());
    }

    @Test
    void closingTheEndpointDuringACheckEndsTheRequestAndStopsTheChecks() throws Exception {
        start(true);
        bind();
        Socket client = post(educationOffer());
        checks.awaitQueued();

        harness.server.close().sync();

        assertEquals("", response(client), "the connection closes unanswered");
        assertTrue(checks.isShutdown());
        awaitTrue(() -> harness.server.gate().pending() == 0, "the budget is given back");
        assertTrue(harness.engine.peers().isEmpty());
    }

    /** Sends an offer on a connection of its own and returns the connection, unread. */
    private Socket post(String offer) throws IOException {
        Socket socket = new Socket();
        socket.setSoTimeout(10_000);
        socket.connect(http.boundAddress());
        byte[] body = offer.getBytes(StandardCharsets.UTF_8);
        String head = "POST /v1/join/client HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/sdp\r\n"
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

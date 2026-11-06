package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.Response;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusAnswer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.get;
import static io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.post;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What HTTP signaling answers on one connection, and when it closes it (rulings E-4 and E-7), on
 * an embedded channel with frozen time.
 */
class HttpSignalingRequestTest {
    private static final String OFFER = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n";
    private static final String ANSWER = "v=0\r\no=- 3 4 IN IP4 127.0.0.1\r\n";

    private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final RecordingSink sink = new RecordingSink();
    private final List<EmbeddedHttp> connections = new ArrayList<>();
    private NetherNetHttpSignaling signaling;

    @AfterEach
    void tearDown() {
        connections.forEach(EmbeddedHttp::close);
        if (signaling != null) {
            signaling.close();
        }
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private EmbeddedHttp connect() {
        return connect(NetherNetHttpSignaling.builder(worker));
    }

    private EmbeddedHttp connect(NetherNetHttpSignaling.Builder builder) {
        if (signaling == null) {
            signaling = builder.build();
            signaling.start(sink);
        }
        EmbeddedHttp connection = new EmbeddedHttp(signaling);
        connections.add(connection);
        return connection;
    }

    @Test
    void statusRequestKeepsTheConnectionForTheJoin() throws Exception {
        EmbeddedHttp http = connect();

        http.send(get("/v1/join"));
        Response status = http.response();
        assertEquals(200, status.status());
        assertFalse(status.closes(), "the join may follow on this connection");
        assertTrue(http.isOpen());

        http.send(post("42", OFFER));
        Join join = sink.next();
        assertEquals("42", join.remoteNetworkId());
        assertEquals(OFFER, join.offer());
        join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS);

        Response answer = http.response();
        assertEquals(200, answer.status());
        assertEquals("application/sdp", answer.headers().get("content-type"));
        assertEquals(ANSWER, answer.body());
        assertTrue(answer.closes(), "one offer per connection");
        assertFalse(http.isOpen());
    }

    @Test
    void joinPipelinedBehindTheStatusRequestWaitsForItsResponse() throws Exception {
        List<Runnable> calls = new ArrayList<>();
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker)
                .statusProvider(request -> StatusAnswer.serve(NetherNetServerStatus.builder().name("A").build()))
                .statusExecutor(calls::add));
        http.send(get("/v1/join") + post("42", OFFER));
        assertTrue(sink.joins.isEmpty(), "the join waits until the status response is written");
        assertTrue(http.responses().isEmpty());

        calls.getFirst().run();
        http.channel.runPendingTasks();

        Join join = sink.next();
        join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS);
        List<Response> responses = http.responses();
        assertEquals(List.of(200, 200), responses.stream().map(Response::status).toList());
        assertEquals("{\"name\":\"A\"}", responses.get(0).body());
        assertFalse(responses.get(0).closes());
        assertEquals(ANSWER, responses.get(1).body());
        assertTrue(responses.get(1).closes());
    }

    @Test
    void declinedStatusRequestDropsTheJoinPipelinedBehindIt() {
        List<Runnable> calls = new ArrayList<>();
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker)
                .statusProvider(request -> StatusAnswer.decline())
                .statusExecutor(calls::add));
        http.send(get("/v1/join") + post("42", OFFER));

        calls.getFirst().run();
        http.channel.runPendingTasks();

        Response declined = http.response();
        assertEquals(404, declined.status());
        assertTrue(declined.closes());
        assertTrue(sink.joins.isEmpty());
        assertFalse(http.isOpen());
    }

    @Test
    void secondStatusRequestIsTheLast() {
        EmbeddedHttp http = connect();

        ByteBuf input = http.send(get("/v1/join") + get("/v1/join") + get("/v1/join"));

        List<Response> responses = http.responses();
        assertEquals(2, responses.size(), "the third request is never read");
        assertFalse(responses.get(0).closes());
        assertTrue(responses.get(1).closes());
        assertEquals(0, input.refCnt());
        assertFalse(http.isOpen());
    }

    @Test
    void clientAskingToCloseGetsItsStatusAndTheClose() {
        EmbeddedHttp http = connect();

        http.send(get("/v1/join", "Connection: close\r\n"));

        Response status = http.response();
        assertEquals(200, status.status());
        assertTrue(status.closes());
        assertFalse(http.isOpen());
    }

    @Test
    void anyOtherFirstRequestIsTheOnlyOne() {
        EmbeddedHttp http = connect();

        http.send(get("/v1/join/42"));

        Response response = http.response();
        assertEquals(404, response.status(), "vanilla answers 404 for a GET with an ID");
        assertTrue(response.closes());
        assertFalse(http.isOpen());
    }

    @Test
    void keptConnectionClosesWhenNoRequestStartsWithinTheGap() {
        EmbeddedHttp http = connect();
        http.send(get("/v1/join"));
        http.responses();

        http.advance(TimeUnit.SECONDS.toMillis(HttpRequestBudget.GAP_SECONDS) - 1);
        assertTrue(http.isOpen());
        http.advance(1);

        assertFalse(http.isOpen());
        assertTrue(http.responses().isEmpty(), "closed unanswered");
    }

    @Test
    void secondRequestHasItsOwnAbsoluteDeadline() {
        EmbeddedHttp http = connect();
        http.send(get("/v1/join"));
        http.responses();

        http.send("POST /v1/join/42 HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 100\r\n\r\nv=0");
        http.advance(TimeUnit.SECONDS.toMillis(HttpRequestBudget.REQUEST_SECONDS) - 1);
        assertTrue(http.isOpen(), "the gap only bounds when the request starts");
        http.advance(1);

        assertFalse(http.isOpen());
        assertTrue(sink.joins.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void slowRequestIsClosedAtTheAbsoluteDeadline(boolean bodyStarted) {
        EmbeddedHttp http = connect();
        long step = TimeUnit.SECONDS.toMillis(HttpRequestBudget.REQUEST_SECONDS) / 4;

        // A byte now and then resets an idle timer, never this deadline
        ByteBuf input = http.send("POST /v1/join/42 HTTP/1.1\r\n");
        http.advance(step);
        http.send("Host: localhost\r\nContent-Length: 100\r\n");
        http.advance(step);
        http.send(bodyStarted ? "\r\nv=0" : "X-Drip: 1\r\n");
        http.advance(step);
        assertTrue(http.isOpen());
        http.advance(TimeUnit.SECONDS.toMillis(HttpRequestBudget.REQUEST_SECONDS) - 3 * step);

        assertFalse(http.isOpen());
        assertEquals(0, input.refCnt());
        assertTrue(sink.joins.isEmpty());
    }

    @Test
    void pendingAnswerKeepsTheConnectionUntilTheNegotiationDeadline() throws Exception {
        EmbeddedHttp http = connect();
        http.send(post("42", OFFER));
        Join join = sink.next();

        http.advance(NetherNetHttpSignaling.DEFAULT_NEGOTIATION_TIMEOUT.toMillis() - 1);
        assertTrue(http.isOpen(), "no idle timer closes a connection whose answer is pending");
        assertTrue(http.responses().isEmpty());

        join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS);
        assertEquals(200, http.response().status());
        assertFalse(join.reply().abandoned().toCompletableFuture().isDone());
    }

    @Test
    void negotiationDeadlineAnswers504AndAbandonsTheJoin() throws Exception {
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker)
                .negotiationTimeout(Duration.ofSeconds(30)));
        http.send(post("42", OFFER));
        Join join = sink.next();

        http.advance(TimeUnit.SECONDS.toMillis(30));

        Response timedOut = http.response();
        assertEquals(JoinRefusal.NO_ANSWER_IN_TIME.httpStatus(), timedOut.status());
        assertTrue(timedOut.body().isEmpty());
        assertFalse(http.isOpen());
        assertTrue(join.reply().abandoned().toCompletableFuture().isDone());
        ExecutionException late = assertThrows(ExecutionException.class,
                () -> join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, late.getCause());
    }

    @Test
    void negotiationDeadlineOfAPipelinedJoinCountsFromItsArrival() throws Exception {
        List<Runnable> calls = new ArrayList<>();
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker)
                .statusProvider(request -> StatusAnswer.serve(NetherNetServerStatus.builder().name("A").build()))
                .statusExecutor(calls::add));
        http.send(get("/v1/join") + post("42", OFFER));
        http.advance(900);
        calls.getFirst().run();
        http.channel.runPendingTasks();
        Join join = sink.next();
        assertEquals(1, http.responses().size(), "the status response");

        http.advance(NetherNetHttpSignaling.DEFAULT_NEGOTIATION_TIMEOUT.toMillis() - 901);
        assertTrue(http.responses().isEmpty());
        http.advance(1);

        assertEquals(JoinRefusal.NO_ANSWER_IN_TIME.httpStatus(), http.response().status(),
                "the wait behind the status request is part of the deadline");
        assertTrue(join.reply().abandoned().toCompletableFuture().isDone());
    }

    @Test
    void pipelinedJoinWhoseDeadlinePassedBehindTheStatusRequestNeverReachesTheEndpoint() {
        List<Runnable> calls = new ArrayList<>();
        // An endpoint that answers at once would otherwise beat the expired deadline
        sink.onJoin = join -> join.reply().answer(ANSWER);
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker)
                .statusProvider(request -> StatusAnswer.serve(NetherNetServerStatus.builder().name("A").build()))
                .statusExecutor(calls::add)
                .negotiationTimeout(Duration.ofMillis(100)));
        http.send(get("/v1/join") + post("42", OFFER));
        http.advance(200);
        calls.getFirst().run();
        http.channel.runPendingTasks();

        List<Response> responses = http.responses();
        assertEquals(List.of(200, JoinRefusal.NO_ANSWER_IN_TIME.httpStatus()),
                responses.stream().map(Response::status).toList());
        assertTrue(responses.get(1).closes());
        assertTrue(sink.delivered.isEmpty(), "an offer past its negotiation deadline is not started");
    }

    @Test
    void clientLeavingAbandonsTheJoin() throws Exception {
        EmbeddedHttp http = connect();
        http.send(post("42", OFFER));
        Join join = sink.next();
        CompletionStage<Void> abandoned = join.reply().abandoned();

        http.channel.close();
        http.channel.runPendingTasks();

        assertTrue(abandoned.toCompletableFuture().isDone());
        assertThrows(ExecutionException.class,
                () -> join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS));
        http.advance(NetherNetHttpSignaling.DEFAULT_NEGOTIATION_TIMEOUT.toMillis());
        assertTrue(http.responses().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(JoinRefusal.class)
    void refusalsAnswerTheirStatusWithAnEmptyBody(JoinRefusal refusal) throws Exception {
        EmbeddedHttp http = connect();
        http.send(post("42", OFFER));

        sink.next().reply().refuse(refusal).toCompletableFuture().get(1, TimeUnit.SECONDS);

        Response response = http.response();
        assertEquals(refusal.httpStatus(), response.status());
        assertTrue(response.body().isEmpty(), "internal reasons stay internal");
        assertTrue(response.closes());
        assertFalse(http.isOpen());
    }

    @Test
    void secondReplyFails() throws Exception {
        EmbeddedHttp http = connect();
        http.send(post("42", OFFER));
        Join join = sink.next();

        join.reply().refuse(JoinRefusal.CAPACITY).toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertThrows(ExecutionException.class,
                () -> join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS));
        assertFalse(join.reply().abandoned().toCompletableFuture().isDone(), "a refused join was not abandoned");
        assertEquals(1, http.responses().size());
    }

    @Test
    void joinCarriesItsSourceAndWhatTheClientReached() throws Exception {
        EmbeddedHttp http = connect();
        http.send(post("42", OFFER));

        Join join = sink.next();
        assertEquals(EmbeddedHttp.CLIENT, join.source().address());
        assertEquals(AddressSource.SIGNALING, join.source().source());
        assertEquals(new Join.Addressed("127.0.0.1:19132", EmbeddedHttp.LOCAL.getPort(), false), join.addressed());
        assertFalse(join.reply().trickles());
        assertThrows(UnsupportedOperationException.class, () -> join.reply().candidate("candidate:1"));
        assertTrue(join.connectionId() > 0);
    }

    @Test
    void endpointFailingToTakeAJoinRefusesIt() {
        sink.onJoin = join -> {
            throw new IllegalStateException("broken endpoint");
        };
        EmbeddedHttp http = connect();

        http.send(post("42", OFFER));

        assertEquals(JoinRefusal.INTERNAL.httpStatus(), http.response().status());
        assertFalse(http.isOpen());
    }

    @Test
    void aggregatorAnswersEndTheConnection() {
        EmbeddedHttp expectation = connect();
        expectation.send(get("/v1/join", "Expect: x\r\n"));
        Response refused = expectation.response();
        assertEquals(417, refused.status());
        assertTrue(refused.closes());
        assertFalse(expectation.isOpen(), "a 417 used to leave the connection open with no timer");

        EmbeddedHttp oversized = connect();
        oversized.send("POST /v1/join/42 HTTP/1.1\r\nHost: localhost\r\nContent-Length: "
                + (NetherNetHttpSignaling.DEFAULT_MAX_REQUEST_BODY_BYTES + 1) + "\r\n\r\n");
        Response tooLarge = oversized.response();
        assertEquals(413, tooLarge.status());
        assertTrue(tooLarge.closes());
        assertFalse(oversized.isOpen());
        assertTrue(sink.joins.isEmpty());
    }

    @Test
    void bodyCapIsConfigurable() {
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker).maxRequestBodyBytes(8));

        http.send(post("42", "v=0\r\no=- 1"));

        assertEquals(413, http.response().status());
        assertTrue(sink.joins.isEmpty());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "GET /|404",
            "GET /v1|404",
            "POST /v1/join|404",
            "PUT /v1/join/42|404",
            "POST /v1/joining/42|404",
            "POST /v1/join/|400",
            "POST /v1/join/42/extra|400"
    })
    void routesItDoesNotServe(String request, int status) {
        EmbeddedHttp http = connect();
        String[] parts = request.split(" ");

        http.send(parts[0] + " " + parts[1] + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nv=0");

        Response response = http.response();
        assertEquals(status, response.status());
        assertTrue(response.closes());
        assertTrue(sink.joins.isEmpty());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "not-a-number|not-a-number",
            "c99b1a2e-89d6-4a88-b031-571f5236d64c|c99b1a2e-89d6-4a88-b031-571f5236d64c",
            "18446744073709551616|18446744073709551616",
            "00042|00042",
            "Peer:Name|Peer:Name",
            "peer+name|peer+name",
            "peer%2Bname|peer+name",
            "peer%20name|peer name",
            "peer%2Fname|peer/name",
            "peer%252Fname|peer%2Fname",
            "peer%3Fx%23y%25z?ignored=1|peer?x#y%z",
            "%E7%8E%A9%E5%AE%B6%F0%9F%8E%AE|玩家🎮",
            "%EF%BF%BD|�"
    })
    void networkIdsAreDecodedOnceAndPreserved(String encoded, String expected) throws Exception {
        EmbeddedHttp http = connect();

        http.send(post(encoded, OFFER));

        assertEquals(expected, sink.next().remoteNetworkId());
    }

    @ParameterizedTest
    @ValueSource(strings = {"%", "%2", "%GG", "peer/child", "peer#fragment", "%FF", "%C3", "%C0%AF", "%ED%A0%80"})
    void malformedNetworkIdsAreRefused(String encoded) {
        EmbeddedHttp http = connect();

        http.send(post(encoded, OFFER));

        assertEquals(400, http.response().status());
        assertTrue(sink.joins.isEmpty());
    }

    @Test
    void emptyOfferIsRefused() {
        EmbeddedHttp http = connect();

        http.send(post("42", ""));

        assertEquals(400, http.response().status());
        assertTrue(sink.joins.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("pipelinedBehindAnOffer")
    void inputBehindAnOfferIsNeverRead(boolean separateRead, String extra) throws Exception {
        EmbeddedHttp http = connect();
        String offer = post("42", OFFER);
        ByteBuf first = http.send(separateRead ? offer : offer + extra);
        ByteBuf second = separateRead ? http.send(extra) : null;

        Join join = sink.next();
        assertTrue(sink.joins.isEmpty());
        assertTrue(http.isOpen());
        assertEquals(0, first.refCnt());
        if (second != null) {
            assertEquals(0, second.refCnt());
        }

        join.reply().answer(ANSWER).toCompletableFuture().get(1, TimeUnit.SECONDS);
        List<Response> responses = http.responses();
        assertEquals(1, responses.size());
        assertEquals(ANSWER, responses.getFirst().body());
    }

    private static Stream<Arguments> pipelinedBehindAnOffer() {
        return Stream.of(
                        get("/v1/join"),
                        post("99", "second offer"),
                        "POST /v1/join/99 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2000000\r\n\r\n",
                        "not an HTTP request\r\n\r\n")
                .flatMap(request -> Stream.of(Arguments.of(false, request), Arguments.of(true, request)));
    }

    @Test
    void nothingIsServedBeforeTheEndpointStartsTheSource() {
        signaling = NetherNetHttpSignaling.builder(worker).build();
        EmbeddedHttp status = new EmbeddedHttp(signaling);
        EmbeddedHttp offer = new EmbeddedHttp(signaling);
        connections.add(status);
        connections.add(offer);

        status.send(get("/v1/join"));
        offer.send(post("42", OFFER));

        assertEquals(404, status.response().status(), "declined, so a client can still fall back");
        assertEquals(503, offer.response().status());
    }

    @Test
    void plainHttpIsRefusedWhenTlsIsRequired() {
        EmbeddedHttp status = connect(NetherNetHttpSignaling.builder(worker).requiresTls(true));
        EmbeddedHttp offer = connect();

        status.send(get("/v1/join"));
        offer.send(post("42", OFFER));

        Response refused = status.response();
        assertEquals(426, refused.status());
        assertEquals("TLS/1.2, HTTP/1.1", refused.headers().get("upgrade"), "a 426 names what to upgrade to");
        assertTrue(refused.closes());
        assertEquals(426, offer.response().status());
        assertTrue(sink.joins.isEmpty(), "no peer is made for a refused offer");
        assertNull(signaling.tlsSelector(), "refused although no certificate is available at all");
    }
}

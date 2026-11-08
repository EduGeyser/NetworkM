package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NetherNetXboxRpcSignalingTest {
    /** What the service sends "From" a player who was offline when a message to them came in. */
    private static final String NOT_DELIVERED = "{\"Code\":2,\"Message\":\"Message to player could not be delivered.\"}";
    /** A host's pmid, as the service addresses players. */
    private static final String HOST = "0f8fad5b-d9cb-469f-a165-70867728950e";
    /** Where the supervisor runs; its clock decides backoff and connection age. */
    private final EmbeddedChannel loop = new EmbeddedChannel();
    private final List<Boolean> tokenRequests = new ArrayList<>();
    private CompletableFuture<String> pendingToken;
    private boolean tokenCarriesPlayerId = true;
    private final Signaling signaling = new Signaling();
    private final RecordingSink sink = new RecordingSink();

    {
        loop.freezeTime();
    }

    @AfterEach
    void tearDown() {
        signaling.close();
        run();
        signaling.sockets.forEach(EmbeddedChannel::finishAndReleaseAll);
        loop.finishAndReleaseAll();
    }

    // Starting, reconnecting and tokens.

    @Test
    void startReturnsAtOnceWhileXboxIsUnreachable() {
        pendingToken = new CompletableFuture<>();

        signaling.start(sink);
        run();

        assertEquals(SignalingState.STARTING, signaling.health().state());
        assertTrue(signaling.sockets.isEmpty());
        CompletableFuture<List<IceServer>> client = signaling.connect(null);
        run();
        assertFalse(client.isDone());

        pendingToken.complete(token(1));
        run();
        handshake(signaling.sockets.getFirst(), turnResult(0, "turn:a.invalid"));
        assertEquals(List.of("turn:a.invalid"), client.join().getFirst().urls());
    }

    @Test
    void aSocketClosedAfterAStableConnectionIsReplacedAtOnceWithAFreshToken() {
        EmbeddedChannel first = connect(turnResult(0));
        loop.advanceTimeBy(SignalingSupervisor.STABLE_AFTER.toSeconds(), TimeUnit.SECONDS);
        AtomicInteger handlerCalls = new AtomicInteger();
        signaling.setSignalHandler("7", "peer", signal -> handlerCalls.incrementAndGet());

        first.writeInbound(new CloseWebSocketFrame(1001, "Signaling server instance is shutting down."));
        run();

        assertFalse(first.isOpen());
        assertEquals(2, signaling.sockets.size(), "The replacement opened without a backoff wait");
        assertEquals(List.of(false, false), tokenRequests);
        assertEquals(token(2), signaling.tokensUsed.getLast());
        assertEquals(SignalingState.RECONNECTING, signaling.health().state());
        assertTrue(signaling.health().lastFailure().getMessage().contains("1001"));

        EmbeddedChannel second = signaling.sockets.getLast();
        handshake(second, turnResult(0));
        assertEquals(SignalingState.CONNECTED, signaling.health().state());
        receiveWebRtc(second, "CANDIDATEADD 7 candidate", false);
        assertEquals(1, handlerCalls.get(), "Handlers registered before the reconnect keep working");
    }

    @Test
    void aRefusedTokenIsRefreshedOnTheNextAttempt() {
        signaling.start();
        run();
        EmbeddedChannel first = signaling.sockets.getFirst();

        first.pipeline().fireExceptionCaught(new WebSocketClientHandshakeException("Invalid handshake response",
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED)));
        run();

        assertInstanceOf(SignalingSupervisor.TokenRejectedException.class, signaling.health().lastFailure());
        assertEquals(1, signaling.health().failedAttempts());
        advanceLoop(signaling.health().retryIn());
        assertEquals(List.of(false, true), tokenRequests);
        assertEquals(2, signaling.sockets.size());
    }

    @Test
    void manualReconnectReplacesTheSocket() {
        EmbeddedChannel first = connect(turnResult(0));

        CompletableFuture<Void> reconnected = signaling.reconnect();
        run();

        assertFalse(first.isOpen());
        assertEquals(2, signaling.sockets.size());
        handshake(signaling.sockets.getLast(), turnResult(0));
        assertTrue(reconnected.isDone());
        assertFalse(reconnected.isCompletedExceptionally());
    }

    // Liveness.

    @Test
    void anyInboundFrameKeepsTheSocketAliveAndSilenceReplacesIt() {
        EmbeddedChannel socket = connect(turnResult(0));

        keepAlive(socket, 120);
        assertEquals(SignalingState.CONNECTED, signaling.health().state());

        advanceSocket(socket, RpcLimits.SILENCE + RpcLimits.CHECK);

        assertEquals(SignalingState.RECONNECTING, signaling.health().state());
        assertInstanceOf(TimeoutException.class, signaling.health().lastFailure());
        assertFalse(socket.isOpen());
    }

    @Test
    void twoUnansweredSystemPingsReplaceTheSocket() {
        EmbeddedChannel socket = connect(turnResult(0));

        for (int step = 0; step < 24 && signaling.health().state() == SignalingState.CONNECTED; step++) {
            advanceSocket(socket, 5);
            drain(socket);
            if (socket.isOpen()) {
                socket.writeInbound(new PongWebSocketFrame());
            }
            run();
        }

        assertEquals(SignalingState.RECONNECTING, signaling.health().state());
        assertTrue(signaling.health().lastFailure().getMessage().contains("System_Ping"));
    }

    @Test
    void aRejectedRouteProbeAfterProofReplacesTheSocket() {
        EmbeddedChannel socket = connect(turnResult(0));
        List<JsonObject> sent = keepAlive(socket, 30);
        assertEquals(1, routeProbes(sent).size());
        receiveFromSelf(socket, NetherNetConstants.XBOX_RPC_INNER_METHOD_ROUTE_PROBE);

        JsonObject probe = routeProbes(keepAlive(socket, 30)).getFirst();
        JsonObject error = new JsonObject();
        error.addProperty("message", "Player not registered");
        respond(socket, probe.get("id").getAsString(), "error", error);
        run();

        assertEquals(SignalingState.RECONNECTING, signaling.health().state());
        assertTrue(signaling.health().lastFailure().getMessage().contains("route probe rejected"));
    }

    @Test
    void routeSilenceAfterProofReplacesTheSocket() {
        EmbeddedChannel socket = connect(turnResult(0));
        keepAlive(socket, 30);
        receiveFromSelf(socket, NetherNetConstants.XBOX_RPC_INNER_METHOD_ROUTE_PROBE);

        keepAlive(socket, 85);
        assertEquals(SignalingState.CONNECTED, signaling.health().state());
        keepAlive(socket, 10);

        assertEquals(SignalingState.RECONNECTING, signaling.health().state());
        assertTrue(signaling.health().lastFailure().getMessage().contains("route probe"));
    }

    @Test
    void routeProbeTimeoutDoesNotDisableLaterProbesOrReportNotFound() {
        AtomicInteger notFound = new AtomicInteger();
        signaling.setNotFoundHandler(message -> notFound.incrementAndGet());
        EmbeddedChannel socket = connect(turnResult(0));

        assertEquals(1, routeProbes(keepAlive(socket, 30)).size());
        assertEquals(0, routeProbes(keepAlive(socket, 25)).size());
        assertEquals(1, routeProbes(keepAlive(socket, 5)).size(), "A lost reply must not disable the self-probe");
        assertEquals(0, notFound.get());
        assertEquals(SignalingState.CONNECTED, signaling.health().state());
    }

    // TURN credentials.

    @ParameterizedTest
    @CsvSource({"0, 1800", "-5, 1800", "600, 300", "10, 5", "1, 1", "30, 15", "86400, 1800"})
    void turnCredentialsAreRefreshedHalfwayThroughTheirLifetime(long lifetime, long refreshAfter) {
        assertEquals(refreshAfter, NetherNetXboxRpcSignaling.turnRefreshDelaySeconds(lifetime));
    }

    @Test
    void theStatedLifetimeSchedulesTheRefresh() {
        tokenCarriesPlayerId = false;
        EmbeddedChannel socket = connect(turnResult(600, "turn:a.invalid"));
        assertEquals(List.of("turn:a.invalid"), signaling.iceServers().getFirst().urls());

        assertTrue(turnRequests(keepAlive(socket, 295)).isEmpty());
        JsonObject refresh = turnRequests(keepAlive(socket, 5)).getFirst();
        respond(socket, refresh.get("id").getAsString(), "result", turnResult(0, "turn:b.invalid"));
        run();

        assertEquals(List.of("turn:b.invalid"), signaling.iceServers().getFirst().urls());
        assertTrue(turnRequests(keepAlive(socket, 1795)).isEmpty());
        assertEquals(1, turnRequests(keepAlive(socket, 5)).size());
    }

    @Test
    void anEmptyRefreshRetriesBeforeTheKeptCredentialsExpire() {
        tokenCarriesPlayerId = false;
        EmbeddedChannel socket = connect(turnResult(600, "turn:a.invalid"));

        JsonObject refresh = turnRequests(keepAlive(socket, 300)).getFirst();
        respond(socket, refresh.get("id").getAsString(), "result", turnResult(0));
        run();

        assertEquals(List.of("turn:a.invalid"), signaling.iceServers().getFirst().urls());
        assertTrue(turnRequests(keepAlive(socket, 55)).isEmpty());
        assertEquals(1, turnRequests(keepAlive(socket, 5)).size());
    }

    @Test
    void joinsWaitForTheFirstTurnCredentialsAndKeepTheirSignalOrder() {
        List<String> events = new ArrayList<>();
        signaling.start(new JoinSink() {
            @Override
            public void join(Join join) {
                events.add("join " + join.offer() + " with " + signaling.iceServers().size() + " TURN servers");
                join.reply().onRemoteSignal(signal -> events.add(signal.toString()));
            }

            @Override
            public boolean admits(SourcedAddress source) {
                return true;
            }
        });
        run();
        EmbeddedChannel socket = signaling.sockets.getLast();
        socket.pipeline().fireUserEventTriggered(WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE);
        JsonObject turn = turnRequests(drain(socket)).getFirst();

        receiveWebRtc(socket, "CONNECTREQUEST 9 offer", false);
        receiveWebRtc(socket, "CANDIDATEADD 9 candidate", false);
        assertEquals(List.of(), events);

        respond(socket, turn.get("id").getAsString(), "result", turnResult(0, "turn:a.invalid"));
        run();

        assertEquals(List.of("join offer with 1 TURN servers", "Candidate[candidate=candidate]"), events);
    }

    @Test
    void aFailedOrEmptyRefreshKeepsTheCredentialsAndRetries() {
        tokenCarriesPlayerId = false;
        EmbeddedChannel socket = connect(turnResult(120, "turn:a.invalid"));

        JsonObject failed = turnRequests(keepAlive(socket, 60)).getFirst();
        JsonObject error = new JsonObject();
        error.addProperty("message", "unavailable");
        respond(socket, failed.get("id").getAsString(), "error", error);
        run();
        JsonObject retry = turnRequests(keepAlive(socket, 60)).getFirst();
        respond(socket, retry.get("id").getAsString(), "result", turnResult(120));
        run();

        assertEquals(List.of("turn:a.invalid"), signaling.iceServers().getFirst().urls());
        assertEquals(SignalingState.CONNECTED, signaling.health().state());
    }

    @Test
    void aMalformedTurnEntryDoesNotDropTheOthers() {
        JsonObject result = turnResult(0, "turn:a.invalid");
        result.getAsJsonArray("TurnAuthServers").add("not an object");
        connect(result);

        assertEquals(1, signaling.iceServers().size());
    }

    // Messages.

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deliveryReceiptsDoNotGenerateMoreDeliveryReceipts(boolean requestHasId) {
        EmbeddedChannel socket = connect(turnResult(0));

        receive(socket, "peer", NetherNetConstants.XBOX_RPC_INNER_METHOD_DELIVERY, new JsonObject(), requestHasId, true);

        List<JsonObject> sent = drain(socket);
        if (requestHasId) {
            JsonObject response = sent.removeFirst();
            assertEquals("request-1", response.get("id").getAsString());
            assertTrue(response.get("result").isJsonNull());
            assertFalse(response.has("method"));
        }
        assertEquals(List.of(), sent, "A receipt must not trigger Signaling_SendClientMessage");
    }

    /** A receipt for a non-delivery notice goes to the offline player, which brings the next notice. */
    @ParameterizedTest
    @ValueSource(strings = {NOT_DELIVERED, "{\"jsonrpc\":\"2.0\",\"method\":\"RouteProbe_v1_0\",\"params\":{}}",
            "{\"jsonrpc\":\"2.0\",\"method\":\"Unknown_v1_0\",\"params\":{}}", "not json"})
    void onlyWebRtcSignalsGetAReceipt(String message) {
        EmbeddedChannel socket = connect(turnResult(0));

        receiveMessage(socket, "peer", "message-1", message);

        List<JsonObject> sent = drain(socket);
        assertEquals(1, sent.size(), "Only the RPC result, no Signaling_SendClientMessage");
        assertFalse(sent.getFirst().has("method"));
    }

    @Test
    void aJoinWhoseAnswerCouldNotBeDeliveredIsAbandoned() {
        signaling.start(sink);
        EmbeddedChannel socket = connect(turnResult(0));
        receiveWebRtc(socket, "CONNECTREQUEST 9 offer", false);
        drain(socket);
        Join join = sink.delivered.getFirst();
        CompletableFuture<Void> abandoned = join.reply().abandoned().toCompletableFuture();
        join.reply().answer("v=0");
        join.reply().candidate("candidate:1");
        List<JsonObject> sent = drain(socket);
        String answer = messageId(sent.get(0));
        String candidate = messageId(sent.get(1));

        receiveMessage(socket, "intruder", answer, NOT_DELIVERED);
        receiveMessage(socket, "peer", candidate, NOT_DELIVERED);
        assertFalse(abandoned.isDone(), "Only the client's notice for the answer abandons the join");

        receiveMessage(socket, "peer", answer, NOT_DELIVERED);
        assertTrue(abandoned.isDone());
    }

    @Test
    void aJoinThatWentActiveForgetsItsMessages() {
        signaling.start(sink);
        EmbeddedChannel socket = connect(turnResult(0));
        receiveWebRtc(socket, "CONNECTREQUEST 9 offer", false);
        drain(socket);
        Join join = sink.delivered.getFirst();
        CompletableFuture<Void> abandoned = join.reply().abandoned().toCompletableFuture();
        join.reply().answer("v=0");
        String answer = messageId(drain(socket).getFirst());

        join.reply().connected();
        receiveMessage(socket, "peer", answer, NOT_DELIVERED);

        assertFalse(abandoned.isDone(), "A notice that arrives after the connection went active ends nothing");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void webRtcMessagesReachTheirHandlerAndGetAReceipt(boolean batched) {
        EmbeddedChannel socket = connect(turnResult(0));
        AtomicReference<Signal> received = new AtomicReference<>();
        signaling.setSignalHandler("42", "peer", received::set);

        receiveWebRtc(socket, "CANDIDATEADD 42 candidate", batched);

        assertEquals(new Signal(SignalType.CANDIDATE_ADD, "42", "candidate"), received.get());
        List<JsonObject> sent = drain(socket);
        assertEquals(2, sent.size());
        assertEquals("request-1", sent.getFirst().get("id").getAsString());
        JsonObject receipt = sent.get(1);
        assertEquals(NetherNetConstants.XBOX_RPC_METHOD_SEND_MESSAGE, receipt.get("method").getAsString());
        assertEquals(NetherNetConstants.XBOX_RPC_INNER_METHOD_DELIVERY, innerMethod(receipt));
    }

    @Test
    void aClientAttemptTakesSignalsOnlyFromThePeerItsOfferWentTo() {
        EmbeddedChannel socket = connect(turnResult(0));
        AtomicReference<Signal> received = new AtomicReference<>();
        signaling.setSignalHandler("42", "peer", received::set);

        receive(socket, "other", NetherNetConstants.XBOX_RPC_INNER_METHOD_WEBRTC, webRtcParams("CONNECTERROR 42 1"),
                true, false);
        assertNull(received.get(), "A third party that knows the connection ID must not fail the attempt");

        receiveWebRtc(socket, "CONNECTERROR 42 1", false);
        assertEquals(new Signal(SignalType.CONNECT_ERROR, "42", "1"), received.get());
    }

    @ParameterizedTest
    @CsvSource({"-32602, Deserializing JSON-RPC argument failed, true",
            "-32000, Player not registered with the service., false"})
    void aRefusedOfferFailsItsAttemptAtOnceUnlessTheHostIsNotFound(int code, String message, boolean fails) {
        AtomicInteger notFound = new AtomicInteger();
        signaling.setNotFoundHandler(reason -> notFound.incrementAndGet());
        EmbeddedChannel socket = connect(turnResult(0));
        List<SignalingFailure> failures = new ArrayList<>();
        signaling.setSignalHandler("42", HOST, attemptFailures(failures));

        signaling.sendSignal(HOST, new Signal(SignalType.CONNECT_REQUEST, "42", "v=0"));
        JsonObject offer = drain(socket).getFirst();
        JsonObject error = new JsonObject();
        error.addProperty("code", code);
        error.addProperty("message", message);
        respond(socket, offer.get("id").getAsString(), "error", error);
        run();

        if (fails) {
            assertEquals(1, failures.size());
            assertEquals("the signaling service refused it: " + message, failures.getFirst().reason());
        } else {
            assertEquals(List.of(), failures, "Only the not-found handler hears of an unknown host");
            assertEquals(1, notFound.get());
        }
    }

    @Test
    void anOfferToANetworkIdFailsItsAttemptWithoutReachingTheService() {
        EmbeddedChannel socket = connect(turnResult(0));
        List<SignalingFailure> failures = new ArrayList<>();
        signaling.setSignalHandler("42", "468682033508464609", attemptFailures(failures));

        signaling.sendSignal("468682033508464609", new Signal(SignalType.CONNECT_REQUEST, "42", "v=0"));
        signaling.sendSignal("468682033508464609", new Signal(SignalType.CANDIDATE_ADD, "42", "candidate:1"));

        assertEquals(List.of(), drain(socket));
        assertEquals(1, failures.size());
        assertTrue(failures.getFirst().reason().contains("pmid"), failures.getFirst().reason());
    }

    @Test
    void aHostDialedByAnUpperCasePmidReachesTheAttempt() {
        EmbeddedChannel socket = connect(turnResult(0));
        AtomicReference<Signal> received = new AtomicReference<>();
        signaling.setSignalHandler("42", HOST.toUpperCase(Locale.ROOT), received::set);

        receive(socket, HOST, NetherNetConstants.XBOX_RPC_INNER_METHOD_WEBRTC, webRtcParams("CONNECTRESPONSE 42 v=0"),
                true, false);

        assertEquals(new Signal(SignalType.CONNECT_RESPONSE, "42", "v=0"), received.get());
    }

    @Test
    void aJoinRepliesToItsSenderAndRefusesWithTheErrorCode() {
        signaling.start(sink);
        EmbeddedChannel socket = connect(turnResult(0));
        receiveWebRtc(socket, "CONNECTREQUEST 9 offer", false);
        drain(socket);

        Join join = sink.delivered.getFirst();
        assertEquals("9", join.connectionId());
        assertEquals("peer", join.remoteNetworkId());
        assertEquals("offer", join.offer());
        assertEquals(AddressSource.UNKNOWN, join.source().source());
        assertTrue(join.reply().trickles());

        CompletableFuture<Void> answered = join.reply().answer("v=0").toCompletableFuture();
        join.reply().candidate("candidate:1");
        join.reply().refuse(JoinRefusal.CAPACITY);
        List<JsonObject> sent = drain(socket);
        assertEquals(List.of("CONNECTRESPONSE 9 v=0", "CANDIDATEADD 9 candidate:1", "CONNECTERROR 9 16"),
                sent.stream().map(NetherNetXboxRpcSignalingTest::webRtcMessage).toList());
        sent.forEach(request -> assertEquals("peer",
                request.getAsJsonObject("params").get("toPlayerId").getAsString()));

        assertFalse(answered.isDone(), "Handed on once the service accepted it");
        respond(socket, sent.getFirst().get("id").getAsString(), "result", new JsonObject());
        run();
        assertTrue(answered.isDone() && !answered.isCompletedExceptionally());
    }

    @Test
    void signalsReachOnlyTheJoinOfTheirSenderUntilItFinishes() {
        signaling.start(sink);
        EmbeddedChannel socket = connect(turnResult(0));
        receiveWebRtc(socket, "CANDIDATEADD 9 early", false);
        receiveWebRtc(socket, "CONNECTREQUEST 9 offer", false);
        receiveWebRtc(socket, "CANDIDATEADD 9 before-the-endpoint-listens", false);
        Join join = sink.delivered.getFirst();
        List<String> signals = RecordingSink.signalsOf(join);

        receive(socket, "intruder", NetherNetConstants.XBOX_RPC_INNER_METHOD_WEBRTC,
                webRtcParams("CANDIDATEADD 9 forged"), true, false);
        receiveWebRtc(socket, "CONNECTREQUEST 9 again", false);
        receiveWebRtc(socket, "CONNECTERROR 9 2", false);
        assertEquals(List.of("candidate before-the-endpoint-listens", "error 2"), signals);
        assertEquals(1, sink.delivered.size(), "A repeated offer is no new join");

        join.reply().finished();
        receiveWebRtc(socket, "CANDIDATEADD 9 late", false);
        assertEquals(2, signals.size());
        receiveWebRtc(socket, "CONNECTREQUEST 9 retry", false);
        assertEquals("retry", sink.delivered.getLast().offer());
    }

    @ParameterizedTest
    @ValueSource(strings = {"00042", "pfab-7Q:x~!"})
    void aConnectionIdIsMatchedAndEchoedExactlyAsWritten(String id) {
        signaling.start(sink);
        EmbeddedChannel socket = connect(turnResult(0));
        receiveWebRtc(socket, "CONNECTREQUEST " + id + " offer", false);
        receiveWebRtc(socket, "CANDIDATEADD " + id + " candidate:1", false);
        drain(socket);

        Join join = sink.delivered.getFirst();
        assertEquals(id, join.connectionId());
        assertEquals(List.of("candidate candidate:1"), RecordingSink.signalsOf(join));
        join.reply().answer("v=0");
        assertEquals(List.of("CONNECTRESPONSE " + id + " v=0"),
                drain(socket).stream().map(NetherNetXboxRpcSignalingTest::webRtcMessage).toList());
    }

    @ParameterizedTest
    @ValueSource(strings = {"CONNECTREQUEST 4\u00072 offer", "CONNECTREQUEST 4é2 offer",
            "CONNECTREQUEST  offer"})
    void aSignalWithAnInvalidConnectionIdIsDropped(String signal) {
        signaling.start(sink);
        EmbeddedChannel socket = connect(turnResult(0));
        receiveWebRtc(socket, signal, false);
        receiveWebRtc(socket, "CONNECTREQUEST " + "1".repeat(65) + " offer", false);
        assertEquals(List.of(), sink.delivered);

        receiveWebRtc(socket, "CONNECTREQUEST " + "1".repeat(64) + " offer", false);
        assertEquals("1".repeat(64), sink.delivered.getFirst().connectionId());
    }

    // Requests.

    @Test
    void unansweredRpcExpiresAndItsLateErrorDoesNotReportNotFound() throws Exception {
        AtomicInteger notFound = new AtomicInteger();
        signaling.setNotFoundHandler(message -> notFound.incrementAndGet());
        EmbeddedChannel socket = connect(turnResult(0));
        CompletableFuture<JsonObject> future = signaling.sendJsonRpcRequest("test", new JsonObject());
        String id = drain(socket).getFirst().get("id").getAsString();

        advanceSocket(socket, 19);
        assertFalse(future.isDone());
        assertEquals(1, pendingRequestCount());
        advanceSocket(socket, 1);

        assertInstanceOf(TimeoutException.class, assertThrows(CompletionException.class, future::join).getCause());
        assertEquals(0, pendingRequestCount());
        JsonObject error = new JsonObject();
        error.addProperty("message", "Player not registered");
        respond(socket, id, "error", error);
        assertEquals(0, notFound.get());
        assertTrue(socket.isOpen());
    }

    @Test
    void successfulReplyCompletesTheRequest() throws Exception {
        EmbeddedChannel socket = connect(turnResult(0));
        CompletableFuture<JsonObject> future = signaling.sendJsonRpcRequest("test", new JsonObject());
        String id = drain(socket).getFirst().get("id").getAsString();
        JsonObject result = new JsonObject();
        result.addProperty("ok", true);

        respond(socket, id, "result", result);
        advanceSocket(socket, 20);

        assertEquals(result, future.join());
        assertEquals(0, pendingRequestCount());
    }

    @Test
    void explicitCurrentNotFoundResponseStillNotifiesOnce() throws Exception {
        AtomicInteger notFound = new AtomicInteger();
        signaling.setNotFoundHandler(message -> notFound.incrementAndGet());
        EmbeddedChannel socket = connect(turnResult(0));
        CompletableFuture<JsonObject> future = signaling.sendJsonRpcRequest("test", new JsonObject());
        String id = drain(socket).getFirst().get("id").getAsString();
        JsonObject error = new JsonObject();
        error.addProperty("message", "Player not registered");

        respond(socket, id, "error", error);
        respond(socket, id, "error", error);

        assertTrue(future.isCompletedExceptionally());
        assertEquals(1, notFound.get());
        assertEquals(0, pendingRequestCount());
    }

    @Test
    void writeFailureFailsTheRequestWithoutWaitingForSocketClose() throws Exception {
        EmbeddedChannel socket = connect(turnResult(0));
        IllegalStateException failure = new IllegalStateException("write rejected");
        AtomicReference<Object> written = new AtomicReference<>();
        socket.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                written.set(message);
                ReferenceCountUtil.release(message);
                promise.setFailure(failure);
            }
        });

        CompletableFuture<JsonObject> future = signaling.sendJsonRpcRequest("test", new JsonObject());

        assertSame(failure, assertThrows(CompletionException.class, future::join).getCause());
        assertEquals(0, ((TextWebSocketFrame) written.get()).refCnt());
        assertEquals(0, pendingRequestCount());
        assertTrue(socket.isOpen());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeFailsPendingRequests(boolean closeSignaling) throws Exception {
        EmbeddedChannel socket = connect(turnResult(0));
        CompletableFuture<JsonObject> future = signaling.sendJsonRpcRequest("test", new JsonObject());

        if (closeSignaling) {
            signaling.close();
        } else {
            socket.close();
        }

        assertInstanceOf(ClosedChannelException.class, assertThrows(CompletionException.class, future::join).getCause());
        assertEquals(0, pendingRequestCount());
    }

    @Test
    void malformedErrorCompletesTheRequest() throws Exception {
        EmbeddedChannel socket = connect(turnResult(0));
        CompletableFuture<JsonObject> future = signaling.sendJsonRpcRequest("test", new JsonObject());
        String id = drain(socket).getFirst().get("id").getAsString();

        respond(socket, id, "error", new JsonArray());

        assertInstanceOf(IllegalArgumentException.class, assertThrows(CompletionException.class, future::join).getCause());
        assertEquals(0, pendingRequestCount());
    }

    // Replaced sockets. Their close is held back to model frames that still arrive after the switch.

    @Test
    void responsesCannotCrossSocketOwnership() {
        EmbeddedChannel previous = connect(turnResult(0));
        holdClose(previous);
        CompletableFuture<JsonObject> previousRequest = signaling.sendJsonRpcRequest("previous", new JsonObject());
        String previousId = drain(previous).getFirst().get("id").getAsString();

        signaling.reconnect();
        run();
        EmbeddedChannel replacement = signaling.sockets.getLast();
        handshake(replacement, turnResult(0));
        CompletableFuture<JsonObject> currentRequest = signaling.sendJsonRpcRequest("current", new JsonObject());
        String currentId = drain(replacement).getFirst().get("id").getAsString();

        respond(previous, currentId, "result", new JsonObject());
        respond(replacement, previousId, "result", new JsonObject());
        assertFalse(previousRequest.isDone());
        assertFalse(currentRequest.isDone());

        respond(replacement, currentId, "result", new JsonObject());
        assertTrue(currentRequest.isDone());
        releaseClose(previous);
        assertInstanceOf(ClosedChannelException.class,
                assertThrows(CompletionException.class, previousRequest::join).getCause());
    }

    @Test
    void aReplacedSocketCannotAffectTheNextAttempt() {
        signaling.start();
        run();
        EmbeddedChannel previous = signaling.sockets.getFirst();
        holdClose(previous);
        previous.pipeline().fireUserEventTriggered(WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE);
        assertEquals(1, turnRequests(drain(previous)).size());
        advanceLoop(SignalingSupervisor.ATTEMPT_TIMEOUT);
        advanceLoop(signaling.health().retryIn());
        EmbeddedChannel replacement = signaling.sockets.getLast();
        assertNotSame(previous, replacement);

        previous.pipeline().fireUserEventTriggered(WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE);
        previous.writeInbound(new TextWebSocketFrame("{}"));
        previous.pipeline().fireExceptionCaught(new IllegalStateException("old socket failed"));
        releaseClose(previous);
        run();

        assertTrue(drain(previous).isEmpty(), "A stale handshake must not start the socket's tasks");
        assertTrue(replacement.isOpen());
        assertEquals(SignalingState.STARTING, signaling.health().state());
        assertEquals(1, signaling.health().failedAttempts());
        handshake(replacement, turnResult(0));
        assertEquals(SignalingState.CONNECTED, signaling.health().state());
    }

    // Harness.

    private final class Signaling extends NetherNetXboxRpcSignaling {
        private final List<EmbeddedChannel> sockets = new ArrayList<>();
        private final List<String> tokensUsed = new ArrayList<>();

        private Signaling() {
            super("local", refresh -> {
                tokenRequests.add(refresh);
                return pendingToken != null ? pendingToken
                        : CompletableFuture.completedFuture(token(tokenRequests.size()));
            }, new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()), loop.eventLoop());
        }

        @Override
        ChannelFuture connectSocket(String token) {
            tokensUsed.add(token);
            EmbeddedChannel socket = new EmbeddedChannel();
            socket.freezeTime();
            socket.pipeline().addLast(this);
            sockets.add(socket);
            return socket.newSucceededFuture();
        }
    }

    /** The liveness constants in seconds, for readability. */
    private static final class RpcLimits {
        static final long SILENCE = NetherNetXboxRpcSignaling.SILENCE_LIMIT_SECONDS;
        static final long CHECK = NetherNetXboxRpcSignaling.LIVENESS_CHECK_SECONDS;
    }

    private String token(int number) {
        JsonObject claims = new JsonObject();
        if (tokenCarriesPlayerId) {
            claims.addProperty("pmid", "self");
        }
        claims.addProperty("n", number);
        return "MCToken header." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(claims.toString().getBytes(StandardCharsets.UTF_8)) + ".signature";
    }

    private EmbeddedChannel connect(JsonObject turnResult) {
        signaling.start();
        run();
        EmbeddedChannel socket = signaling.sockets.getLast();
        handshake(socket, turnResult);
        assertEquals(SignalingState.CONNECTED, signaling.health().state());
        return socket;
    }

    private void handshake(EmbeddedChannel socket, JsonObject turnResult) {
        socket.pipeline().fireUserEventTriggered(WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE);
        JsonObject request = turnRequests(drain(socket)).getFirst();
        respond(socket, request.get("id").getAsString(), "result", turnResult);
        run();
    }

    private static JsonObject turnResult(long lifetime, String... urls) {
        JsonArray servers = new JsonArray();
        for (String url : urls) {
            JsonArray serverUrls = new JsonArray();
            serverUrls.add(url);
            JsonObject server = new JsonObject();
            server.add("Urls", serverUrls);
            server.addProperty("Username", "user");
            server.addProperty("Password", "secret");
            servers.add(server);
        }
        JsonObject result = new JsonObject();
        result.add("TurnAuthServers", servers);
        if (lifetime != 0) {
            result.addProperty("ExpirationInSeconds", lifetime);
        }
        return result;
    }

    /**
     * Lets the socket's clock run in 5 s steps while the service answers WebSocket and RPC pings.
     *
     * @return every other request the source sent meanwhile
     */
    private List<JsonObject> keepAlive(EmbeddedChannel socket, long seconds) {
        List<JsonObject> sent = new ArrayList<>();
        for (long elapsed = 0; elapsed < seconds; elapsed += 5) {
            advanceSocket(socket, 5);
            for (JsonObject request : drain(socket)) {
                if (NetherNetConstants.XBOX_RPC_METHOD_PING.equals(method(request))) {
                    respond(socket, request.get("id").getAsString(), "result", new JsonObject());
                } else {
                    sent.add(request);
                }
            }
            if (socket.isOpen()) {
                socket.writeInbound(new PongWebSocketFrame());
            }
            run();
        }
        return sent;
    }

    private void advanceSocket(EmbeddedChannel socket, long seconds) {
        socket.advanceTimeBy(seconds, TimeUnit.SECONDS);
        run();
    }

    private void advanceLoop(Duration duration) {
        loop.advanceTimeBy(duration.toNanos(), TimeUnit.NANOSECONDS);
        run();
    }

    private void run() {
        for (int i = 0; i < 4; i++) {
            loop.runPendingTasks();
            for (EmbeddedChannel socket : List.copyOf(signaling.sockets)) {
                socket.runPendingTasks();
            }
        }
    }

    /** @return the JSON text frames the source sent, oldest first; other frames are dropped */
    private static List<JsonObject> drain(EmbeddedChannel socket) {
        List<JsonObject> sent = new ArrayList<>();
        Object message;
        while ((message = socket.readOutbound()) != null) {
            try {
                if (message instanceof TextWebSocketFrame frame) {
                    sent.add(JsonParser.parseString(frame.text()).getAsJsonObject());
                }
            } finally {
                ReferenceCountUtil.release(message);
            }
        }
        return sent;
    }

    private static String method(JsonObject request) {
        return request.has("method") ? request.get("method").getAsString() : null;
    }

    private static String innerMethod(JsonObject request) {
        return JsonParser.parseString(request.getAsJsonObject("params").get("message").getAsString())
                .getAsJsonObject().get("method").getAsString();
    }

    private static List<JsonObject> turnRequests(List<JsonObject> sent) {
        return sent.stream().filter(r -> NetherNetConstants.XBOX_RPC_METHOD_TURN_AUTH.equals(method(r))).toList();
    }

    private static List<JsonObject> routeProbes(List<JsonObject> sent) {
        return sent.stream()
                .filter(r -> NetherNetConstants.XBOX_RPC_METHOD_SEND_MESSAGE.equals(method(r)))
                .filter(r -> NetherNetConstants.XBOX_RPC_INNER_METHOD_ROUTE_PROBE.equals(innerMethod(r)))
                .toList();
    }

    private static void respond(EmbeddedChannel socket, String id, String field, JsonElement value) {
        JsonObject response = new JsonObject();
        response.addProperty("jsonrpc", "2.0");
        response.addProperty("id", id);
        response.add(field, value);
        socket.writeInbound(new TextWebSocketFrame(response.toString()));
    }

    private void receiveWebRtc(EmbeddedChannel socket, String signal, boolean batched) {
        receive(socket, "peer", NetherNetConstants.XBOX_RPC_INNER_METHOD_WEBRTC, webRtcParams(signal), true, batched);
    }

    private static JsonObject webRtcParams(String signal) {
        JsonObject params = new JsonObject();
        params.addProperty("netherNetId", "123");
        params.addProperty("message", signal);
        return params;
    }

    /** A client attempt's handler that records why the signaling gave up on its offer. */
    private static NetherNetSignaling.SignalHandler attemptFailures(List<SignalingFailure> failures) {
        return new NetherNetSignaling.SignalHandler() {
            @Override
            public void onSignal(Signal signal) {
                fail("No signal expected, got " + signal);
            }

            @Override
            public void onFailure(String connectionId, SignalingFailure failure) {
                failures.add(failure);
            }
        };
    }

    private static String messageId(JsonObject sentMessage) {
        return sentMessage.getAsJsonObject("params").get("messageId").getAsString();
    }

    /** The signal a sent client message carries, null for anything else. */
    private static String webRtcMessage(JsonObject request) {
        if (!NetherNetConstants.XBOX_RPC_METHOD_SEND_MESSAGE.equals(method(request))) {
            return null;
        }
        JsonObject inner = JsonParser.parseString(request.getAsJsonObject("params").get("message").getAsString())
                .getAsJsonObject();
        return inner.getAsJsonObject("params").get("message").getAsString();
    }

    private void receiveFromSelf(EmbeddedChannel socket, String innerMethod) {
        receive(socket, "self", innerMethod, new JsonObject(), true, false);
        drain(socket);
    }

    private void receive(EmbeddedChannel socket, String from, String innerMethod, JsonObject innerParams,
                         boolean requestHasId, boolean batched) {
        JsonObject inner = new JsonObject();
        inner.addProperty("jsonrpc", "2.0");
        inner.addProperty("method", innerMethod);
        inner.add("params", innerParams);
        receiveMessage(socket, from, "message-1", inner.toString(), requestHasId, batched);
    }

    private void receiveMessage(EmbeddedChannel socket, String from, String messageId, String text) {
        receiveMessage(socket, from, messageId, text, true, false);
    }

    /** Delivers one message as the service does, its text as the sender wrote it. */
    private void receiveMessage(EmbeddedChannel socket, String from, String messageId, String text,
                                boolean requestHasId, boolean batched) {
        JsonObject message = new JsonObject();
        message.addProperty("From", from);
        message.addProperty("Id", messageId);
        message.addProperty("Message", text);
        JsonObject request = new JsonObject();
        request.addProperty("jsonrpc", "2.0");
        request.addProperty("method", NetherNetConstants.XBOX_RPC_METHOD_RECEIVE_MESSAGE);
        if (requestHasId) {
            request.addProperty("id", "request-1");
        }
        if (batched) {
            JsonArray messages = new JsonArray();
            messages.add(message);
            request.add("params", messages);
        } else {
            request.add("params", message);
        }
        socket.writeInbound(new TextWebSocketFrame(request.toString()));
        run();
    }

    private static void holdClose(EmbeddedChannel socket) {
        socket.pipeline().addFirst("hold-close", new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                promise.setSuccess();
            }
        });
    }

    private void releaseClose(EmbeddedChannel socket) {
        socket.pipeline().remove("hold-close");
        socket.close();
        run();
    }

    private int pendingRequestCount() throws Exception {
        Field field = NetherNetXboxRpcSignaling.class.getDeclaredField("pendingRequests");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(signaling)).size();
    }
}

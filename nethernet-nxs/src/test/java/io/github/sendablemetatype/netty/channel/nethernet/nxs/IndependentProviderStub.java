/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * An independent NXS provider for tests: registration, recovery, signed operations over HTTPS and the WebSocket,
 * and assisted joins pushed on request. It checks every signature, generation and sequence like a provider must,
 * on the clock it is given, which tests share with the client.
 */
final class IndependentProviderStub implements AutoCloseable {
    final String origin;
    private final LongSupplier clock;
    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final Channel server;
    volatile Runnable heartbeatResponseHook = () -> {};
    /** Runs as each heartbeat arrives, before the stub decides whether it fails. */
    volatile Runnable heartbeatAttemptHook = () -> {};
    final Map<String, JsonObject> challenges = new HashMap<>(), keys = new HashMap<>();
    JsonObject registration;
    String initialChallengeId;
    volatile long challengeLifetimeMillis = 60000;
    /** The stub's clock minus the client's, as a host whose clock is wrong sees the provider. */
    volatile long clockOffsetMillis;
    volatile int staleSignatures;
    volatile boolean operatorActions;
    volatile int operatorActionRequests;
    volatile JsonObject lastHeartbeat, lastHostProfile;
    final List<String> heartbeatBodies = new CopyOnWriteArrayList<>();
    volatile int failHeartbeats;
    /** Answers every enrollment register instead of a challenge, and every heartbeat, while set. */
    volatile Refusal registrationRefusal, heartbeatRefusal;
    volatile boolean failOutcomes;
    /** Outcome batches still to reject permanently. */
    volatile int rejectOutcomes;
    /** Answer the next heartbeat without a profile as if the provider held none. */
    volatile boolean forgetProfile;
    /** Leave the readiness out of heartbeat replies, as a broken provider might. */
    volatile boolean omitReadiness;
    volatile JsonObject readiness;
    volatile int outcomeAttempts;
    volatile boolean loseCompletionResponse;
    volatile long checkInMillis;
    /** The schedule's minimum interval between early heartbeats. */
    volatile long minUpdateIntervalMillis = 1000;
    final List<JsonObject> events = new CopyOnWriteArrayList<>();
    long generation, sequence;
    volatile boolean poolOnlyRegistration;
    volatile int registrations, heartbeats;
    volatile String challengeAuthorization;
    volatile String selectedMode;
    /** Whether the bearer token carries attachment authority, which automatic mode follows. */
    volatile boolean attachToken;
    volatile int challengeDifficulty = -1;
    volatile JsonObject extensionMetadata;
    volatile JsonArray heartbeatRetirements;
    volatile JsonObject heartbeatExtensions;
    volatile int extensionRequests, keyAcknowledgements;
    boolean draining;
    String keyRequestId;
    JsonObject requestedKey;
    int epoch = 1, profileRevision;
    final List<String> operationsSeen = new CopyOnWriteArrayList<>();
    final List<String> websocketOperations = new CopyOnWriteArrayList<>();

    // WebSocket control
    volatile boolean websocket;
    volatile boolean rejectUpgrade, ignorePings;
    /** The operation, such as {@code rotate}, whose next WebSocket attempt is applied and then left unanswered. */
    volatile String dropAfterCommit;
    /** Sent instead of the next WebSocket reply, after the operation was applied. */
    volatile String brokenReply;
    /** Sent before each WebSocket reply. */
    volatile String pushBeforeReply;
    /** Headers added to each WebSocket reply. */
    final JsonObject responseHeaders = new JsonObject();
    /** Answers the next signed operation instead of applying it. */
    volatile Refusal rejectNext;
    /** Answers every replay of the dropped operation instead of the receipt. */
    volatile Refusal rejectAfterDrop;
    /** Answers every replay of the dropped operation with 503. */
    volatile boolean refuseAfterDrop;
    final AtomicInteger upgrades = new AtomicInteger(), pings = new AtomicInteger(), fallbacks = new AtomicInteger();
    final List<Map<String, String>> upgradeHeaders = new CopyOnWriteArrayList<>();
    /** The generation each accepted upgrade was signed for. */
    final List<Long> generations = new CopyOnWriteArrayList<>();
    /** The paths of operations that arrived over HTTPS, as sent. */
    final List<String> httpOperations = new CopyOnWriteArrayList<>();
    /** What the front door found wrong with a client's frames or fallbacks; a passing test leaves it empty. */
    final List<String> faults = new CopyOnWriteArrayList<>();
    final LinkedBlockingQueue<JsonObject> assistedResults = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<Integer> closeCodes = new LinkedBlockingQueue<>();
    volatile Channel socket;
    /** Successful replies by idempotency key, so a replayed operation is answered without being applied twice. */
    private final Map<String, Reply> receipts = new HashMap<>();
    private Map<String, String> droppedHeaders;
    private String droppedBody;

    IndependentProviderStub(LongSupplier clock) throws InterruptedException {
        this(clock, null);
    }

    IndependentProviderStub(LongSupplier clock, SslContext tls) throws InterruptedException {
        this.clock = clock;
        server = new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        if (tls != null) {
                            channel.pipeline().addLast(tls.newHandler(channel.alloc()));
                        }
                        channel.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(1 << 20),
                                new Http());
                    }
                })
                .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        origin = (tls == null ? "http://127.0.0.1:" : "https://localhost:") + port;
    }

    /** Pushes one text frame to the live control socket. */
    void push(String frame) {
        socket.writeAndFlush(new TextWebSocketFrame(frame));
    }

    /** An error response a test sets: its status, JSON body and optional Retry-After header. */
    static final class Refusal extends Exception {
        final int status;
        final JsonObject body;
        final String retryAfter;
        final AtomicInteger answered = new AtomicInteger();

        Refusal(int status, String body, String retryAfter) {
            super(null, null, false, false);
            this.status = status;
            this.body = JsonParser.parseString(body).getAsJsonObject();
            this.retryAfter = retryAfter;
        }
    }

    private static final class Failure extends Exception {
        final int status;

        Failure(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private record Reply(int status, JsonObject body, Map<String, String> headers) {
    }

    private long now() {
        return clock.getAsLong() + clockOffsetMillis;
    }

    /**
     * The provider's front door around {@link #handle}: an operation replayed with its idempotency key gets the
     * first reply instead of being applied twice, and the fault knobs act here. An HTTPS replay of a dropped
     * WebSocket operation must carry the very bytes that were signed for it.
     */
    private synchronized Reply front(String method, String path, Map<String, String> headers, String raw,
                                     boolean overWebSocket) {
        String id = headers.get("idempotency-key");
        boolean replay = id != null && droppedHeaders != null && id.equals(droppedHeaders.get("idempotency-key"));
        if (!overWebSocket) {
            httpOperations.add(path);
            if (replay) {
                if (!raw.equals(droppedBody)) {
                    faults.add("HTTPS fallback rewrote the signed body");
                }
                if (fallbacks.getAndIncrement() == 0 && !signed(headers).equals(droppedHeaders)) {
                    faults.add("HTTPS fallback changed the signed headers");
                }
            }
        }
        if (replay && rejectAfterDrop != null) {
            return refused(rejectAfterDrop);
        }
        Refusal next = rejectNext;
        if (id != null && next != null) {
            rejectNext = null;
            return refused(next);
        }
        if (replay && refuseAfterDrop) {
            JsonObject error = new JsonObject();
            error.addProperty("code", "fixture_unavailable");
            return new Reply(503, error, Map.of());
        }
        if (id != null && receipts.containsKey(id)) {
            return receipts.get(id);
        }
        Reply reply = handle(method, path, headers, raw, overWebSocket);
        if (id != null && reply.status() / 100 == 2) {
            receipts.put(id, reply);
        }
        return reply;
    }

    private synchronized void dropped(Map<String, String> headers, String body) {
        droppedHeaders = signed(headers);
        droppedBody = body;
    }

    private static Map<String, String> signed(Map<String, String> headers) {
        Map<String, String> signed = new HashMap<>();
        for (String name : ProviderWebSocket.AUTH_HEADERS) {
            signed.put(name, headers.get(name));
        }
        return signed;
    }

    private static Reply refused(Refusal refusal) {
        refusal.answered.incrementAndGet();
        return new Reply(refusal.status, refusal.body,
                refusal.retryAfter == null ? Map.of() : Map.of("retry-after", refusal.retryAfter));
    }

    private synchronized Reply handle(String method, String path, Map<String, String> headers, String raw,
                                      boolean overWebSocket) {
        Map<String, String> replyHeaders = new HashMap<>();
        try {
            JsonObject body = dispatch(method, path, headers, raw, overWebSocket);
            if (path.endsWith("/heartbeat")) {
                heartbeatResponseHook.run();
            }
            return new Reply(200, body, replyHeaders);
        } catch (Refusal r) {
            r.answered.incrementAndGet();
            if (r.retryAfter != null) {
                replyHeaders.put("retry-after", r.retryAfter);
            }
            return new Reply(r.status, r.body, replyHeaders);
        } catch (Failure f) {
            JsonObject error = new JsonObject();
            error.addProperty("code", f.getMessage());
            if ("stale_provider_signature".equals(f.getMessage())) {
                replyHeaders.put("x-provider-time", Long.toString(now()));
            }
            return new Reply(f.status, error, replyHeaders);
        } catch (Exception f) {
            JsonObject error = new JsonObject();
            error.addProperty("code", "invalid_request");
            return new Reply(400, error, replyHeaders);
        }
    }

    private JsonObject dispatch(String method, String path, Map<String, String> headers, String raw,
                                boolean overWebSocket) throws Exception {
        String signedPath = path;
        if (path.startsWith("/v1/nxs/")) {
            path = "/example/" + path.substring("/v1/nxs/".length());
        }
        if (raw.length() > 65536) {
            throw new Failure(400, "payload_limit");
        }
        JsonObject body = raw.isEmpty() ? new JsonObject() : JsonParser.parseString(raw).getAsJsonObject();
        if (path.equals("/.well-known/nethernet-external-signaling")) {
            JsonObject d = new JsonObject();
            d.addProperty("provider", origin);
            d.add("protocols", strings(ProviderCrypto.PROTOCOL));
            d.add("signatures", strings(ProviderCrypto.SIGNATURE));
            JsonObject extensions = extensionMetadata == null ? new JsonObject() : extensionMetadata.deepCopy();
            if (websocket) {
                JsonObject control = new JsonObject();
                control.addProperty("version", 1);
                control.addProperty("critical", false);
                control.add("data", new JsonObject());
                extensions.add(Wire.WEBSOCKET, control);
            }
            if (!extensions.isEmpty()) {
                d.add("extensions", extensions);
            }
            JsonObject limits = new JsonObject();
            limits.addProperty("heartbeatIntervalMs", 1000);
            d.add("limits", limits);
            JsonObject authorization = new JsonObject();
            authorization.add("schemes", strings("anonymous-proof-of-work", "bearer-token"));
            d.add("authorization", authorization);
            return d;
        }
        operationsSeen.add(path);
        if (overWebSocket) {
            websocketOperations.add(path);
        }
        if (path.equals("/example/register")) {
            return register(headers, body);
        }
        if (path.equals("/example/complete")) {
            return complete(body);
        }
        if (path.equals("/example/heartbeat")) {
            heartbeatAttemptHook.run();
            Refusal refusal = heartbeatRefusal;
            if (refusal != null) {
                throw refusal;
            }
            int remainingFailures = failHeartbeats;
            failHeartbeats--;
            if (remainingFailures > 0) {
                throw new Failure(503, "fixture_transient");
            }
        }
        authenticate(method, signedPath, headers, raw);
        JsonObject ok = new JsonObject();
        switch (path) {
            case "/example/heartbeat" -> heartbeat(raw, body, ok);
            case "/example/extension" -> extensionRequests++;
            case "/example/operator-action" -> {
                operatorActionRequests++;
                ok.add("operatorAction", operatorAction("https://panel.example/claim/" + operatorActionRequests));
            }
            case "/example/deregister" -> draining = true;
            case "/example/outcomes" -> {
                outcomeAttempts++;
                if (failOutcomes) {
                    throw new Failure(503, "fixture_outcome_unavailable");
                }
                if (rejectOutcomes > 0) {
                    rejectOutcomes--;
                    throw new Failure(400, "invalid_outcomes");
                }
                for (JsonElement event : body.getAsJsonArray("events")) {
                    events.add(event.getAsJsonObject());
                }
            }
            case "/example/rotate" -> {
                String old = headers.get("nxs-key-id"), intent = headers.get("idempotency-key");
                JsonObject key = body.getAsJsonObject("publicKeyJwk");
                if (!ProviderCrypto.verify(key, body.get("proof").getAsString(),
                        ProviderCrypto.array(ProviderCrypto.PROTOCOL, "rotate", origin, "example-machine-1", old,
                                ProviderCrypto.thumbprint(key), generation, intent))) {
                    throw new Failure(401, "replacement_proof_invalid");
                }
                String id = "example-key-" + UUID.randomUUID();
                keys.put(id, key);
                registration.addProperty("keyId", id);
                ok.addProperty("keyId", id);
            }
            case "/example/retire" -> keys.remove(body.get("keyId").getAsString());
            default -> throw new Failure(422, "unknown_operation");
        }
        return ok;
    }

    private JsonObject register(Map<String, String> headers, JsonObject body) throws Exception {
        boolean recovery = body.has("registrationId");
        if (!ProviderCrypto.PROTOCOL.equals(body.get("protocol").getAsString())) {
            throw new Failure(400, "unsupported_protocol");
        }
        Refusal refusal = registrationRefusal;
        if (!recovery && refusal != null) {
            throw refusal;
        }
        // A lost completion recovers by the challenge that created the registration
        if (recovery && (registration == null || !registration.get("registrationId").equals(body.get("registrationId"))
                && !body.get("registrationId").getAsString().equals(initialChallengeId))) {
            throw new Failure(403, "recovery_unavailable");
        }
        JsonObject key = recovery ? keys.get(registration.get("keyId").getAsString())
                : body.getAsJsonObject("publicKeyJwk");
        String authorization;
        if (recovery) {
            authorization = "recovery";
        } else if (headers.containsKey("authorization")) {
            authorization = "bearer-token";
            challengeAuthorization = headers.get("authorization");
            if (!"Bearer independent-provider-token".equals(challengeAuthorization)) {
                throw new Failure(401, "invalid_bearer_token");
            }
        } else {
            authorization = "anonymous-proof-of-work";
        }
        JsonObject c = new JsonObject();
        c.addProperty("protocol", ProviderCrypto.PROTOCOL);
        c.addProperty("signature", ProviderCrypto.SIGNATURE);
        c.addProperty("challengeId", UUID.randomUUID().toString());
        c.addProperty("audience", origin);
        c.addProperty("nonce", UUID.randomUUID().toString());
        c.addProperty("thumbprint", ProviderCrypto.thumbprint(key));
        c.addProperty("expiresAt", now() + challengeLifetimeMillis);
        c.addProperty("serverTime", now());
        JsonObject context = new JsonObject();
        for (String f : List.of("label", "authorizationId", "serviceId", "registrationId")) {
            context.addProperty(f, "");
        }
        if (!recovery && body.has("label")) {
            context.add("label", body.get("label"));
        }
        String mode = recovery ? "recover"
                : authorization.equals("bearer-token") && attachToken ? "attach-instance" : "new-service";
        context.addProperty("mode", mode);
        if (recovery) {
            context.add("registrationId", registration.get("registrationId"));
        }
        if (!recovery && authorization.equals("bearer-token")) {
            context.addProperty("authorizationId", "independent-authority");
            JsonObject selected = new JsonObject();
            selected.addProperty("scheme", authorization);
            selected.addProperty("reference", "independent-authority");
            c.add("authorization", selected);
        }
        if (!recovery && selectedMode != null) {
            context.addProperty("mode", selectedMode);
        }
        c.add("context", context);
        c.addProperty("contextDigest", ProviderCrypto.contextDigest(context));
        JsonObject pow = new JsonObject();
        challengeDifficulty = recovery || authorization.equals("bearer-token") ? 0 : 2;
        pow.addProperty("difficulty", challengeDifficulty);
        c.add("pow", pow);
        challenges.put(c.get("challengeId").getAsString(), c.deepCopy());
        keys.put(c.get("challengeId").getAsString(), key);
        return c;
    }

    private JsonObject complete(JsonObject body) throws Exception {
        String id = body.get("challengeId").getAsString();
        JsonObject c = challenges.get(id);
        if (c == null) {
            throw new Failure(409, "recovery_required");
        }
        String proof = ProviderCrypto.proof(c, body.get("proofNonce").getAsString(),
                body.get("idempotencyKey").getAsString());
        if (!ProviderCrypto.verify(keys.get(id), body.get("signature").getAsString(), proof)
                || !ProviderCrypto.meetsDifficulty(ProviderCrypto.digest(proof),
                c.getAsJsonObject("pow").get("difficulty").getAsInt())) {
            throw new Failure(401, "proof_invalid");
        }
        challenges.remove(id);
        generation++;
        sequence = 0;
        draining = false;
        profileRevision = 0;
        if (c.getAsJsonObject("context").get("mode").getAsString().equals("recover")) {
            JsonObject r = registration.deepCopy();
            r.remove("admissionKey");
            if (poolOnlyRegistration) {
                r.remove("serviceId");
                r.remove("publicAddress");
            }
            r.addProperty("leaseGeneration", generation);
            return r;
        }
        if (registration != null) {
            throw new Failure(409, "already_registered");
        }
        registrations++;
        registration = new JsonObject();
        registration.addProperty("protocol", ProviderCrypto.PROTOCOL);
        registration.addProperty("provider", origin);
        registration.addProperty("registrationId", "example-registration-" + registrations);
        initialChallengeId = id;
        registration.addProperty("instanceId", "example-machine-1");
        if (!poolOnlyRegistration) {
            registration.addProperty("serviceId", "example-service-1");
        }
        registration.addProperty("keyId", "example-key-1");
        if (!poolOnlyRegistration) {
            registration.addProperty("publicAddress", "https://play.example.invalid");
        }
        registration.addProperty("leaseGeneration", generation);
        registration.add("admissionKey", ticket());
        if (extensionMetadata != null) {
            registration.add("extensions", extensionMetadata.deepCopy());
        }
        keys.put("example-key-1", keys.get(id));
        if (loseCompletionResponse) {
            loseCompletionResponse = false;
            throw new Failure(503, "completion_response_lost");
        }
        JsonObject completed = registration.deepCopy();
        if (operatorActions) {
            completed.add("operatorAction", operatorAction("https://panel.example/claim/first"));
        }
        return completed;
    }

    private void heartbeat(String raw, JsonObject body, JsonObject ok) throws Failure {
        heartbeatBodies.add(raw);
        lastHeartbeat = body;
        heartbeats++;
        ok.addProperty("serverTime", now());
        if (body.has("installedAdmissionKeyIds")) {
            keyAcknowledgements++;
        }
        if (body.has("hostProfile")) {
            lastHostProfile = body.getAsJsonObject("hostProfile");
            JsonObject profile = body.getAsJsonObject("hostProfile");
            if (keyAcknowledgements == 0 || !"nethernet.stateless-admission.v1".equals(
                    profile.getAsJsonObject("statelessAdmission").get("capability").getAsString())
                    || !profile.get("dtlsFingerprint").getAsString().matches("sha-256 [0-9A-F]{2}(?::[0-9A-F]{2}){31}")) {
                throw new Failure(400, "invalid_host_profile");
            }
            profileRevision++;
        }
        if (body.has("admissionKeyRequestId")) {
            String wanted = body.get("admissionKeyRequestId").getAsString();
            if (!wanted.equals(keyRequestId)) {
                keyRequestId = wanted;
                requestedKey = ticket();
                epoch++;
                requestedKey.addProperty("keyId", String.format("T%03d", epoch));
                ok.add("admissionKey", requestedKey.deepCopy());
            }
            JsonObject request = new JsonObject();
            request.addProperty("id", keyRequestId);
            request.add("keyId", requestedKey.get("keyId"));
            ok.add("admissionKeyRequest", request);
        }
        JsonArray retirements = heartbeatRetirements;
        ok.add("admissionKeyRetirements", retirements == null ? new JsonArray() : retirements.deepCopy());
        draining = !body.get("acceptingPlayers").getAsBoolean();
        ok.addProperty("hostProfileRevision", "example-profile-" + profileRevision);
        if (forgetProfile && !body.has("hostProfile")) {
            forgetProfile = false;
            ok.add("hostProfileRevision", JsonNull.INSTANCE);
        }
        if (!omitReadiness) {
            JsonObject ready = readiness;
            if (ready == null) {
                ready = new JsonObject();
                ready.addProperty("routable", profileRevision > 0 && keyAcknowledgements > 0 && !draining);
                ready.add("reasons", new JsonArray());
            }
            ok.add("readiness", ready.deepCopy());
        }
        JsonObject extensions = heartbeatExtensions != null ? heartbeatExtensions
                : extensionMetadata != null ? extensionMetadata : null;
        if (extensions != null) {
            ok.add("extensions", extensions.deepCopy());
        }
        if (operatorActions) {
            ok.addProperty("operatorActionAvailable", true);
        }
        if (checkInMillis > 0) {
            JsonObject schedule = new JsonObject();
            schedule.addProperty("afterMillis", checkInMillis);
            schedule.addProperty("leaseMillis", checkInMillis + 30000);
            schedule.addProperty("minUpdateIntervalMillis", minUpdateIntervalMillis);
            ok.add("checkIn", schedule);
        }
    }

    private JsonObject operatorAction(String url) {
        JsonObject action = new JsonObject();
        action.addProperty("url", url);
        action.addProperty("text", "Claim this server.");
        action.addProperty("expiresAt", now() + 60000);
        return action;
    }

    private void authenticate(String method, String path, Map<String, String> h, String raw) throws Failure {
        try {
            String key = h.get("nxs-key-id"), instance = h.get("nxs-instance-id");
            long timestamp = Long.parseLong(h.get("nxs-timestamp")), gen = Long.parseLong(h.get("nxs-generation")),
                    seq = Long.parseLong(h.get("nxs-sequence"));
            if (gen < generation) {
                throw new Failure(409, "generation_fenced");
            }
            if (Math.abs(now() - timestamp) > 60000) {
                staleSignatures++;
                throw new Failure(401, "stale_provider_signature");
            }
            if (!ProviderCrypto.SIGNATURE.equals(h.get("nxs-signature-version"))
                    || !"example-machine-1".equals(instance) || gen != generation || seq <= sequence) {
                throw new Failure(401, "auth_invalid");
            }
            String target = path;
            if (!ProviderCrypto.verify(keys.get(key), h.get("nxs-signature"), ProviderCrypto.request(origin, method,
                    target, timestamp, instance, key, h.get("idempotency-key"), gen, seq, raw))) {
                throw new Failure(401, "signature_invalid");
            }
            sequence = seq;
        } catch (RuntimeException f) {
            throw new Failure(401, "auth_invalid");
        }
    }

    /** Checks an upgrade's signature over {@code GET} on the control path with an empty body. */
    private synchronized boolean upgradeAuthentic(Map<String, String> h, String path) {
        try {
            return ProviderCrypto.verify(keys.get(h.get("nxs-key-id")), h.get("nxs-signature"),
                    ProviderCrypto.request(origin, "GET", path, Long.parseLong(h.get("nxs-timestamp")),
                            h.get("nxs-instance-id"), h.get("nxs-key-id"), h.get("idempotency-key"),
                            Long.parseLong(h.get("nxs-generation")), Long.parseLong(h.get("nxs-sequence")), ""))
                    && Long.parseLong(h.get("nxs-generation")) == generation;
        } catch (RuntimeException invalid) {
            return false;
        }
    }

    private static JsonArray strings(String... strings) {
        JsonArray a = new JsonArray();
        for (String s : strings) {
            a.add(s);
        }
        return a;
    }

    static JsonObject ticket() {
        JsonObject key = new JsonObject();
        key.addProperty("keyId", "T001");
        key.addProperty("secret", "LYDph--S9dykiRFwmo5VAzY5GO_QGapcKQ4luEgrCRM");
        return key;
    }

    private static Map<String, String> headers(HttpHeaders headers) {
        Map<String, String> map = new HashMap<>();
        headers.forEach(entry -> map.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue()));
        return map;
    }

    /** HTTPS operations, and the WebSocket upgrade on the control path. */
    private final class Http extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) {
            Map<String, String> headers = headers(request.headers());
            if (request.uri().equals("/v1/nxs/control")) {
                upgrades.incrementAndGet();
                upgradeHeaders.add(headers);
                if (rejectUpgrade || !upgradeAuthentic(headers, request.uri())) {
                    var denied = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FORBIDDEN);
                    denied.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
                    context.writeAndFlush(denied).addListener(ChannelFutureListener.CLOSE);
                    return;
                }
                if (!ProviderCrypto.PROTOCOL.equals(headers.get("sec-websocket-protocol"))) {
                    faults.add("Upgrade without the NXS subprotocol");
                }
                generations.add(Long.parseLong(headers.get("nxs-generation")));
                var handshaker = new WebSocketServerHandshakerFactory(origin.replace("http", "ws") + request.uri(),
                        ProviderCrypto.PROTOCOL, false, ProviderWebSocket.MAX_FRAME_BYTES).newHandshaker(request);
                context.pipeline().addLast(new WebSocketFrameAggregator(ProviderWebSocket.MAX_FRAME_BYTES),
                        new Control());
                context.pipeline().remove(this);
                handshaker.handshake(context.channel(), request).addListener(done -> socket = context.channel());
                return;
            }
            String raw = request.content().toString(StandardCharsets.UTF_8);
            Reply reply = front(request.method().name(), request.uri(), headers, raw, false);
            byte[] bytes = reply.body().toString().getBytes(StandardCharsets.UTF_8);
            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.valueOf(reply.status()), Unpooled.wrappedBuffer(bytes));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
            reply.headers().forEach(response.headers()::set);
            context.writeAndFlush(response);
        }
    }

    /** Operations over the control socket, its pings, and the host's assisted-join results. */
    private final class Control extends SimpleChannelInboundHandler<WebSocketFrame> {
        @Override
        protected void channelRead0(ChannelHandlerContext context, WebSocketFrame frame) {
            if (frame instanceof CloseWebSocketFrame close) {
                closeCodes.add(close.statusCode());
                context.close();
                return;
            }
            if (!(frame instanceof TextWebSocketFrame text)) {
                return;
            }
            String message = text.text();
            if (message.equals("ping")) {
                pings.incrementAndGet();
                if (!ignorePings) {
                    context.writeAndFlush(new TextWebSocketFrame("pong"));
                }
                return;
            }
            JsonObject json = JsonParser.parseString(message).getAsJsonObject();
            switch (json.get("kind").getAsString()) {
                case "operation" -> {
                    if (!json.keySet().equals(Set.of("kind", "operation", "headers", "body"))) {
                        faults.add("Operation frame members " + json.keySet());
                    }
                    Map<String, String> headers = new HashMap<>();
                    json.getAsJsonObject("headers").entrySet()
                            .forEach(entry -> headers.put(entry.getKey(), entry.getValue().getAsString()));
                    if (!headers.keySet().equals(Set.copyOf(ProviderWebSocket.AUTH_HEADERS))) {
                        faults.add("Operation frame headers " + headers.keySet());
                    }
                    String operation = json.get("operation").getAsString(), body = json.get("body").getAsString();
                    Reply reply = front("POST", "/v1/nxs/" + operation, headers, body, true);
                    if (operation.equals(dropAfterCommit)) {
                        dropAfterCommit = null;
                        dropped(headers, body);
                        context.close();
                        return;
                    }
                    String broken = brokenReply;
                    if (broken != null) {
                        brokenReply = null;
                        dropped(headers, body);
                        context.writeAndFlush(new TextWebSocketFrame(broken));
                        return;
                    }
                    String push = pushBeforeReply;
                    if (push != null) {
                        context.writeAndFlush(new TextWebSocketFrame(push));
                    }
                    JsonObject response = new JsonObject();
                    response.addProperty("kind", "response");
                    response.addProperty("id", headers.get("idempotency-key"));
                    response.addProperty("status", reply.status());
                    JsonObject replyHeaders = responseHeaders.deepCopy();
                    reply.headers().forEach(replyHeaders::addProperty);
                    response.add("headers", replyHeaders);
                    response.addProperty("body", reply.body().toString());
                    // Fragmented, so the client's reassembly is part of every exchange
                    String wire = response.toString();
                    int split = wire.length() / 2;
                    context.write(new TextWebSocketFrame(false, 0, wire.substring(0, split)));
                    context.writeAndFlush(new ContinuationWebSocketFrame(true, 0, wire.substring(split)));
                }
                case "assisted-join-result" -> assistedResults.add(json);
                default -> {
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) {
            if (socket == context.channel()) {
                socket = null;
            }
            context.fireChannelInactive();
        }
    }

    @Override
    public void close() {
        server.close().syncUninterruptibly();
        group.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
    }
}

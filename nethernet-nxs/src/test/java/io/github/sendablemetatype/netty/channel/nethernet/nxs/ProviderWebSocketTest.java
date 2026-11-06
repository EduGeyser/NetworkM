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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ControlCarrier;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ControlTransport;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ProviderException;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.ClientHarness.SOFTWARE;
import static org.junit.jupiter.api.Assertions.*;

/** Signed operations over the provider's WebSocket, the HTTPS fallback, and assisted joins pushed over it. */
class ProviderWebSocketTest {
    private static final String CPK;
    private static final String OFFER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=setup:actpass\r\n"
            + "a=sctp-port:5000\r\na=max-message-size:262144\r\na=ice-ufrag:assistedClient\r\na=ice-pwd:"
            + "c".repeat(128) + "\r\na=fingerprint:sha-256 " + String.join(":", Collections.nCopies(32, "AA"))
            + "\r\na=candidate:1 1 UDP 123 127.0.0.1 19132 typ host\r\n";
    private static final AssistedAnswer ANSWER = new AssistedAnswer("v=0\r\na=identity:signed-by-the-host\r\n");

    static {
        try {
            var generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp384r1"));
            CPK = Base64.getEncoder().encodeToString(generator.generateKeyPair().getPublic().getEncoded());
        } catch (Exception unavailable) {
            throw new ExceptionInInitializerError(unavailable);
        }
    }

    /** A harness whose provider advertises its WebSocket and asks for check-ins every 15 minutes. */
    private static ClientHarness harness() throws Exception {
        var h = new ClientHarness();
        h.stub.websocket = true;
        h.stub.checkInMillis = 900000;
        return h;
    }

    private static ProviderClient client(ClientHarness h, Path directory, ControlTransport mode,
                                         FakeTransport transport) throws Exception {
        return h.client(h.configuration().authorizationToken("independent-provider-token").controlTransport(mode)
                        .build(), directory, transport, () -> null,
                () -> new ProviderClient.Health(true, 20, SOFTWARE, h.connected(0)));
    }

    private static ProviderClient assisted(ClientHarness h, Path directory, FakeTransport transport)
            throws Exception {
        transport.assisted = true;
        transport.assistedFamilies = Set.of(AddressFamily.IPV4);
        return h.client(h.configuration().authorizationToken("independent-provider-token")
                        .controlTransport(ControlTransport.AUTO).assistedJoins(true).build(), directory, transport,
                () -> null, () -> new ProviderClient.Health(true, 10, SOFTWARE, h.connected(0)));
    }

    private static String state(Path directory) throws Exception {
        return Files.readString(directory.resolve("provider-state.json"));
    }

    @Test
    void httpIsTheDefaultAndAutoNeedsTheAdvertisement(@TempDir Path directory) throws Exception {
        for (boolean advertised : List.of(false, true)) {
            try (var h = harness()) {
                h.stub.websocket = advertised;
                var mode = advertised ? ControlTransport.HTTP : ControlTransport.AUTO;
                var client = client(h, directory.resolve(Boolean.toString(advertised)), mode, new FakeTransport());
                h.await(client.start());
                assertEquals(0, h.stub.upgrades.get());
                assertEquals(ControlCarrier.HTTP, client.lastControlCarrier());
                assertTrue(h.stub.httpOperations.contains("/v1/nxs/heartbeat"));
            }
        }
    }

    @Test
    void signedOperationsUseTheSocketAndFallBackWithTheSameSignedBytes(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            var client = client(h, directory, ControlTransport.AUTO, transport);
            h.await(client.start());
            assertTrue(h.stub.websocketOperations.contains("/example/heartbeat"));
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            assertFalse(h.stub.httpOperations.contains("/v1/nxs/heartbeat"));
            assertTrue(h.stub.httpOperations.contains("/v1/nxs/register"));
            assertTrue(h.stub.httpOperations.contains("/v1/nxs/complete"));
            h.stub.dropAfterCommit = "rotate";
            h.await(client.rotateMachineKey());
            assertTrue(h.stub.websocketOperations.contains("/example/rotate"));
            assertTrue(h.stub.httpOperations.contains("/v1/nxs/rotate"));
            assertTrue(h.stub.operationsSeen.contains("/example/retire"));
            assertEquals(1, h.stub.fallbacks.get());
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
            assertEquals(0, transport.drains, "Losing the control socket must not touch gameplay");
            h.stop(client);

            var recovered = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            assertEquals(2, h.await(recovered.start()).leaseGeneration());
            assertEquals(List.of(1L, 2L), h.stub.generations);
            h.await(recovered.deregister());
            assertTrue(h.stub.websocketOperations.contains("/example/deregister"));
            assertEquals(List.of(), h.stub.faults);
        }
    }

    @Test
    void anAmbiguousRotationKeepsItsCandidateAndNeedsOrdinaryRecovery(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            h.stub.dropAfterCommit = "rotate";
            h.stub.refuseAfterDrop = true;
            assertNotNull(h.settledFailure(client.rotateMachineKey()));
            JsonObject state = JsonParser.parseString(state(directory)).getAsJsonObject();
            assertTrue(state.has("pendingWebSocketOperation"));
            String candidate = state.get("pendingPrivateKey").getAsString();
            assertNotNull(h.settledFailure(client.rotateMachineKey()));
            assertEquals(candidate, JsonParser.parseString(state(directory)).getAsJsonObject()
                    .get("pendingPrivateKey").getAsString(), "The candidate key stays until recovery settles it");
            h.stop(client);

            h.stub.refuseAfterDrop = false;
            var recovered = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(recovered.start());
            JsonObject after = JsonParser.parseString(state(directory)).getAsJsonObject();
            assertFalse(after.has("pendingWebSocketOperation"));
            assertFalse(after.has("pendingPrivateKey"));
            assertEquals(h.stub.registration.get("keyId"), after.getAsJsonObject("registration").get("keyId"));
        }
    }

    @Test
    void aMalformedReplyClosesTheSocketAndFallsBackWithTheSameSignedBytes(@TempDir Path directory)
            throws Exception {
        try (var h = harness()) {
            h.stub.brokenReply = "{\"kind\":\"response\",\"id\":\"AAAAAAAAAAAAAAAA\" ,\"id\":\"BBBBBBBBBBBBBBBB\","
                    + "\"status\":200,\"headers\":{},\"body\":\"{}\"}";
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            assertTrue(h.stub.fallbacks.get() > 0);
            assertEquals(1008, h.stub.closeCodes.poll(5, TimeUnit.SECONDS));
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
            assertEquals(List.of(), h.stub.faults);
        }
    }

    @Test
    void framesOfUnknownKindAreIgnored(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            h.stub.pushBeforeReply = "{\"kind\":\"later-push\",\"data\":{}}";
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            assertEquals(0, h.stub.fallbacks.get());
            assertTrue(h.stub.closeCodes.isEmpty());
        }
    }

    @Test
    void replyHeadersAreProviderNeutralButNeverDuplicated(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            h.stub.responseHeaders.addProperty("Content-Type", "application/json");
            h.stub.responseHeaders.addProperty("X-Example-Receipt", "recorded");
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            assertEquals(0, h.stub.fallbacks.get());
            h.stub.responseHeaders.addProperty("content-type", "application/json");
            h.await(client.readiness());
            assertEquals(ControlCarrier.HTTP, client.lastControlCarrier());
            assertTrue(h.stub.httpOperations.contains("/v1/nxs/heartbeat"));
        }
    }

    @Test
    void aDefinitiveRefusalOrARateLimitLeavesNothingToRecover(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            h.stub.rejectNext = new IndependentProviderStub.Refusal(400, "{\"code\":\"check_in_profile_required\"}",
                    null);
            assertNotNull(h.failure(client.drain()));
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
            h.stub.rejectNext = new IndependentProviderStub.Refusal(429, "{\"code\":\"rate_limited\"}", null);
            h.settled(client.drain());
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
        }
    }

    @Test
    void correctingARefusedHeartbeatNeedsNoRecovery(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            h.stub.heartbeatRefusal = new IndependentProviderStub.Refusal(400, "{\"code\":\"invalid_heartbeat\"}",
                    null);
            var refused = assertInstanceOf(ProviderException.class, h.failure(client.drain()));
            assertEquals("invalid_heartbeat", refused.code());
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
            h.stub.heartbeatRefusal = null;
            h.await(client.drain());
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            assertEquals(1, h.stub.generation);
        }
    }

    @Test
    void aLostAcknowledgementNeedsOrdinaryRecoveryBeforeAFreshHeartbeat(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            h.stub.dropAfterCommit = "heartbeat";
            h.stub.rejectAfterDrop = new IndependentProviderStub.Refusal(400, "{\"code\":\"invalid_heartbeat\"}",
                    null);
            assertNotNull(h.failure(client.drain()));
            assertTrue(state(directory).contains("pendingWebSocketOperation"));
            h.settled(client.drain());
            assertEquals(2, h.stub.generation, "Fresh signed operations need a recovered generation");
            assertEquals(List.of(1L, 2L), h.stub.generations);
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
        }
    }

    @Test
    void aRefusedUpgradeUsesHttpsAndBacksOff(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            h.stub.rejectUpgrade = true;
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            assertEquals(1, h.stub.upgrades.get(), "Immediate key-install check-ins must not storm the provider");
            assertTrue(h.stub.httpOperations.contains("/v1/nxs/heartbeat"));
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
        }
    }

    @Test
    void socketFailuresStayQuietDuringTheFallbackAndRecoverOnce(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            h.stub.rejectUpgrade = true;
            var client = client(h, directory, ControlTransport.AUTO, new FakeTransport());
            h.await(client.start());
            String failure = ProviderLog.Operation.WEBSOCKET.failure;
            for (int i = 0; i < 20 && h.logged(ProviderDiagnostic.Level.DEBUG, failure).isEmpty(); i++) {
                h.advance(Duration.ofSeconds(5));
                h.await(client.readiness());
            }
            assertEquals(ControlCarrier.HTTP, client.lastControlCarrier());
            assertEquals(1, h.logged(ProviderDiagnostic.Level.WARN, failure).size());
            assertFalse(h.logged(ProviderDiagnostic.Level.DEBUG, failure).isEmpty());
            assertTrue(h.logged(ProviderDiagnostic.Level.INFO, ProviderLog.Operation.WEBSOCKET.recovery).isEmpty());
            h.stub.rejectUpgrade = false;
            for (int i = 0; i < 20 && client.lastControlCarrier() != ControlCarrier.WEBSOCKET; i++) {
                h.advance(Duration.ofSeconds(30));
                h.await(client.readiness());
            }
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            h.await(client.readiness());
            assertEquals(1, h.logged(ProviderDiagnostic.Level.INFO, ProviderLog.Operation.WEBSOCKET.recovery).size());
        }
    }

    // Assisted joins

    /** A join for the profile the stub last saw, due on the provider's clock {@code inMillis} from now. */
    private static JsonObject join(ClientHarness h, String id, long inMillis) {
        JsonObject profile = h.stub.lastHostProfile;
        JsonObject join = new JsonObject();
        join.addProperty("kind", "assisted-join");
        join.addProperty("version", 1);
        join.addProperty("id", id);
        join.add("instanceId", h.stub.registration.get("instanceId"));
        join.addProperty("generation", h.stub.generation);
        join.add("incarnation", profile.getAsJsonObject("statelessAdmission").get("incarnation"));
        join.add("keyId", profile.get("admissionKeyId"));
        join.add("hostFingerprint", profile.get("dtlsFingerprint"));
        join.addProperty("expiresAt", h.scheduler.millis() + h.stub.clockOffsetMillis + inMillis);
        join.addProperty("networkId", "1234");
        join.addProperty("cpk", CPK);
        join.addProperty("localUfrag", "NXS1assistedHost");
        join.addProperty("localPassword", "h".repeat(32));
        join.addProperty("offer", OFFER);
        return join;
    }

    /** Pushes a frame once the provider holds a live socket. */
    private static void push(ClientHarness h, String frame) throws InterruptedException {
        h.until(() -> h.stub.socket != null);
        h.stub.push(frame);
    }

    /**
     * Moves the clock a second at a time, letting the upgrade travel between steps, until the client opened a new
     * socket. The reconnect backoff is under 5 seconds for the few failures these tests cause.
     */
    private static void reconnected(ClientHarness h, int upgrades) throws InterruptedException {
        for (int i = 0; i < 8 && h.stub.upgrades.get() == upgrades; i++) {
            h.advance(Duration.ofSeconds(1));
            h.scheduler.quiet(20);
        }
        assertTrue(h.stub.upgrades.get() > upgrades, "A dead socket is replaced without waiting for an operation");
        h.until(() -> h.stub.socket != null);
    }

    private static JsonObject result(ClientHarness h) throws InterruptedException {
        h.until(() -> !h.stub.assistedResults.isEmpty());
        return h.stub.assistedResults.poll();
    }

    private static long warnings(ClientHarness h) {
        return h.logged(ProviderDiagnostic.Level.WARN, "A player could not connect using an assisted join.").size();
    }

    @Test
    void assistanceIsAdvertisedFromTheStartAndFeedbackLeavesItAlone(@TempDir Path directory) throws Exception {
        for (boolean enabled : List.of(false, true)) {
            try (var h = harness()) {
                var transport = new FakeTransport();
                transport.revision = 1;
                var client = enabled ? assisted(h, directory.resolve("on"), transport)
                        : client(h, directory.resolve("off"), ControlTransport.AUTO, transport);
                h.await(client.start());
                assertEquals(enabled ? "[4]" : null, assistedFamilies(h.stub), "Configuration applies before feedback");
                assertEquals(enabled, "1".equals(h.stub.upgradeHeaders.getLast().get("nxs-assisted")));
                for (String outcome : List.of("unknown", "not-established", "established", "unavailable")) {
                    h.stub.heartbeatExtensions = JsonParser.parseString("{\"" + Wire.CONNECTIVITY
                            + "\":{\"version\":1,\"critical\":false,\"data\":{\"candidateRevision\":1,\"checks\":[{"
                            + "\"region\":\"lim1\",\"family\":4,\"outcome\":\"" + outcome + "\",\"checkedAt\":"
                            + h.scheduler.millis() + ",\"expiresAt\":" + (h.scheduler.millis() + 60000)
                            + "}]}}}").getAsJsonObject();
                    h.await(client.readiness());
                    assertEquals(enabled ? "[4]" : null, assistedFamilies(h.stub));
                }
                assertEquals(4, transport.reports.size(), "Each heartbeat's feedback reaches the endpoint");
            }
        }
    }

    private static String assistedFamilies(IndependentProviderStub stub) {
        JsonObject admission = stub.lastHostProfile.getAsJsonObject("statelessAdmission");
        return admission.has("assistedFamilies") ? admission.getAsJsonArray("assistedFamilies").toString() : null;
    }

    @Test
    void anAssistedJoinIsAnsweredOverTheSocketAndTheAuthorityIsRenewedEarly(@TempDir Path directory)
            throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            var joins = new CopyOnWriteArrayList<AssistedJoin>();
            transport.joins = (join, guard) -> {
                guard.requireCurrent();
                joins.add(join);
                return CompletableFuture.completedFuture(ANSWER);
            };
            var client = assisted(h, directory, transport);
            h.await(client.start());
            JsonObject join = join(h, "ab".repeat(16), 14000);
            join.add("kind", join.remove("kind")); // Members arrive in any order
            push(h, " \n" + new GsonBuilder().setPrettyPrinting().create().toJson(join));
            JsonObject reply = result(h);
            assertTrue(reply.get("accepted").getAsBoolean());
            assertEquals(ANSWER.sdp(), reply.get("answer").getAsString());
            assertEquals("ab".repeat(16), joins.getFirst().id());

            // A 15-minute check-in schedule still renews the five-minute authority before it lapses
            int heartbeats = h.stub.heartbeats;
            h.run(Duration.ofMinutes(4));
            assertTrue(h.stub.heartbeats > heartbeats);
            h.run(Duration.ofMinutes(2));
            push(h, join(h, "cd".repeat(16), 14000).toString());
            assertTrue(result(h).get("accepted").getAsBoolean(), "The renewed authority still serves joins");
            assertTrue(h.stub.closeCodes.isEmpty());
        }
    }

    @Test
    void joinsTheHostCannotServeAreRefusedWithoutCostingTheSocket(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            var reached = new CopyOnWriteArrayList<AssistedJoin>();
            var answer = new java.util.concurrent.atomic.AtomicReference<>(ANSWER);
            transport.joins = (join, guard) -> {
                reached.add(join);
                return CompletableFuture.completedFuture(answer.get());
            };
            var client = assisted(h, directory, transport);
            h.await(client.start());

            JsonObject otherGeneration = join(h, "01".repeat(16), 14000);
            otherGeneration.addProperty("generation", h.stub.generation + 1);
            push(h, otherGeneration.toString());
            assertFalse(result(h).get("accepted").getAsBoolean(), "Joins for another lease are refused");

            JsonObject tooLate = join(h, "02".repeat(16), 31000);
            push(h, tooLate.toString());
            assertFalse(result(h).get("accepted").getAsBoolean(), "A deadline beyond 30 seconds is refused");

            JsonObject otherKey = join(h, "03".repeat(16), 14000);
            otherKey.addProperty("keyId", "Z999");
            push(h, otherKey.toString());
            assertFalse(result(h).get("accepted").getAsBoolean(), "A join for another admission key is refused");
            assertTrue(reached.isEmpty(), "Refused joins never reach the endpoint");

            answer.set(new AssistedAnswer("v=0\r\n"));
            push(h, join(h, "04".repeat(16), 14000).toString());
            assertFalse(result(h).get("accepted").getAsBoolean(),
                    "A player's answer without the server identity is refused");

            answer.set(ANSWER);
            push(h, join(h, "05".repeat(16), 14000).toString());
            assertTrue(result(h).get("accepted").getAsBoolean());
            assertTrue(h.stub.closeCodes.isEmpty(), "Refusals never close the socket");
            assertEquals(4, warnings(h));
        }
    }

    @Test
    void joinsWhileAssistanceIsOffAreRefused(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            transport.assistedFamilies = Set.of(AddressFamily.IPV4);
            var client = client(h, directory, ControlTransport.AUTO, transport);
            h.await(client.start());
            push(h, join(h, "ab".repeat(16), 14000).toString());
            assertFalse(result(h).get("accepted").getAsBoolean());
            h.await(client.readiness());
            assertEquals(ControlCarrier.WEBSOCKET, client.lastControlCarrier());
            assertTrue(h.stub.closeCodes.isEmpty());
        }
    }

    @Test
    void joinsBeyondTheInFlightLimitAreRefused(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            var pending = new CopyOnWriteArrayList<CompletableFuture<AssistedAnswer>>();
            transport.joins = (join, guard) -> {
                var answer = new CompletableFuture<AssistedAnswer>();
                pending.add(answer);
                return answer;
            };
            var client = assisted(h, directory, transport);
            h.await(client.start());
            for (int i = 0; i < ProviderWebSocket.MAX_ASSISTED_IN_FLIGHT; i++) {
                push(h, join(h, "%032x".formatted(i + 1), 14000).toString());
            }
            h.until(() -> pending.size() == ProviderWebSocket.MAX_ASSISTED_IN_FLIGHT);
            push(h, join(h, "ff".repeat(16), 14000).toString());
            JsonObject refused = result(h);
            assertEquals("ff".repeat(16), refused.get("id").getAsString());
            assertFalse(refused.get("accepted").getAsBoolean());
            pending.forEach(answer -> answer.complete(ANSWER));
            h.until(() -> h.stub.assistedResults.size() == ProviderWebSocket.MAX_ASSISTED_IN_FLIGHT);
            assertTrue(h.stub.assistedResults.stream().allMatch(r -> r.get("accepted").getAsBoolean()));
            assertTrue(h.stub.closeCodes.isEmpty());
        }
    }

    @Test
    void aLateAnswerIsDroppedAloneAndAnExpiredAuthorityServesNothing(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            var pending = new CopyOnWriteArrayList<CompletableFuture<AssistedAnswer>>();
            transport.joins = (join, guard) -> {
                var answer = new CompletableFuture<AssistedAnswer>();
                pending.add(answer);
                return answer;
            };
            var client = assisted(h, directory, transport);
            h.await(client.start());
            push(h, join(h, "ab".repeat(16), 2000).toString());
            h.until(() -> pending.size() == 1);
            h.advance(Duration.ofSeconds(3));
            pending.getFirst().complete(ANSWER);
            h.run(Duration.ofSeconds(1));
            assertTrue(h.stub.assistedResults.isEmpty(), "An answer past the join's deadline is not sent");
            assertEquals(1, warnings(h));

            transport.joins = (join, guard) -> CompletableFuture.completedFuture(ANSWER);
            push(h, join(h, "cd".repeat(16), 14000).toString());
            assertTrue(result(h).get("accepted").getAsBoolean(), "The socket outlives the dropped answer");

            // Without a successful heartbeat the authority lapses after five minutes
            h.stub.heartbeatRefusal = new IndependentProviderStub.Refusal(400, "{\"code\":\"invalid_heartbeat\"}",
                    null);
            var reached = new CopyOnWriteArrayList<AssistedJoin>();
            transport.joins = (join, guard) -> {
                reached.add(join);
                return CompletableFuture.completedFuture(ANSWER);
            };
            h.run(Duration.ofMinutes(5).plusSeconds(1));
            push(h, join(h, "ef".repeat(16), 14000).toString());
            assertFalse(result(h).get("accepted").getAsBoolean());
            assertTrue(reached.isEmpty(), "An expired authority never reaches the endpoint");
        }
    }

    @Test
    void theJoinDeadlineIsReadOnTheProviderClock(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            h.stub.clockOffsetMillis = 30000;
            var deadlines = new CopyOnWriteArrayList<Long>();
            var transport = new FakeTransport() {
                @Override
                public CompletionStage<AssistedAnswer> assistedJoin(AssistedJoin join, long deadlineNanos,
                                                                    OwnershipGuard guard) {
                    deadlines.add(deadlineNanos - h.scheduler.nanos());
                    return CompletableFuture.completedFuture(ANSWER);
                }
            };
            var client = assisted(h, directory, transport);
            h.await(client.start());
            // 14 seconds on the provider's clock is 44 on this host's, which no join may take
            push(h, join(h, "ab".repeat(16), 14000).toString());
            assertTrue(result(h).get("accepted").getAsBoolean());
            assertEquals(14, TimeUnit.NANOSECONDS.toSeconds(deadlines.getFirst() + 999_999_999));
        }
    }

    @Test
    void invalidFramesCloseWith1008AndAssistanceReconnectsAtOnce(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            transport.joins = (join, guard) -> CompletableFuture.completedFuture(ANSWER);
            var client = assisted(h, directory, transport);
            h.await(client.start());
            for (String invalid : List.of("{\"kind\":\"assisted-join\",\"version\":1}", "{\"data\":{}}", "[]")) {
                int upgrades = h.stub.upgrades.get();
                push(h, invalid);
                h.until(() -> !h.stub.closeCodes.isEmpty());
                assertEquals(1008, h.stub.closeCodes.poll());
                // No operation is due, yet the provider can push joins again once the backoff passes
                reconnected(h, upgrades);
                assertEquals("1", h.stub.upgradeHeaders.getLast().get("nxs-assisted"));
            }
            push(h, join(h, "ab".repeat(16), 14000).toString());
            assertTrue(result(h).get("accepted").getAsBoolean());
        }
    }

    @Test
    void aSocketLostDuringAnOperationReconnectsOnceTheFallbackSettledIt(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var transport = new FakeTransport();
            transport.joins = (join, guard) -> CompletableFuture.completedFuture(ANSWER);
            var client = assisted(h, directory, transport);
            h.await(client.start());
            int upgrades = h.stub.upgrades.get();
            h.stub.dropAfterCommit = "rotate";
            h.await(client.rotateMachineKey());
            assertEquals(1, h.stub.fallbacks.get());
            assertFalse(state(directory).contains("pendingWebSocketOperation"));
            // The socket died while the rotation was unsettled; no operation is due, yet joins can arrive again
            reconnected(h, upgrades);
            push(h, join(h, "ab".repeat(16), 14000).toString());
            assertTrue(result(h).get("accepted").getAsBoolean());
        }
    }

    @Test
    void anUnansweredPingDropsTheSocketAndAssistanceReconnects(@TempDir Path directory) throws Exception {
        try (var h = harness()) {
            var client = assisted(h, directory, new FakeTransport());
            h.await(client.start());
            int upgrades = h.stub.upgrades.get();
            h.stub.ignorePings = true;
            h.run(Duration.ofSeconds(41));
            assertTrue(h.stub.pings.get() >= 1);
            h.stub.ignorePings = false;
            reconnected(h, upgrades);
        }
    }
}

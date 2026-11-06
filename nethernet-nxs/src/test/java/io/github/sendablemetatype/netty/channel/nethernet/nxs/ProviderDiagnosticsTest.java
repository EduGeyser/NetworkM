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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.ClientHarness.SOFTWARE;
import static org.junit.jupiter.api.Assertions.*;

/** Diagnostic admission and connectivity feedback over a real HTTPS heartbeat; the endpoint only records. */
class ProviderDiagnosticsTest {
    private static final InetSocketAddress PUBLIC = new InetSocketAddress("8.8.8.8", 19133);

    /** An endpoint that serves diagnostics, whose captures turn stale once its candidate revision moves on. */
    private static FakeTransport transport() {
        var transport = new FakeTransport() {
            @Override
            public CompletionStage<HostProfileSnapshot> captureHostProfile() {
                long captured = revision;
                guard = () -> {
                    if (revision != captured) {
                        throw new ProfileReplacedException();
                    }
                };
                return super.captureHostProfile();
            }
        };
        transport.diagnostics = true;
        transport.revision = 1;
        transport.version = 1;
        return transport;
    }

    private static ProviderClient client(ClientHarness h, Path directory, FakeTransport transport, boolean diagnostics,
                                         CandidateMethod method) throws Exception {
        return h.client(h.configuration().authorizationToken("independent-provider-token")
                        .diagnosticAdmission(diagnostics).connectivityMethod(method).build(), directory, transport,
                () -> null, () -> new ProviderClient.Health(true, 10, SOFTWARE, h.connected(0)));
    }

    private static JsonObject extension(ClientHarness h) {
        return h.stub.lastHeartbeat.getAsJsonObject("extensions").getAsJsonObject(Wire.CONNECTIVITY);
    }

    private static JsonObject data(ClientHarness h) {
        return extension(h).getAsJsonObject("data");
    }

    @Test
    void theHostInstallsThePolicyBeforeItAdvertisesDiagnostics(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            var client = client(h, directory, transport, true, CandidateMethod.DEFINED);
            var advertised = new CopyOnWriteArrayList<Boolean>();
            var premature = new AtomicBoolean();
            h.stub.heartbeatResponseHook = () -> {
                var extensions = h.stub.lastHeartbeat.getAsJsonObject("extensions");
                boolean diagnostics = extensions != null && extensions.getAsJsonObject(Wire.CONNECTIVITY)
                        .getAsJsonObject("data").get("diagnostics").getAsBoolean();
                premature.compareAndSet(false, diagnostics && transport.policies.isEmpty());
                advertised.add(diagnostics);
            };
            long before = h.scheduler.millis();
            var registration = h.await(client.start());
            assertFalse(advertised.getFirst(), "The first heartbeat has no installed policy to advertise");
            assertTrue(advertised.contains(true));
            assertFalse(premature.get(), "Opt-in follows the actual local installation");

            var policy = transport.policies.getLast();
            assertEquals(registration.instanceId(), policy.context().hostId());
            assertEquals(registration.leaseGeneration(), policy.context().generation());
            assertEquals(h.stub.origin, policy.context().providerOrigin());
            assertTrue(policy.expiresAt() >= before && policy.expiresAt() <= h.scheduler.millis() + 300000);
            assertEquals(transport.installs.getLast().getFirst().secret(), policy.keys().getFirst().secret());
            assertEquals(9007199254740991L, policy.keys().getFirst().retireAfter());
            assertEquals(Set.of("version", "critical", "data"), extension(h).keySet());
            assertEquals(Set.of("diagnostics", "candidateRevision", "method"), data(h).keySet());
            assertEquals("defined", data(h).get("method").getAsString());
            assertEquals(1, data(h).get("candidateRevision").getAsLong());

            int installed = transport.policies.size();
            h.await(client.readiness());
            assertTrue(transport.policies.size() > installed, "Each successful heartbeat renews the policy");
            assertEquals(policy.keys(), transport.policies.getLast().keys());

            h.await(client.drain());
            assertFalse(transport.diagnosticsEnabled);
            assertFalse(h.stub.lastHeartbeat.has("extensions"));
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
        }
    }

    @Test
    void discoveryConfiguresStunAndAReflexiveCandidateIsKeptWarm(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            h.stub.extensionMetadata = JsonParser.parseString("{\"" + Wire.CONNECTIVITY + "\":{\"version\":1,"
                    + "\"critical\":false,\"data\":{\"stunServers\":[{\"host\":\"stun.example\",\"port\":3478}]}}}")
                    .getAsJsonObject();
            var transport = transport();
            transport.candidates = List.of(new HostCandidate(PUBLIC, CandidateType.SRFLX));
            var client = client(h, directory, transport, true, CandidateMethod.DISCOVERED);
            h.await(client.start());
            assertEquals(List.of(new StunServer("stun.example", 3478)), transport.stunServers.getLast());
            assertTrue(data(h).get("diagnostics").getAsBoolean());
            assertEquals("warm_stun", data(h).get("method").getAsString());
            assertFalse(data(h).has("probeCandidates"), "Matching lists are not published twice");
            assertFalse(data(h).has("assistedFamilies"));
        }
    }

    @Test
    void withdrawnCandidatesStayProbeTargetsUntilTheyAreBack(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            var client = client(h, directory, transport, true, CandidateMethod.DISCOVERED);
            h.await(client.start());
            var published = transport.candidates;
            transport.probes = published;
            transport.candidates = List.of();
            transport.version++;
            h.await(client.readiness());
            assertEquals(1, data(h).getAsJsonArray("probeCandidates").size());
            assertEquals(1, transport.policies.getLast().endpoints().size());
            transport.candidates = published;
            transport.probes = null;
            transport.version++;
            h.await(client.readiness());
            assertFalse(data(h).has("probeCandidates"));
        }
    }

    @Test
    void aFailedHeartbeatRenewsNothingAndTheConnectivityNamespaceIsTheClients(@TempDir Path directory)
            throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            var client = client(h, directory, transport, true, CandidateMethod.DISCOVERED);
            h.await(client.start());
            int installed = transport.policies.size();
            h.stub.failHeartbeats = 100;
            assertNotNull(h.settledFailure(client.readiness()));
            assertEquals(installed, transport.policies.size());
            assertThrows(IllegalArgumentException.class, () -> client.updateHeartbeatExtensions(
                    Map.of(Wire.CONNECTIVITY, new ProviderClient.ProtocolExtension(1, false, "{}"))));
            h.stub.failHeartbeats = 0;
        }
    }

    @Test
    void aCaptureReplacedDuringTheHeartbeatInstallsNoPolicy(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            var client = client(h, directory, transport, true, CandidateMethod.DEFINED);
            h.stub.heartbeatResponseHook = () -> transport.revision++;
            // The lease and keys of each committed heartbeat hold; only the replaced profile is withdrawn
            h.settled(client.start());
            assertTrue(transport.policies.isEmpty(), "A replaced capture never installs its policy");
            assertFalse(transport.diagnosticsEnabled);
            assertFalse(data(h).get("diagnostics").getAsBoolean());
        }
    }

    @Test
    void anAssistedOnlyPolicyNeedsAssistanceOn(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            transport.candidates = List.of();
            transport.assistedFamilies = Set.of(AddressFamily.IPV4);
            var client = client(h, directory, transport, true, CandidateMethod.DISCOVERED);
            h.await(client.start());
            assertTrue(transport.policies.isEmpty());
            assertFalse(data(h).get("diagnostics").getAsBoolean());
            assertFalse(data(h).has("assistedFamilies"));
        }
    }

    @Test
    void feedbackKeepsItsTimesAndDropsExpiredFutureAndStaleChecks(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            var client = client(h, directory, transport, false, CandidateMethod.DISCOVERED);
            h.await(client.start());
            long now = h.scheduler.millis();
            var checks = new JsonArray();
            for (long checked : List.of(now - 1000, now - 300000, now + 10000)) {
                checks.add(check("warm_stun", "not-established", checked));
            }
            var data = new JsonObject();
            data.addProperty("candidateRevision", 1);
            data.add("checks", checks);
            var extension = new JsonObject();
            extension.addProperty("version", 1);
            extension.addProperty("critical", false);
            extension.add("data", data);
            h.stub.heartbeatExtensions = new JsonObject();
            h.stub.heartbeatExtensions.add(Wire.CONNECTIVITY, extension);
            h.await(client.readiness());
            assertEquals(List.of(new ConnectivityCheck("fixture", AddressFamily.IPV4, CandidateMethod.WARM_STUN, PUBLIC,
                    ConnectivityOutcome.NOT_ESTABLISHED, now - 1000, now + 59000)), transport.reports.getLast());

            var many = new JsonArray();
            for (int i = 0; i < 18; i++) {
                many.add(check(List.of("discovered", "warm_stun", "per_join").get(i % 3),
                        i % 2 == 0 ? "unavailable" : "unknown", now - 1000));
            }
            data.add("checks", many);
            h.await(client.readiness());
            assertEquals(18, transport.reports.getLast().size());
            assertEquals(ConnectivityOutcome.UNAVAILABLE, transport.reports.getLast().getFirst().outcome());
            assertEquals(CandidateMethod.PER_JOIN, transport.reports.getLast().get(2).method());

            int reports = transport.reports.size();
            data.addProperty("candidateRevision", 2);
            h.await(client.readiness());
            assertEquals(reports, transport.reports.size(), "Feedback for another revision is not for this profile");
        }
    }

    private static JsonObject check(String method, String outcome, long checkedAt) {
        var check = new JsonObject();
        check.addProperty("region", "fixture");
        check.addProperty("family", 4);
        check.addProperty("method", method);
        check.add("target", JsonParser.parseString("{\"address\":\"8.8.8.8\",\"port\":19133}"));
        check.addProperty("outcome", outcome);
        check.addProperty("checkedAt", checkedAt);
        check.addProperty("expiresAt", checkedAt + 60000);
        return check;
    }

    @Test
    void aRetryNeverSendsACaptureWhoseMappingChanged(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            var transport = transport();
            var client = client(h, directory, transport, false, CandidateMethod.DISCOVERED);
            h.await(client.start());
            var attempts = new AtomicInteger();
            // The first attempt fails, and the mapping changes while it is on its way
            h.stub.failHeartbeats = 1;
            h.stub.heartbeatAttemptHook = () -> {
                if (attempts.incrementAndGet() == 1) {
                    transport.candidates = List.of(HostCandidate.host(new InetSocketAddress("127.0.0.1", 19200)));
                    transport.revision++;
                    transport.version++;
                }
            };
            h.settled(client.readiness());
            assertEquals(2, attempts.get());
            assertEquals(19200, h.stub.lastHeartbeat.getAsJsonObject("hostProfile").getAsJsonArray("candidates")
                    .get(0).getAsJsonObject().get("port").getAsInt(),
                    "The retry carries a fresh capture, never the retired one");
        }
    }

    @Test
    void aMaterialChangeWakesALongCheckIn(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            h.stub.checkInMillis = 120000;
            var transport = transport();
            var client = client(h, directory, transport, false, CandidateMethod.DISCOVERED);
            h.await(client.start());
            int before = h.stub.heartbeats;
            transport.candidates = List.of(HostCandidate.host(new InetSocketAddress("127.0.0.1", 19200)));
            transport.revision++;
            transport.version++;
            h.run(Duration.ofSeconds(3));
            assertTrue(h.stub.heartbeats > before);
            assertEquals(19200, h.stub.lastHeartbeat.getAsJsonObject("hostProfile").getAsJsonArray("candidates")
                    .get(0).getAsJsonObject().get("port").getAsInt());

            int replaced = h.stub.heartbeats;
            transport.candidates = List.of();
            transport.revision++;
            transport.version++;
            h.run(Duration.ofSeconds(3));
            assertTrue(h.stub.heartbeats > replaced);
            assertTrue(h.stub.lastHeartbeat.getAsJsonObject("hostProfile").getAsJsonArray("candidates").isEmpty());
            assertTrue(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean(), "Withdrawal keeps gameplay");
            assertEquals(0, transport.drains);
        }
    }

    @Test
    void aShortDiagnosticLeaseDoesNotTurnTheTimerIntoAHeartbeatLoop(@TempDir Path directory) throws Exception {
        try (var h = ClientHarness.secure()) {
            h.stub.checkInMillis = 10000; // A 40 second lease, shorter than a fixed 60 second renewal lead
            var transport = transport();
            var client = client(h, directory, transport, true, CandidateMethod.DEFINED);
            h.await(client.start());
            int before = h.stub.heartbeats;
            h.run(Duration.ofSeconds(5));
            assertEquals(before, h.stub.heartbeats);
        }
    }
}

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
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.*;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.ClientHarness.SOFTWARE;
import static org.junit.jupiter.api.Assertions.*;

class ProviderClientTest {
    private static final TicketId TICKET = new TicketId("0123456789abcdef0123456789abcdef");

    private static TransportOutcome outcome(OutcomeStage stage, long at) {
        return new TransportOutcome(TICKET, stage, at, stage == OutcomeStage.FAILED ? "ice_failed" : null, null);
    }

    @Test
    void registersPublishesAndStopsWithADrainingHeartbeat(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            Registration registration = h.await(client.start());
            assertEquals("example-machine-1", registration.instanceId());
            assertEquals(1, h.stub.registrations);
            assertEquals(1, h.stub.generation);
            JsonObject profile = h.stub.lastHostProfile;
            assertEquals("T001", profile.get("admissionKeyId").getAsString());
            assertEquals(List.of("T001"), h.stub.lastHeartbeat.getAsJsonArray("installedAdmissionKeyIds").asList()
                    .stream().map(e -> e.getAsString()).toList());
            assertTrue(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            assertEquals(ControlCarrier.HTTP, client.lastControlCarrier());
            assertEquals(List.of(new InetSocketAddress("127.0.0.1", 19133)),
                    client.publishedEndpoints().orElseThrow().endpoints());
            h.stop(client);
            assertTrue(h.stub.draining, "Orderly shutdown reports the drain");
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            assertTrue(transport.closed.isDone());
            assertTrue(client.publishedEndpoints().isEmpty());
        }
    }

    @Test
    void repeatedStatusReadFailuresRecoverOnlyWhenStatusCanBeRead(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var broken = new AtomicBoolean();
            var client = h.client(h.configuration().build(), path, new FakeTransport(), () -> {
                if (broken.get()) {
                    throw new IllegalStateException("private failure details");
                }
                return new ServerStatus("Example", "", 40, 0);
            }, () -> new Health(true, 100, SOFTWARE, h.connected(0)));
            h.await(client.start());
            broken.set(true);
            h.await(client.readiness());
            h.await(client.readiness());
            String failure = ProviderLog.Operation.SERVER_STATUS.failure;
            assertEquals(1, h.logged(ProviderDiagnostic.Level.WARN, failure).size());
            assertFalse(h.logged(ProviderDiagnostic.Level.DEBUG, failure).isEmpty());
            assertTrue(h.logged(ProviderDiagnostic.Level.INFO, ProviderLog.Operation.SERVER_STATUS.recovery).isEmpty());
            broken.set(false);
            h.await(client.readiness());
            h.await(client.readiness());
            assertEquals(1, h.logged(ProviderDiagnostic.Level.INFO, ProviderLog.Operation.SERVER_STATUS.recovery).size());
            assertTrue(h.logs.stream().noneMatch(d -> d.message().contains("private failure details")));
        }
    }

    @Test
    void anUnusableAdmissionKeyIsNotPersistedAndTheHostStartsAgain(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var refusing = new FakeTransport();
            refusing.refuseKeys = true;
            var first = h.client(h.configuration().authorizationToken("independent-provider-token").build(), path,
                    refusing, () -> null, () -> new Health(true, 20, SOFTWARE, h.connected(0)));
            assertNotNull(h.failure(first.start()));
            h.stop(first);
            // The refused epoch must not have reached the state file, or every later start dies on it
            String persisted = Files.readString(path.resolve("provider-state.json"));
            assertFalse(persisted.contains("\"ticketKeys\":[{"), "a refused admission key was persisted: " + persisted);
            var second = h.client(h.configuration().authorizationToken("independent-provider-token").build(), path,
                    new FakeTransport(), () -> null, () -> new Health(true, 20, SOFTWARE, h.connected(0)));
            assertNotNull(h.await(second.start()));
        }
    }

    @Test
    void attachedInstancesAndRemovedPublicEndpointsPreserveRuntimeIdentity(@TempDir Path path) throws Exception {
        for (boolean standalone : List.of(false, true)) {
            try (var h = new ClientHarness()) {
                h.stub.poolOnlyRegistration = !standalone;
                h.stub.attachToken = !standalone;
                var config = h.configuration().authorizationToken("independent-provider-token").build();
                Registration first = null;
                for (int generation = 1; generation <= 2; generation++) {
                    var client = h.client(config, path.resolve(standalone ? "standalone" : "pool"), new FakeTransport(),
                            () -> null, () -> new Health(true, 20, SOFTWARE, h.connected(0)));
                    var current = h.await(client.start());
                    assertEquals(standalone && generation == 1, current.serviceId() != null);
                    assertEquals(standalone && generation == 1, current.publicAddress() != null);
                    assertEquals(generation, current.leaseGeneration());
                    if (first == null) {
                        first = current;
                    } else {
                        assertEquals(first.instanceId(), current.instanceId());
                        assertEquals(first.registrationId(), current.registrationId());
                    }
                    h.stop(client);
                    h.stub.poolOnlyRegistration = true;
                }
                assertEquals(1, h.stub.registrations);
            }
        }
    }

    @Test
    void runtimeCountsWakeCheckInsAndRemainCountedDuringDrain(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.checkInMillis = 900000;
            var sample = new AtomicReference<>(new ConnectedPlayers(3, h.scheduler.millis()));
            var accepting = new AtomicBoolean(true);
            var client = h.client(h.configuration().build(), path, new FakeTransport(),
                    () -> new ServerStatus("Global listing", "world", 30000, 0),
                    () -> new Health(accepting.get(), 20, SOFTWARE, sample.get()));
            var extensions = Map.of("org.example.location",
                    new ProtocolExtension(1, false, "{\"country\":\"NL\"}"));
            h.await(client.updateHeartbeatExtensions(extensions));
            h.await(client.start());
            assertEquals("NL", h.stub.lastHeartbeat.getAsJsonObject("extensions")
                    .getAsJsonObject("org.example.location").getAsJsonObject("data").get("country").getAsString());
            assertEquals(3, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            assertEquals(sample.get().sampledAt(),
                    h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("sampledAt").getAsLong());
            assertEquals(20, h.stub.lastHeartbeat.get("capacity").getAsInt());
            assertFalse(h.stub.lastHeartbeat.getAsJsonObject("serverStatus").has("players"));
            h.advance(Duration.ofSeconds(1));
            accepting.set(false);
            client.requestStatusRefresh();
            h.settle();
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            h.advance(Duration.ofSeconds(1));
            accepting.set(true);
            client.requestStatusRefresh();
            h.settle();
            assertTrue(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            int before = h.stub.heartbeats;
            h.advance(Duration.ofSeconds(1));
            sample.set(new ConnectedPlayers(3, h.scheduler.millis()));
            client.requestStatusRefresh();
            h.run(Duration.ofSeconds(2));
            assertEquals(before, h.stub.heartbeats, "Timestamp-only changes use the ordinary schedule");
            sample.set(new ConnectedPlayers(4, h.scheduler.millis()));
            client.requestStatusRefresh();
            h.settle();
            assertEquals(4, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            h.await(client.drain());
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            h.advance(Duration.ofSeconds(1));
            sample.set(new ConnectedPlayers(5, h.scheduler.millis()));
            client.requestStatusRefresh();
            h.settle();
            assertEquals(5, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            h.stop(client);
            assertTrue(h.stub.draining);
            assertEquals(5, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
        }
    }

    @Test
    void deregistersOnShutdownOnlyWhenItsStateWillNotPersist(@TempDir Path path) throws Exception {
        for (boolean deregister : List.of(false, true)) {
            try (var h = new ClientHarness()) {
                Path directory = path.resolve(Boolean.toString(deregister));
                var transport = new FakeTransport();
                var client = h.client(h.configuration().deregisterOnShutdown(deregister).build(), directory, transport,
                        () -> null, () -> new Health(true, 10, SOFTWARE, h.connected(0)));
                h.await(client.start());
                // A report the provider will not take must not keep the instance registered
                h.stub.failOutcomes = true;
                transport.outcomes.add(outcome(OutcomeStage.DATA_CHANNELS_OPEN, h.scheduler.millis()));
                h.stop(client);
                assertEquals(deregister, h.stub.operationsSeen.contains("/example/deregister"));
                try (ProviderStateStore state = new ProviderStateStore(directory)) {
                    assertEquals(!deregister, state.read().has("registration"));
                }
            }
        }
    }

    @Test
    void heartbeatsUseTheStandardFormatAndPauseWithoutChangingGeneration(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.checkInMillis = 60000;
            var accepting = new AtomicBoolean(true);
            var client = h.client(h.configuration().build(), path, new FakeTransport(),
                    () -> new ServerStatus("Game", "world", 30, 0),
                    () -> new Health(accepting.get(), 20, SOFTWARE, new ConnectedPlayers(3, h.scheduler.millis())));
            h.await(client.start());
            long generation = h.stub.generation;
            for (String field : List.of("healthy", "load", "protocolVersion", "checkInVersion", "state",
                    "appliedStateRevision")) {
                assertFalse(h.stub.lastHeartbeat.has(field), field);
            }
            for (String field : List.of("protocol", "version", "players")) {
                assertFalse(h.stub.lastHeartbeat.getAsJsonObject("serverStatus").has(field), field);
            }
            assertTrue(h.stub.lastHeartbeat.has("gameOutcomes"));
            assertEquals("1.0", h.stub.lastHeartbeat.getAsJsonObject("software").get("version").getAsString());
            accepting.set(false);
            h.await(client.readiness());
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            assertFalse(h.stub.lastHeartbeat.has("gameOutcomes"));
            assertFalse(h.stub.lastHeartbeat.has("software"), "Unchanged software is not repeated");
            assertEquals(3, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            assertTrue(h.stub.lastHeartbeat.has("hostProfileRevision"));
            assertFalse(h.stub.lastHeartbeat.has("hostProfile"));
            accepting.set(true);
            h.await(client.readiness());
            assertTrue(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            assertEquals(generation, h.stub.generation);
        }
    }

    @Test
    void usesProviderNeutralBearerAuthorizationWithoutPowOrPersistingTheToken(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var config = h.configuration().label("Hosted customer").authorizationToken("independent-provider-token")
                    .build();
            var client = h.client(config, path, new FakeTransport(), () -> null,
                    () -> new Health(true, 10, SOFTWARE, h.connected(0)));
            assertEquals("example-machine-1", h.await(client.start()).instanceId());
            assertEquals("Bearer independent-provider-token", h.stub.challengeAuthorization);
            assertEquals(0, h.stub.challengeDifficulty);
            assertFalse(Files.readString(path.resolve("provider-state.json")).contains("independent-provider-token"));
            assertFalse(config.toString().contains("independent-provider-token"));
        }
    }

    @Test
    void optionalExtensionsRemainOpaqueAndExplicit(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.extensionMetadata = JsonParser.parseString("{\"org.example.operator\":{\"version\":1,"
                    + "\"critical\":false,\"data\":{\"message\":\"optional\",\"operations\":{\"inspect\":\""
                    + h.stub.origin + "/example/extension\"}}}}").getAsJsonObject();
            var client = h.client(h.configuration().build(), path, new FakeTransport(), () -> null,
                    () -> new Health(true, 10, null, h.connected(0)));
            var extensions = Map.of("org.example.location", new ProtocolExtension(1, false, "{\"location\":null}"));
            h.await(client.updateHeartbeatExtensions(extensions));
            h.await(client.start());
            assertEquals(JsonParser.parseString("{\"org.example.location\":{\"version\":1,\"critical\":false,"
                            + "\"data\":{\"location\":null}}}"), h.stub.lastHeartbeat.getAsJsonObject("extensions"),
                    "Explicit null in opaque extension data must survive wire serialization");
            assertFalse(h.stub.lastHeartbeat.has("software"), "Unspecified optional health fields stay omitted");
            var registered = h.await(client.extensions()).get("org.example.operator");
            assertEquals(JsonParser.parseString(registered.data()), h.stub.extensionMetadata
                    .getAsJsonObject("org.example.operator").get("data"));
            assertFalse(Files.readString(path.resolve("provider-state.json")).contains("org.example.operator"));
            assertEquals(0, h.stub.extensionRequests);
            assertTrue(h.await(client.readiness()).routable());
            h.await(client.extensionRequest("org.example.operator", "inspect", "POST", "{}"));
            assertEquals(1, h.stub.extensionRequests);
            assertThrows(IllegalArgumentException.class, () -> client.updateHeartbeatExtensions(Map.of(
                    Wire.CONNECTIVITY, new ProtocolExtension(1, false, "{}"))), "The client owns connectivity");
        }
    }

    @Test
    void followsTheProviderScheduleWithoutIdlePollingAndWakesForPlayers(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.checkInMillis = 900000;
            var players = new AtomicInteger(0);
            var client = h.client(h.configuration().build(), path, new FakeTransport(),
                    () -> new ServerStatus("Scheduled", "world", 40, 0),
                    () -> new Health(true, 40, SOFTWARE, h.connected(players.get())));
            h.await(client.start());
            h.run(Duration.ofSeconds(3));
            assertEquals(1, h.stub.heartbeats);
            for (int i = 0; i < 100; i++) {
                client.requestStatusRefresh();
            }
            h.run(Duration.ofSeconds(2));
            assertEquals(1, h.stub.heartbeats, "Unchanged local refreshes must not send requests");
            players.set(1);
            client.requestStatusRefresh();
            h.settle();
            assertEquals(1, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            int before = h.stub.heartbeats;
            h.run(Duration.ofMinutes(14));
            assertEquals(before, h.stub.heartbeats, "Nothing until the schedule");
            h.run(Duration.ofMinutes(2));
            assertEquals(before + 1, h.stub.heartbeats);
            h.stop(client);
            assertTrue(h.stub.draining, "Orderly shutdown still reports the drain while the timer is asleep");
            int beforeRestart = h.stub.heartbeats;
            var resumed = h.client(h.configuration().build(), path, new FakeTransport(),
                    () -> new ServerStatus("Restarted", "world", 40, 0),
                    () -> new Health(true, 40, SOFTWARE, h.connected(0)));
            h.await(resumed.start());
            assertEquals(2, h.stub.generation);
            assertEquals(beforeRestart + 1, h.stub.heartbeats, "A restart publishes at once despite the old schedule");
            assertEquals("Restarted", h.stub.lastHeartbeat.getAsJsonObject("serverStatus").get("name").getAsString());
        }
    }

    @Test
    void acceptanceAndEndpointChangesGoOutBeforeTheMinimumInterval(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.checkInMillis = 900000;
            h.stub.minUpdateIntervalMillis = 60000;
            var accepting = new AtomicBoolean(true);
            var players = new AtomicInteger(0);
            var transport = new FakeTransport();
            transport.revision = 1;
            transport.version = 1;
            var client = h.client(h.configuration().build(), path, transport, () -> null,
                    () -> new Health(accepting.get(), 20, SOFTWARE, h.connected(players.get())));
            h.await(client.start());
            int before = h.stub.heartbeats;
            accepting.set(false);
            h.run(Duration.ofSeconds(2));
            assertEquals(before + 1, h.stub.heartbeats, "A pause goes out at once");
            assertFalse(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean());
            accepting.set(true);
            client.requestStatusRefresh();
            h.settle();
            assertTrue(h.stub.lastHeartbeat.get("acceptingPlayers").getAsBoolean(), "So does the resumption");
            transport.candidates = List.of(HostCandidate.host(new InetSocketAddress("127.0.0.1", 19200)));
            transport.revision++;
            transport.version++;
            h.run(Duration.ofSeconds(2));
            assertEquals(19200, h.stub.lastHostProfile.getAsJsonArray("candidates").get(0).getAsJsonObject()
                    .get("port").getAsInt(), "A replaced endpoint goes out at once");
            before = h.stub.heartbeats;
            players.set(3);
            client.requestStatusRefresh();
            h.run(Duration.ofSeconds(30));
            assertEquals(before, h.stub.heartbeats, "A count waits for the provider's minimum interval");
            h.run(Duration.ofSeconds(31));
            assertEquals(3, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
        }
    }

    @Test
    void registrationRefreshRotationAndRestart(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var players = new AtomicInteger(2);
            var renamed = new AtomicReference<ServerStatus>();
            var transport = new FakeTransport();
            var client = h.client(h.configuration().build(), path, transport,
                    () -> renamed.get() != null ? renamed.get() : new ServerStatus("Example", "", 40, 0),
                    () -> new Health(true, 100, SOFTWARE, h.connected(players.get())));
            assertEquals("example-machine-1", h.await(client.start()).instanceId());
            assertEquals(1, transport.installs.size());
            assertEquals(100, h.stub.lastHeartbeat.get("capacity").getAsInt());
            assertEquals(40, h.stub.lastHeartbeat.getAsJsonObject("serverStatus").get("maxPlayerCount").getAsInt());
            players.set(7);
            h.run(Duration.ofSeconds(2));
            assertEquals(7, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            renamed.set(new ServerStatus("Renamed", "World", 30, 2));
            h.advance(Duration.ofSeconds(1));
            client.requestStatusRefresh();
            h.settle();
            assertEquals("Renamed", h.stub.lastHeartbeat.getAsJsonObject("serverStatus").get("name").getAsString());
            String oldKey = h.stub.registration.get("keyId").getAsString();
            var rotated = h.await(client.rotateMachineKey());
            assertNotEquals(oldKey, rotated.keyId());
            assertFalse(h.stub.keys.containsKey(oldKey), "The old machine key was retired");
            assertTrue(h.await(client.readiness()).routable(), "Signed with the replacement key");
            h.await(client.drain());
            assertTrue(h.stub.draining);
            h.stop(client);
            var resumed = h.client(h.configuration().build(), path, new FakeTransport(),
                    () -> new ServerStatus("Restarted", "", 50, 1), () -> new Health(true, 100, SOFTWARE, h.connected(0)));
            assertEquals("example-machine-1", h.await(resumed.start()).instanceId());
            assertEquals(1, h.stub.registrations);
            assertEquals(2, h.stub.generation);
        }
    }

    @Test
    void retirementRepliesAndRestartNeverExtendTheSavedCutoff(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.checkInMillis = 3_600_000;
            var first = new FakeTransport();
            var client = h.client(path, first);
            h.await(client.start());
            long cutoff = h.scheduler.millis() + 300_000;
            JsonObject retirement = new JsonObject();
            retirement.addProperty("keyId", "T001");
            retirement.addProperty("retireAfter", cutoff);
            h.stub.heartbeatRetirements = new JsonArray();
            h.stub.heartbeatRetirements.add(retirement);
            h.await(client.rotateAdmissionKey());
            var latest = first.installs.getLast();
            assertEquals(List.of("T001", "T002"), latest.stream().map(k -> k.keyId()).toList());
            assertEquals(cutoff, latest.getFirst().retireAfter());
            assertEquals(Long.MAX_VALUE, latest.get(1).retireAfter());
            // Even a provider that rebases a later reply cannot extend the original bound
            h.await(client.readiness());
            JsonArray later = h.stub.heartbeatRetirements.deepCopy();
            later.get(0).getAsJsonObject().addProperty("retireAfter", cutoff + 300_000);
            h.stub.heartbeatRetirements = later;
            h.await(client.readiness());
            assertEquals(cutoff, first.installs.getLast().getFirst().retireAfter());
            h.stop(client);
            var saved = JsonParser.parseString(Files.readString(path.resolve("provider-state.json"))).getAsJsonObject()
                    .getAsJsonArray("ticketKeys");
            assertEquals(cutoff, saved.get(0).getAsJsonObject().get("retireAfter").getAsLong());
            var restarted = new FakeTransport();
            var resumed = h.client(path, restarted);
            h.await(resumed.start());
            h.await(resumed.readiness());
            for (var snapshot : restarted.installs) {
                assertEquals(List.of("T001", "T002"), snapshot.stream().map(k -> k.keyId()).toList());
                assertEquals(cutoff, snapshot.getFirst().retireAfter(), "The first install after a restart keeps it");
            }
            h.advance(Duration.ofMinutes(5));
            h.await(resumed.readiness());
            assertEquals(List.of("T002"), restarted.installs.getLast().stream().map(k -> k.keyId()).toList(),
                    "A retired epoch is installed no more");
        }
    }

    @Test
    void aFailedRefreshRetriesAndBurstsCoalesce(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var players = new AtomicInteger(2);
            var client = h.client(h.configuration().build(), path, new FakeTransport(), () -> {
                if (players.get() < 0) {
                    throw new IllegalStateException("query unavailable");
                }
                return new ServerStatus("Example", "", 40, 0);
            }, () -> new Health(true, 100, SOFTWARE, h.connected(Math.max(0, players.get()))));
            h.await(client.start());
            players.set(-1);
            h.run(Duration.ofSeconds(2));
            assertFalse(h.stub.lastHeartbeat.has("serverStatus"));
            int before = h.stub.heartbeats;
            players.set(5);
            h.stub.failHeartbeats = 1;
            for (int i = 0; i < 500; i++) {
                client.requestStatusRefresh();
            }
            h.run(Duration.ofSeconds(3));
            assertTrue(h.stub.lastHeartbeat.has("serverStatus"));
            assertEquals(5, h.stub.lastHeartbeat.getAsJsonObject("connectedPlayers").get("count").getAsInt());
            assertTrue(h.stub.heartbeats - before <= 3, "A burst coalesces within the heartbeat cadence");
        }
    }

    @Test
    void reportsTheEndpointsTheProviderAcknowledged(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var client = h.client(path, new FakeTransport());
            assertTrue(client.publishedEndpoints().isEmpty());
            h.await(client.start());
            var published = client.publishedEndpoints().orElseThrow();
            assertEquals(List.of(new InetSocketAddress("127.0.0.1", 19133)), published.endpoints());
            assertTrue(published.assistedFamilies().isEmpty());
            h.await(client.drain());
            assertTrue(client.publishedEndpoints().isEmpty(), "A draining host offers nothing");
        }
    }

    @Test
    void republishesTheProfileWhenTheProviderHoldsNone(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var client = h.client(path, new FakeTransport());
            h.await(client.start());
            h.await(client.readiness());
            assertFalse(h.stub.lastHeartbeat.has("hostProfile"));
            int published = h.stub.profileRevision;
            h.stub.forgetProfile = true;
            h.await(client.readiness());
            assertEquals(published + 1, h.stub.profileRevision, "The forgotten profile is sent again at once");
            assertEquals(List.of(new InetSocketAddress("127.0.0.1", 19133)),
                    client.publishedEndpoints().orElseThrow().endpoints(), "Known again once republished");
        }
    }

    @Test
    void bedrockProtocolsGoWithEachProfileOnlyWhenConfigured(@TempDir Path path) throws Exception {
        String member = "\"bedrockProtocols\":{\"accepted\":[818,819,2225],\"version\":\"26.60\"}";
        try (var h = new ClientHarness()) {
            var client = h.client(h.configuration().bedrockProtocols(new BedrockProtocols(List.of(818, 819, 2225),
                            "26.60")).build(), path.resolve("configured"), new FakeTransport(),
                    () -> new ServerStatus("Example", "", 40, 0), () -> new Health(true, 10, SOFTWARE, h.connected(0)));
            h.await(client.start());
            String initial = lastProfileHeartbeat(h.stub);
            assertTrue(initial.contains("\"location\":null," + member + ",\"serverStatus\":"), initial);
            h.await(client.readiness());
            assertFalse(h.stub.lastHeartbeat.has("bedrockProtocols"), "Acknowledged protocols are not repeated");
            h.stub.forgetProfile = true;
            h.await(client.readiness());
            assertTrue(lastProfileHeartbeat(h.stub).contains(member), "A new profile carries them again");
        }
        try (var h = new ClientHarness()) {
            var client = h.client(h.configuration().build(), path.resolve("unconfigured"), new FakeTransport(),
                    () -> new ServerStatus("Example", "", 40, 0), () -> new Health(true, 10, SOFTWARE, h.connected(0)));
            h.await(client.start());
            assertTrue(lastProfileHeartbeat(h.stub).contains("\"location\":null,\"serverStatus\":"));
            assertTrue(h.stub.heartbeatBodies.stream().noneMatch(body -> body.contains("bedrockProtocols")));
        }
    }

    private static String lastProfileHeartbeat(IndependentProviderStub stub) {
        return stub.heartbeatBodies.stream().filter(body -> body.contains("\"hostProfile\":"))
                .reduce((first, second) -> second).orElseThrow();
    }

    @Test
    void keepsTheActiveEpochWhileDrainingWithoutAProfile(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            h.await(client.drain());
            assertEquals("T001", h.stub.lastHeartbeat.getAsJsonArray("installedAdmissionKeyIds").get(0).getAsString());
            h.await(client.rotateAdmissionKey());
            assertEquals("T002", transport.installs.getLast().getLast().keyId(), "The delivered epoch is installed");
            assertFalse(h.stub.lastHeartbeat.has("hostProfile"));
            assertFalse(h.stub.lastHeartbeat.has("installedAdmissionKeyIds"),
                    "A new active epoch waits for a matching profile");
        }
    }

    @Test
    void readinessChangesAreLoggedOncePerChangeWithTheProvidersReasons(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var client = h.client(path, new FakeTransport());
            h.await(client.start());
            assertEquals(1, h.logged(ProviderDiagnostic.Level.INFO, "is routing players to this server.").size());
            h.stub.readiness = JsonParser.parseString(
                    "{\"routable\":false,\"reasons\":[\"lease_expired\",\"Not A Code\"]}").getAsJsonObject();
            var readiness = h.await(client.readiness());
            assertFalse(readiness.routable());
            assertEquals(List.of("lease_expired"), readiness.reasons(), "Only provider codes are kept");
            h.await(client.readiness());
            assertEquals(1, h.logged(ProviderDiagnostic.Level.WARN, "not routing players to this server: lease_expired.")
                    .size());
            h.stub.readiness = null;
            h.await(client.readiness());
            assertEquals(1, h.logged(ProviderDiagnostic.Level.INFO, "routing players to this server again.").size());
        }
    }
}

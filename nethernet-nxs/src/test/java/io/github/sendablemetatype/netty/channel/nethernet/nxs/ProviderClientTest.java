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
}

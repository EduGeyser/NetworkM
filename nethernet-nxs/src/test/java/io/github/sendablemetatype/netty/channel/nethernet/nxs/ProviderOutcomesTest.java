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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.GameOutcome;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.Health;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Set;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.ClientHarness.SOFTWARE;
import static org.junit.jupiter.api.Assertions.*;

/** Outcome reports: collected each second, saved, batched within ten seconds, and never blocking anything else. */
class ProviderOutcomesTest {
    private static TicketId ticket(int number) {
        return new TicketId(HexFormat.of().formatHex(new byte[16]).substring(8) + String.format("%08x", number));
    }

    private static TransportOutcome open(int number, long at) {
        return new TransportOutcome(ticket(number), OutcomeStage.DATA_CHANNELS_OPEN, at, null, null);
    }

    private static int savedOutcomes(Path path) throws IOException {
        JsonObject state = JsonParser.parseString(Files.readString(path.resolve("provider-state.json"))).getAsJsonObject();
        return state.has("pendingEvents") ? state.getAsJsonArray("pendingEvents").size() : 0;
    }

    private static JsonObject connectivity() {
        return JsonParser.parseString("{\"dev.opencollab.nxs.connectivity\":{\"version\":1,\"critical\":false,\"data\":{}}}")
                .getAsJsonObject();
    }

    @Test
    void anOutageBacksOffWhileHeartbeatsContinueAndShutdownStillDelivers(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.extensionMetadata = connectivity();
            h.stub.failOutcomes = true;
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            transport.outcomes.add(new TransportOutcome(ticket(1), OutcomeStage.DATA_CHANNELS_OPEN, 1788739200000L,
                    "both_channels_open", new InetSocketAddress("2001:db8::1234", 54321)));
            h.run(Duration.ofSeconds(11));
            assertEquals(1, h.stub.outcomeAttempts);
            int before = h.stub.heartbeats;
            h.run(Duration.ofSeconds(5));
            assertTrue(h.stub.heartbeats >= before + 2, "Heartbeats continue during the outcome outage");
            assertEquals(1, h.stub.outcomeAttempts, "The outcome failure backs off on its own");
            JsonObject saved = JsonParser.parseString(Files.readString(path.resolve("provider-state.json")))
                    .getAsJsonObject().getAsJsonArray("pendingEvents").get(0).getAsJsonObject();
            assertEquals("2001:db8:0:0:0:0:0:1234", saved.get("remoteAddress").getAsString());
            assertEquals(54321, saved.get("remotePort").getAsInt());
            h.stub.failOutcomes = false;
            h.stop(client);
            assertEquals(1, h.stub.events.size(), "Shutdown retries the saved outcome without losing it");
            assertEquals(54321, h.stub.events.getFirst().get("remotePort").getAsInt());
            assertEquals("both_channels_open", h.stub.events.getFirst().get("reason").getAsString());
        }
    }

    @Test
    void aPermanentlyRejectedBatchIsDroppedRatherThanBlockingLaterOnes(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            h.stub.rejectOutcomes = 1;
            var client = h.client(path, transport);
            h.await(client.start());
            transport.outcomes.add(open(1, h.scheduler.millis()));
            h.run(Duration.ofSeconds(11));
            assertEquals(1, h.stub.outcomeAttempts);
            transport.outcomes.add(open(2, h.scheduler.millis()));
            h.run(Duration.ofSeconds(11));
            assertEquals(1, h.stub.events.size());
            assertEquals(ticket(2).value(), h.stub.events.getFirst().get("ticketId").getAsString());
        }
    }

    @Test
    void outcomesWithinTenSecondsGoOutAsOneBatch(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            transport.outcomes.add(open(1, h.scheduler.millis()));
            h.run(Duration.ofSeconds(2));
            transport.outcomes.add(open(2, h.scheduler.millis()));
            h.run(Duration.ofSeconds(2));
            transport.outcomes.add(open(3, h.scheduler.millis()));
            h.run(Duration.ofSeconds(2));
            assertEquals(0, h.stub.outcomeAttempts, "The batch waits for later outcomes");
            h.run(Duration.ofSeconds(6));
            assertEquals(3, h.stub.events.size());
            assertEquals(1, h.stub.outcomeAttempts);
        }
    }

    @Test
    void aFullBatchIsSentWithoutWaiting(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            for (int i = 0; i < 100; i++) {
                transport.outcomes.add(open(i, h.scheduler.millis()));
            }
            h.run(Duration.ofSeconds(1));
            assertEquals(100, h.stub.events.size());
            assertEquals(1, h.stub.outcomeAttempts);
        }
    }

    @Test
    void drainSendsWaitingOutcomesAtOnce(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            transport.outcomes.add(open(1, h.scheduler.millis()));
            h.run(Duration.ofSeconds(1));
            assertEquals(1, savedOutcomes(path), "Saved as soon as it is collected, then held for its batch");
            assertEquals(0, h.stub.outcomeAttempts);
            h.await(client.drain());
            assertEquals(1, h.stub.events.size());
            assertEquals(0, savedOutcomes(path));
        }
    }

    @Test
    void gameOutcomesAreReportedOnlyWhenDeclaredAndOnlyAsCodes(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var undeclared = h.client(path.resolve("off"), new FakeTransport());
            assertThrows(IllegalStateException.class,
                    () -> undeclared.reportGameOutcome(ticket(1), GameOutcome.JOINED, null));
            var client = h.client(h.configuration().gameOutcomes(true).build(), path.resolve("on"), new FakeTransport(),
                    () -> null, () -> new Health(true, 10, SOFTWARE, h.connected(0)));
            h.await(client.start());
            assertEquals("available", h.stub.lastHeartbeat.get("gameOutcomes").getAsString());
            // Refused locally, so a bad report can never take a batch of good ones with it
            assertThrows(IllegalArgumentException.class,
                    () -> client.reportGameOutcome(ticket(1), GameOutcome.REJECTED, "Player was kicked: bad name"));
            long before = h.scheduler.millis();
            assertTrue(client.reportGameOutcome(ticket(1), GameOutcome.REJECTED, "unsupported_version"));
            h.run(Duration.ofSeconds(11));
            JsonObject event = h.stub.events.getFirst();
            assertEquals("ticket.game_rejected", event.get("stage").getAsString());
            assertEquals(ticket(1).value(), event.get("ticketId").getAsString());
            assertEquals("unsupported_version", event.get("reason").getAsString());
            assertTrue(event.get("occurredAt").getAsLong() >= before);
            h.stop(client);
            assertFalse(client.reportGameOutcome(ticket(2), GameOutcome.JOINED, null));
        }
    }

    @Test
    void outcomesOmitUnadvertisedConnectivityMetadata(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            transport.outcomes.add(new TransportOutcome(ticket(1), OutcomeStage.DATA_CHANNELS_OPEN, 1788739200000L,
                    null, new InetSocketAddress("8.8.8.8", 54321)));
            h.run(Duration.ofSeconds(11));
            assertEquals(Set.of("ticketId", "stage", "occurredAt"), h.stub.events.getFirst().keySet());
        }
    }

    @Test
    void aBurstGoesOutInBoundedBatches(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            for (int i = 0; i < 250; i++) {
                transport.outcomes.add(open(i, 1788739200000L));
            }
            h.run(Duration.ofSeconds(13));
            assertEquals(250, h.stub.events.size());
            assertEquals(3, h.stub.outcomeAttempts, "At most 100 to a batch");
        }
    }

    @Test
    void failedPlayersWarnUnlessAssistanceAlreadyDid(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            transport.outcomes.add(new TransportOutcome(ticket(1), OutcomeStage.FAILED, 1, "dtls_failed", null));
            transport.outcomes.add(new TransportOutcome(ticket(2), OutcomeStage.FAILED, 1, "assisted_answer_failed", null));
            h.run(Duration.ofSeconds(1));
            assertEquals(1, h.logged(ProviderDiagnostic.Level.WARN, "A player could not connect to the server.").size());
        }
    }
}

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
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.Health;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.Location;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.ClientHarness.SOFTWARE;
import static org.junit.jupiter.api.Assertions.*;

/** The operator journeys a host goes through with one provider, end to end. */
class ProviderJourneysTest {
    private static final TicketId TICKET = new TicketId("00112233445566778899aabbccddeeff");

    private static ProviderClient client(ClientHarness h, Path directory, ProviderClient.Configuration configuration,
                                         FakeTransport transport) throws Exception {
        return h.client(configuration, directory, transport, () -> new ServerStatus("Independent host", "world", 20, 0),
                () -> new Health(true, 100, SOFTWARE, h.connected(0)));
    }

    @Test
    void allFourOperatorJourneysUseOneNeutralLifecycle(@TempDir Path directory) throws Exception {
        for (String journey : List.of("anonymous-standalone", "token-new-service", "token-fleet-attachment",
                "custom-host-provider")) {
            try (var h = new ClientHarness()) {
                boolean bearer = !journey.equals("anonymous-standalone");
                h.stub.attachToken = journey.equals("token-fleet-attachment");
                var configuration = h.configuration().label(journey)
                        .authorizationToken(bearer ? "independent-provider-token" : null).build();
                var transport = new FakeTransport();
                var instance = client(h, directory.resolve(journey), configuration, transport);
                h.await(instance.start());
                assertTrue(h.await(instance.extensions()).isEmpty(), "A provider needs no product extension");
                assertEquals(bearer ? 0 : 2, h.stub.challengeDifficulty);
                assertTrue(h.stub.keyAcknowledgements > 0);
                assertTrue(h.await(instance.readiness()).routable());
                transport.outcomes.add(new TransportOutcome(TICKET, OutcomeStage.DATA_CHANNELS_OPEN,
                        h.scheduler.millis(), "connected", null));
                h.run(Duration.ofSeconds(11));
                assertEquals(1, h.stub.events.size());
                h.await(instance.deregister());
                assertTrue(h.stub.draining);
            }
        }
    }

    @Test
    void rejectsUnknownModesAndAnonymousAttachment(@TempDir Path directory) throws Exception {
        for (String selected : List.of("automatic", "unknown", "attach-instance")) {
            try (var h = new ClientHarness()) {
                h.stub.selectedMode = selected;
                var instance = client(h, directory.resolve(selected), h.configuration().build(), new FakeTransport());
                assertNotNull(h.failure(instance.start()));
                assertFalse(h.stub.operationsSeen.contains("/example/complete"));
            }
        }
    }

    @Test
    void olderStateKeepsDurableIdentityAndAssignedIds(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            var configuration = h.configuration().label("Migration").build();
            var first = client(h, directory, configuration, new FakeTransport());
            var registration = h.await(first.start());
            h.stop(first);
            JsonObject previous;
            try (ProviderStateStore store = new ProviderStateStore(directory)) {
                previous = store.read();
                // The upstream client's layout: no version, extra members this client does not read
                JsonObject legacy = previous.deepCopy();
                legacy.remove("stateVersion");
                legacy.remove("protocol");
                legacy.addProperty("profilePublishedAt", 1);
                legacy.getAsJsonObject("registration").addProperty("profile", "legacy-profile-fixture");
                store.write(legacy);
            }
            var resumed = client(h, directory, configuration, new FakeTransport());
            var migrated = h.await(resumed.start());
            assertEquals(registration.instanceId(), migrated.instanceId());
            assertEquals(registration.serviceId(), migrated.serviceId());
            assertEquals(registration.registrationId(), migrated.registrationId());
            assertEquals(registration.keyId(), migrated.keyId());
            assertEquals(1, h.stub.registrations);
            assertEquals(2, h.stub.generation);
            h.stop(resumed);
            try (ProviderStateStore store = new ProviderStateStore(directory)) {
                JsonObject current = store.read();
                assertEquals(previous.get("privateKey"), current.get("privateKey"));
                assertEquals(previous.get("publicKeyJwk"), current.get("publicKeyJwk"));
                assertEquals(ProviderState.VERSION, current.get("stateVersion").getAsInt());
            }
        }
    }

    @Test
    void stateFromALaterClientIsNotGuessedAt(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            try (ProviderStateStore store = new ProviderStateStore(directory)) {
                JsonObject later = new JsonObject();
                later.addProperty("stateVersion", ProviderState.VERSION + 1);
                store.write(later);
            }
            var client = client(h, directory, h.configuration().build(), new FakeTransport());
            assertTrue(h.failure(client.start()).getMessage().contains("newer client"));
        }
    }

    @Test
    void aRequiredUnknownExtensionFailsBeforeCredentialsAreSent(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.extensionMetadata = JsonParser.parseString(
                    "{\"org.example.required\":{\"version\":1,\"critical\":true,\"data\":{}}}").getAsJsonObject();
            var instance = client(h, directory, h.configuration().authorizationToken("independent-provider-token")
                    .build(), new FakeTransport());
            assertNotNull(h.failure(instance.start()));
            assertNull(h.stub.challengeAuthorization);
            assertEquals(0, h.stub.registrations);
        }
    }

    @Test
    void recoversACommittedRegistrationWhenTheCompletionReplyIsLost(@TempDir Path directory) throws Exception {
        // Bearer attachment too: a recovery challenge has no bearer authorization, while the recovered registration
        // keeps the attachment
        for (boolean attach : new boolean[]{false, true}) {
            try (var h = new ClientHarness()) {
                h.stub.attachToken = attach;
                Path state = directory.resolve(attach ? "attached" : "standalone");
                var configuration = h.configuration().label("Lost completion")
                        .authorizationToken(attach ? "independent-provider-token" : null).build();
                h.stub.loseCompletionResponse = true;
                var first = client(h, state, configuration, new FakeTransport());
                // The retried completion finds the challenge consumed and recovers inline
                String registrationId = h.settled(first.start()).registrationId();
                assertEquals(h.stub.registration.get("registrationId").getAsString(), registrationId);
                assertNotEquals(h.stub.initialChallengeId, registrationId, "The recovered registration keeps its own ID");
                assertEquals(1, h.stub.registrations, "The provider committed despite the lost reply");
                assertEquals(2, h.stub.generation);
                assertTrue(h.await(first.readiness()).routable());
                assertTrue(h.stub.keyAcknowledgements > 0, "Lost one-time key material is provisioned afresh");
                h.stop(first);
                try (ProviderStateStore store = new ProviderStateStore(state)) {
                    JsonObject after = store.read();
                    assertFalse(after.has("challenge"));
                    assertTrue(after.has("registration"));
                }
            }
        }
    }

    @Test
    void anExpiredUnfinishedChallengeIsReplaced(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            var configuration = h.configuration().label("Expired").build();
            h.stub.challengeLifetimeMillis = 0;
            var first = client(h, directory, configuration, new FakeTransport());
            assertNotNull(h.failure(first.start()));
            h.stop(first);
            h.stub.challengeLifetimeMillis = 60000;
            var resumed = client(h, directory, configuration, new FakeTransport());
            h.await(resumed.start());
            assertEquals(1, h.stub.registrations);
        }
    }

    @Test
    void aForgottenRegistrationRegistersAgainWithAFreshKey(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            var configuration = h.configuration().label("Forgotten").build();
            var first = client(h, directory, configuration, new FakeTransport());
            String before = h.await(first.start()).registrationId();
            h.stop(first);
            String oldKey = JsonParser.parseString(Files.readString(directory.resolve("provider-state.json")))
                    .getAsJsonObject().get("privateKey").getAsString();
            synchronized (h.stub) {
                h.stub.registration = null;
            }
            var resumed = client(h, directory, configuration, new FakeTransport());
            assertNotEquals(before, h.await(resumed.start()).registrationId());
            assertEquals(2, h.stub.registrations);
            assertFalse(h.logged(ProviderDiagnostic.Level.WARN, "example-machine-1").isEmpty());
            String newKey = JsonParser.parseString(Files.readString(directory.resolve("provider-state.json")))
                    .getAsJsonObject().get("privateKey").getAsString();
            assertNotEquals(oldKey, newKey, "A new instance gets a fresh machine key");
        }
    }

    @Test
    void aDeregisteredClientStopsReporting(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = client(h, directory, h.configuration().label("Gone").build(), transport);
            h.await(client.start());
            h.await(client.deregister());
            transport.outcomes.add(new TransportOutcome(TICKET, OutcomeStage.DATA_CHANNELS_OPEN, h.scheduler.millis(),
                    null, null));
            h.run(Duration.ofSeconds(12));
            assertEquals(0, h.stub.outcomeAttempts);
            JsonObject state = JsonParser.parseString(Files.readString(directory.resolve("provider-state.json")))
                    .getAsJsonObject();
            assertFalse(state.has("registration"));
            assertFalse(state.has("privateKey"), "A deregistered instance can never recover, so its key goes too");
        }
    }

    @Test
    void operatorActionsComeFromTheCompletionThenOnRequest(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.operatorActions = true;
            var configuration = h.configuration().label("Action").build();
            var client = client(h, directory, configuration, new FakeTransport());
            h.await(client.start());
            var first = h.await(client.operatorAction()).orElseThrow();
            assertEquals(URI.create("https://panel.example/claim/first"), first.url());
            assertFalse(first.toString().contains("claim"), "The link stays out of logs");
            assertEquals(0, h.stub.operatorActionRequests, "An unexpired action is reused");
            assertFalse(Files.readString(directory.resolve("provider-state.json")).contains("panel.example"));
            h.stop(client);
            var resumed = client(h, directory, configuration, new FakeTransport());
            h.await(resumed.start());
            var fresh = h.await(resumed.operatorAction()).orElseThrow();
            assertEquals(URI.create("https://panel.example/claim/1"), fresh.url());
            assertEquals(1, h.stub.operatorActionRequests);
            h.stub.operatorActions = false;
            h.await(resumed.readiness());
            assertTrue(h.await(resumed.operatorAction()).isPresent(), "A current action is kept");
        }
    }

    @Test
    void theLocationIsSentWithEachProfileAndNullClearsIt(@TempDir Path directory) throws Exception {
        try (var h = new ClientHarness()) {
            var located = h.configuration().label("Located")
                    .location(Location.of(Map.of("country", " gb ", "latitude", "51.5", "longitude", "-0.12"))).build();
            var client = client(h, directory, located, new FakeTransport());
            h.await(client.start());
            assertEquals(JsonParser.parseString("{\"country\":\"GB\",\"latitude\":51.5,\"longitude\":-0.12}"),
                    h.stub.lastHeartbeat.get("location"));
            h.await(client.readiness());
            assertFalse(h.stub.lastHeartbeat.has("location"), "An unchanged location is not repeated");
            h.stop(client);
            var resumed = client(h, directory, h.configuration().label("Located").build(), new FakeTransport());
            h.await(resumed.start());
            assertTrue(h.stub.lastHeartbeat.get("location").isJsonNull(), "Dropping the setting clears the declaration");
        }
        for (var invalid : List.of(Map.of("city", "London"), Map.of("latitude", "0"), Map.of("country", "GBR"),
                Map.of("latitude", "91", "longitude", "0"), Map.of("latitude", "NaN", "longitude", "0"),
                Map.of("country", "GB", "extra", "x"))) {
            assertThrows(IllegalArgumentException.class, () -> Location.of(invalid), invalid.toString());
        }
        assertNull(Location.of(Map.of()));
    }
}

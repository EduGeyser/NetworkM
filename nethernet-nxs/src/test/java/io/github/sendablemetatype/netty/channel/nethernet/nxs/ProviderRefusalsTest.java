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

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ProviderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Provider refusals, backoff, fencing, storage failure and clock skew. */
class ProviderRefusalsTest {
    private static ProviderException refusedStart(ClientHarness h, Path path) throws Exception {
        var client = h.client(path, new FakeTransport());
        var started = client.start();
        h.until(() -> h.stub.registrationRefusal.answered.get() > 0);
        // A refusal for now waits out its Retry-After, which stopping ends with that refusal
        h.stop(client);
        return assertInstanceOf(ProviderException.class, h.failure(started));
    }

    @Test
    void aRefusalCarriesTheProvidersMessageLinkAndRetryAfter(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.registrationRefusal = new IndependentProviderStub.Refusal(403, """
                    {"code":"capacity_reached","message":"Add a token, or start the server again later.",
                     "actionUrl":"https://provider.example/tokens"}
                    """, "3600");
            ProviderException refused = refusedStart(h, path);
            assertEquals(403, refused.status());
            assertEquals("capacity_reached", refused.code());
            assertEquals(Optional.of("Add a token, or start the server again later."), refused.providerMessage());
            assertEquals(Optional.of(URI.create("https://provider.example/tokens")), refused.actionUrl());
            assertEquals(Optional.of(Duration.ofHours(1)), refused.retryAfter());
            assertEquals("Add a token, or start the server again later. (403 capacity_reached) "
                    + "https://provider.example/tokens", refused.getMessage());
        }
    }

    @Test
    void anInvalidActionUrlIsDroppedAndTheCodeKept(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            // Error links never carry credentials, so one with userinfo is not shown
            h.stub.registrationRefusal = new IndependentProviderStub.Refusal(403, """
                    {"code":"registration_disabled","message":"Registration is closed.",
                     "actionUrl":"https://operator:secret@provider.example/tokens"}
                    """, null);
            var client = h.client(path, new FakeTransport());
            var refused = assertInstanceOf(ProviderException.class, h.failure(client.start()));
            assertEquals("registration_disabled", refused.code());
            assertEquals(Optional.empty(), refused.actionUrl());
            assertEquals(Optional.empty(), refused.retryAfter());
            assertEquals("Registration is closed. (403 registration_disabled)", refused.getMessage());
        }
    }

    @Test
    void aRetryAfterBeyondTheInPlaceBudgetKeepsTheProvidersCode(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.registrationRefusal = new IndependentProviderStub.Refusal(429, "{\"code\":\"rate_limited\"}", "60");
            ProviderException refused = refusedStart(h, path);
            assertEquals("rate_limited", refused.code());
            assertEquals(Optional.of(Duration.ofSeconds(60)), refused.retryAfter());
            // Not repeated in place: the start waits out the hint instead
            assertEquals(1, h.stub.registrationRefusal.answered.get());
        }
    }

    @Test
    void aRegistrationRefusedForNowIsTriedAgainAfterItsRetryAfter(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var refusal = new IndependentProviderStub.Refusal(403,
                    "{\"code\":\"capacity_reached\",\"message\":\"Full for now.\"}", "1");
            h.stub.registrationRefusal = refusal;
            var client = h.client(path, new FakeTransport());
            var started = client.start();
            String warning = "The signaling service cannot register this server yet: Full for now. "
                    + "(403 capacity_reached). Trying again in 10 seconds.";
            h.until(() -> !h.logged(ProviderDiagnostic.Level.WARN, warning).isEmpty());
            h.stub.registrationRefusal = null;
            h.advance(Duration.ofSeconds(9));
            assertFalse(started.isDone(), "Never sooner than ten seconds");
            h.advance(Duration.ofSeconds(1));
            assertEquals("example-machine-1", h.await(started).instanceId());
            assertEquals(1, refusal.answered.get());
        }
    }

    @Test
    void warningsNameWhatFailedWithoutQuotingResponses() {
        assertEquals("ConnectException: Connection refused",
                ProviderFailures.describe(new java.net.ConnectException("Connection refused")));
        assertEquals("IOException: Provider transport unavailable (UnknownHostException: agent.example)",
                ProviderFailures.describe(new CompletionException(new IOException("Provider transport unavailable",
                        new java.net.UnknownHostException("agent.example")))));
        assertEquals("SSLHandshakeException: PKIX path building failed",
                ProviderFailures.describe(new javax.net.ssl.SSLHandshakeException("PKIX path\nbuilding failed")));
        assertEquals("HttpConnectTimeoutException",
                ProviderFailures.describe(new java.net.http.HttpConnectTimeoutException("")));
        // A parser's message can quote the response, which may carry a claim link
        assertEquals("IllegalStateException",
                ProviderFailures.describe(new IllegalStateException("Not a JSON Object: {\"url\":\"secret\"}")));
        assertEquals(200, ProviderFailures.describe(new IOException("x".repeat(300))).length() - "IOException: ".length());
    }

    @Test
    void failedCheckInsBackOffFromTenSecondsToTwoMinutes() {
        var unreachable = new IOException("unreachable");
        long[] seconds = {10, 20, 40, 80, 120, 120};
        for (int failures = 1; failures <= seconds.length; failures++) {
            assertWithinJitter(seconds[failures - 1], ProviderFailures.retryDelayNanos(unreachable, failures));
        }
        // A hint replaces the streak's backoff, though never below 10 seconds
        assertWithinJitter(60, ProviderFailures.retryDelayNanos(new CompletionException(rateLimited(60)), 1));
        assertWithinJitter(10, ProviderFailures.retryDelayNanos(rateLimited(3), 5));
    }

    private static ProviderException rateLimited(long seconds) {
        return new ProviderException(429, "rate_limited", null, null, Duration.ofSeconds(seconds));
    }

    private static void assertWithinJitter(long seconds, long nanos) {
        long base = TimeUnit.SECONDS.toNanos(seconds);
        assertTrue(nanos >= base && nanos <= base + base / 10, nanos + " ns is not " + seconds + " s plus jitter");
    }

    @Test
    void aRefusedHeartbeatWaitsForItsRetryAfterInsteadOfTenSeconds(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var client = h.client(path, new FakeTransport());
            h.await(client.start());
            var refusal = new IndependentProviderStub.Refusal(429,
                    "{\"code\":\"rate_limited\",\"message\":\"Slow down.\"}", "30");
            h.stub.heartbeatRefusal = refusal;
            h.run(Duration.ofSeconds(2));
            assertEquals(1, refusal.answered.get());
            // The old fixed 10 second retry would have sent another by now
            h.run(Duration.ofSeconds(12));
            assertEquals(1, refusal.answered.get());
            assertFalse(h.logged(ProviderDiagnostic.Level.WARN, "Slow down. (429 rate_limited)").isEmpty());
            h.run(Duration.ofSeconds(20));
            assertEquals(2, refusal.answered.get());
            h.stub.heartbeatRefusal = null;
        }
    }

    @Test
    void aGoneRegistrationStopsSignalingUntilARestart(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            assertTrue(client.publishedEndpoints().isPresent());
            var refusal = new IndependentProviderStub.Refusal(403,
                    "{\"code\":\"registration_gone\",\"message\":\"Its owner removed it.\"}", null);
            h.stub.heartbeatRefusal = refusal;
            h.run(Duration.ofSeconds(2));
            h.until(transport.closed::isDone);
            assertTrue(client.publishedEndpoints().isEmpty());
            assertEquals(1, refusal.answered.get());
            // Neither drained nor deregistered, as the registration is no longer this process's
            assertFalse(h.stub.operationsSeen.contains("/example/deregister"));
            assertFalse(h.logged(ProviderDiagnostic.Level.WARN, "The signaling service no longer has this server's "
                    + "registration: Its owner removed it. (403 registration_gone). Signaling is stopping until the "
                    + "server restarts.").isEmpty());
            assertTrue(h.logged(ProviderDiagnostic.Level.WARN, "Retrying automatically").isEmpty());
        }
    }

    @Test
    void stopsWithoutDeregisteringWhenAnotherProcessTakesOver(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(h.configuration().deregisterOnShutdown(true).build(), path, transport, () -> null,
                    () -> new ProviderClient.Health(true, 10, ClientHarness.SOFTWARE, h.connected(0)));
            h.await(client.start());
            synchronized (h.stub) {
                h.stub.generation++; // Another process recovered this registration
            }
            h.run(Duration.ofSeconds(2));
            h.until(transport.closed::isDone);
            assertFalse(h.stub.operationsSeen.contains("/example/deregister"));
            assertFalse(h.logged(ProviderDiagnostic.Level.WARN, "Another process took over").isEmpty());
            try (ProviderStateStore state = new ProviderStateStore(path)) {
                assertTrue(state.read().has("registration"));
            }
        }
    }

    @Test
    void localPersistenceFailureStopsPublicationAndClosesTransport(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var transport = new FakeTransport();
            var client = h.client(path, transport);
            h.await(client.start());
            Files.move(path.resolve("provider-state.json"), path.resolve("saved-state.json"));
            Files.createDirectory(path.resolve("provider-state.json"));
            assertNotNull(h.failure(client.readiness()));
            h.until(transport.closed::isDone);
            assertFalse(h.logged(ProviderDiagnostic.Level.WARN, ProviderLog.Operation.STORAGE.failure).isEmpty());
        }
    }

    @Test
    void namesTheClockDifferenceWhenRegistrationIsRefusedAsStale(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            h.stub.clockOffsetMillis = 300_000;
            var client = h.client(path, new FakeTransport());
            var refused = assertInstanceOf(ProviderException.class, h.failure(client.start()));
            assertEquals("stale_provider_signature", refused.code());
            assertEquals(1, h.stub.staleSignatures);
            assertTrue(Math.abs(client.clockOffsetMillis() - 300_000) < 5_000, "offset " + client.clockOffsetMillis());
            // The registration challenge already said what time it is, so the warning precedes the refusal
            assertEquals(1, h.logged(ProviderDiagnostic.Level.WARN,
                    "This machine's clock is 5 minutes behind the signaling service,").size());
        }
    }

    @Test
    void warnsWhenTheClockDriftsAndSaysWhenItIsBackInStep(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var client = h.client(path, new FakeTransport());
            h.await(client.start());
            assertEquals(0, client.clockOffsetMillis());
            h.stub.clockOffsetMillis = -300_000; // This machine is now ahead
            assertNotNull(h.failure(client.readiness()));
            assertEquals(1, h.stub.staleSignatures);
            assertTrue(Math.abs(client.clockOffsetMillis() + 300_000) < 5_000, "offset " + client.clockOffsetMillis());
            assertFalse(h.logged(ProviderDiagnostic.Level.WARN,
                    "This machine's clock is 5 minutes ahead of the signaling service,").isEmpty());
            h.stub.clockOffsetMillis = 0;
            assertNotNull(h.await(client.readiness()));
            assertEquals(0, client.clockOffsetMillis());
            assertFalse(h.logged(ProviderDiagnostic.Level.INFO,
                    "This machine's clock is in step with the signaling service again.").isEmpty());
        }
    }

    @Test
    void anIncompleteHeartbeatReplyFailsTheHeartbeat(@TempDir Path path) throws Exception {
        try (var h = new ClientHarness()) {
            var client = h.client(path, new FakeTransport());
            h.await(client.start());
            h.stub.omitReadiness = true;
            var failure = h.failure(client.readiness());
            assertTrue(failure.getMessage().contains("readiness"), failure.toString());
        }
    }
}

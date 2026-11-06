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
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ControlCarrier;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ProviderException;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Sends one provider request over the WebSocket when it carries the operation, else over HTTPS, with a bounded
 * number of attempts. A failed WebSocket attempt is resent over HTTPS with the same signed inputs. A request whose
 * outcome is unknown (a lost reply, a 5xx, or a WebSocket frame that may have arrived) is ambiguous: its idempotency
 * key stays saved, and ordinary recovery must settle it before fresh state is sent. Used on the lifecycle executor.
 */
final class Exchanger {
    static final Duration TIMEOUT = Duration.ofSeconds(15);
    /** Outcome uploads try once and briefly, so they cannot starve heartbeat renewal. */
    static final Duration OUTCOME_TIMEOUT = Duration.ofSeconds(3);
    private static final Set<String> WEBSOCKET_OPERATIONS = Set.of("heartbeat", "outcomes", "rotate", "retire",
            "deregister");
    /** A provider cannot hold a host back for longer than this at a time. */
    private static final long MAX_RETRY_AFTER_SECONDS = 86400;
    /** The longest Retry-After waited out in place; a longer one fails the request with the provider's code. */
    private static final long IN_PLACE_RETRY_MILLIS = 10000;

    /**
     * @param body   The exact bytes sent and signed, or null for none
     * @param bearer The enrollment token, only on {@code register}
     */
    private record Request(URI uri, String method, String body, boolean signed, String intent, String bearer,
                           Duration timeout, int attempts, OwnershipGuard guard, String webSocketOperation) {
    }

    /** Whether a request's outcome may be unknown, carried across its attempts. */
    private static final class Attempts {
        boolean ambiguous;
    }

    private final ClientRuntime runtime;
    private final RequestSigner signer;
    private final ProviderHttp http;
    private final Supplier<ProviderWebSocket> websocket;
    private final Supplier<Map<String, String>> upgradeHeaders;
    private final Relinquish relinquish;

    /** Stops a client whose registration another process took over or the provider no longer has. */
    interface Relinquish {
        void fenced();

        void gone(ProviderException refusal);
    }

    Exchanger(ClientRuntime runtime, RequestSigner signer, ProviderHttp http, Supplier<ProviderWebSocket> websocket,
              Supplier<Map<String, String>> upgradeHeaders, Relinquish relinquish) {
        this.runtime = runtime;
        this.signer = signer;
        this.http = http;
        this.websocket = websocket;
        this.upgradeHeaders = upgradeHeaders;
        this.relinquish = relinquish;
    }

    URI operation(String name) throws IOException {
        return trusted(URI.create(runtime.origin + "/v1/nxs/" + name));
    }

    /** Refuses a URL outside the provider origin, or one carrying userinfo or a fragment. */
    URI trusted(URI uri) throws IOException {
        try {
            URI authority = URI.create(uri.getScheme() + "://" + uri.getRawAuthority());
            if (!ProviderOrigin.canonicalize(authority).equals(runtime.origin) || uri.getUserInfo() != null
                    || uri.getFragment() != null) {
                throw new IOException("Untrusted provider operation");
            }
            return uri;
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Untrusted provider operation", invalid);
        }
    }

    /** An unauthenticated read, such as discovery. */
    CompletableFuture<JsonObject> get(URI uri) {
        return exchange(new Request(uri, "GET", null, false, null, null, TIMEOUT, 3, OwnershipGuard.ALWAYS, null));
    }

    /** An unsigned registration step. */
    CompletableFuture<JsonObject> unsigned(String operation, String body, String bearer) {
        return Async.attempt(() -> exchange(new Request(operation(operation), "POST", body, false, null, bearer, TIMEOUT,
                3, OwnershipGuard.ALWAYS, null)));
    }

    CompletableFuture<JsonObject> signed(String operation, String body) {
        return signed(operation, body, UUID.randomUUID().toString(), OwnershipGuard.ALWAYS);
    }

    /**
     * A signed operation. Reserves the next sequence first.
     *
     * @param guard Checked before every attempt, once the WebSocket send is saved, and before the frame is written
     */
    CompletableFuture<JsonObject> signed(String operation, String body, String intent, OwnershipGuard guard) {
        boolean outcomes = operation.equals("outcomes");
        return Async.attempt(() -> {
            URI uri = operation(operation);
            runtime.requireResolved();
            signer.reserveSequence();
            return exchange(new Request(uri, "POST", body, true, intent, null,
                    outcomes ? OUTCOME_TIMEOUT : TIMEOUT, outcomes ? 1 : 3, guard,
                    WEBSOCKET_OPERATIONS.contains(operation) ? operation : null));
        });
    }

    /** A signed request to an operation an extension advertised, which only an application asks for. */
    CompletableFuture<JsonObject> signedTo(URI uri, String method, String body) {
        return Async.attempt(() -> {
            trusted(uri);
            runtime.requireResolved();
            signer.reserveSequence();
            return exchange(new Request(uri, method, body, true, UUID.randomUUID().toString(), null, TIMEOUT, 3,
                    OwnershipGuard.ALWAYS, null));
        });
    }

    private CompletableFuture<JsonObject> exchange(Request request) {
        return attempt(request, 0, new Attempts());
    }

    private CompletableFuture<JsonObject> attempt(Request request, int index, Attempts attempts) {
        Map<String, String> headers;
        try {
            request.guard().requireCurrent();
            headers = request.signed()
                    ? signer.sign(request.method(), request.uri(), request.body(), request.intent())
                    : request.bearer() == null ? Map.of() : Map.of("authorization", "Bearer " + request.bearer());
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(failure);
        }
        ProviderWebSocket socket = request.webSocketOperation() == null ? null : websocket.get();
        CompletableFuture<Optional<ProviderReply>> viaSocket = socket == null
                ? CompletableFuture.completedFuture(Optional.empty())
                : socket.exchange(request.webSocketOperation(), headers, request.body() == null ? "" : request.body(),
                        upgradeHeaders, request.timeout(), () -> beforeSend(request), request.guard())
                .exceptionallyAsync(failure -> {
                    // Retried over HTTPS with the same signed inputs; neither intent nor body is replaced
                    attempts.ambiguous |= runtime.keeper.state().pendingOperation() != null;
                    runtime.log.failed(ProviderLog.Operation.WEBSOCKET, ProviderFailures.describe(failure));
                    return Optional.empty();
                }, runtime.executor);
        return viaSocket.thenComposeAsync(reply -> {
            if (reply.isPresent()) {
                return handle(request, index, attempts, reply.get(), true);
            }
            request.guard().requireCurrent();
            return Async.within(http.send(request.uri(), request.method(), request.body(), headers, request.timeout()),
                            request.timeout().plusSeconds(1), runtime.executor, "Provider request")
                    .handleAsync((response, failure) -> {
                        if (failure == null) {
                            return handle(request, index, attempts, response, false);
                        }
                        attempts.ambiguous = true;
                        if (index == request.attempts() - 1) {
                            return CompletableFuture.<JsonObject>failedFuture(
                                    new IOException("Provider transport unavailable", Async.unwrap(failure)));
                        }
                        return Async.delay(runtime.executor, Duration.ofMillis((250L << index)
                                        + ThreadLocalRandom.current().nextLong(100)))
                                .thenCompose(ignored -> attempt(request, index + 1, attempts));
                    }, runtime.executor)
                    .thenCompose(next -> next);
        }, runtime.executor);
    }

    /** Saves that the operation may reach the provider, before any frame carries it. */
    private void beforeSend(Request request) throws IOException {
        request.guard().requireCurrent();
        runtime.keeper.save(runtime.keeper.state().withPendingOperation(request.intent()));
        try {
            request.guard().requireCurrent();
        } catch (RuntimeException replaced) {
            clearPending(); // Nothing was handed to the carrier yet
            throw replaced;
        }
    }

    private CompletableFuture<JsonObject> handle(Request request, int index, Attempts attempts, ProviderReply reply,
                                                 boolean usedWebSocket) {
        try {
            long received = runtime.clock.millis();
            reply.header("x-provider-time").ifPresent(value -> {
                try {
                    runtime.observeProviderTime(Long.parseLong(value.trim()), received);
                } catch (NumberFormatException ignored) {
                    // Not a time; nothing to learn from it
                }
            });
            int status = reply.status();
            boolean success = status / 100 == 2;
            Wire.ErrorReply error = success ? null : WireCodec.error(reply.body());
            Duration retryAfter = success ? null : retryAfter(reply);
            // An error reply had no effect. A previous ambiguous attempt still needs recovery.
            if (!attempts.ambiguous && status / 100 == 4) {
                clearPending();
            }
            if (status >= 500) {
                attempts.ambiguous = true;
            }
            if ((status == 429 || status == 502 || status == 503 || status == 504) && index < request.attempts() - 1) {
                long delay = 250L << index;
                if (retryAfter != null) {
                    delay = Math.max(delay, retryAfter.toMillis());
                }
                // A longer wait fails at once below, keeping the provider's code and hint for the caller
                if (delay <= IN_PLACE_RETRY_MILLIS) {
                    return Async.delay(runtime.executor,
                                    Duration.ofMillis(delay + ThreadLocalRandom.current().nextLong(100)))
                            .thenCompose(ignored -> attempt(request, index + 1, attempts));
                }
            }
            if (!success) {
                ProviderException refused = new ProviderException(status, error.code(), error.message(),
                        error.actionUrl(), retryAfter);
                if (request.signed() && ProviderFailures.GENERATION_FENCED.equals(error.code())) {
                    relinquish.fenced();
                } else if (request.signed() && ProviderFailures.REGISTRATION_GONE.equals(error.code())) {
                    relinquish.gone(refused);
                }
                return CompletableFuture.failedFuture(refused);
            }
            JsonObject result = WireCodec.object(reply.body());
            observeServerTime(result, received);
            if (request.signed()) {
                runtime.carried(usedWebSocket ? ControlCarrier.WEBSOCKET : ControlCarrier.HTTP);
                clearPending();
            }
            if (usedWebSocket) {
                runtime.log.recovered(ProviderLog.Operation.WEBSOCKET);
            }
            return CompletableFuture.completedFuture(result);
        } catch (IOException | RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    void clearPending() throws IOException {
        ProviderState state = runtime.keeper.state();
        if (state.pendingOperation() != null) {
            runtime.keeper.save(state.withPendingOperation(null));
        }
    }

    /** Retry-After in delta-seconds, the only form NXS gives a meaning, capped at a day; null otherwise. */
    private static Duration retryAfter(ProviderReply reply) {
        String value = reply.header("retry-after").orElse("").trim();
        if (!value.matches("[0-9]{1,18}")) {
            return null;
        }
        return Duration.ofSeconds(Math.min(Long.parseLong(value), MAX_RETRY_AFTER_SECONDS));
    }

    private void observeServerTime(JsonObject result, long received) {
        try {
            if (result.has("serverTime")) {
                runtime.observeProviderTime(result.get("serverTime").getAsLong(), received);
            }
        } catch (RuntimeException ignored) {
            // Not a time; nothing to learn from it
        }
    }
}

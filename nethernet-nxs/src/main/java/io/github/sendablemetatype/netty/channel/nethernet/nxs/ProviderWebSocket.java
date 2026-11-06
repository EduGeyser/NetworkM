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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.ControlSocket;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The optional WebSocket carrier for signed operations, and the receiver of assisted joins. Signing, sequencing,
 * retries and recovery stay with the client, which falls back to HTTPS with the same signed bytes. All state belongs
 * to the client's lifecycle executor.
 *
 * <p>One join never costs the connection: a join the host cannot serve is answered {@code accepted:false}, and a
 * result whose authority lapsed while it was prepared is dropped alone. Only a frame without a {@code kind}, or of a
 * known kind with an invalid shape, closes the socket, with code 1008.
 */
final class ProviderWebSocket implements AutoCloseable {
    static final int MAX_FRAME_BYTES = 524288;
    static final int MAX_ASSISTED_IN_FLIGHT = 32;
    static final List<String> AUTH_HEADERS = List.of("nxs-instance-id", "nxs-key-id", "nxs-timestamp",
            "nxs-signature-version", "nxs-generation", "nxs-sequence", "idempotency-key", "nxs-signature");
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    static final Duration PING_INTERVAL = Duration.ofSeconds(20);
    /** The longest a player's assisted attempt may take, on the provider's clock. */
    static final long MAX_PLAYER_ATTEMPT_MILLIS = 30_000;
    /**
     * How far past the longest attempt a join's deadline may seem. The clock offset comes from a reply that spent time
     * in transit, so a full-length attempt can look up to that much longer than it is.
     */
    static final long DEADLINE_TOLERANCE_MILLIS = 2000;
    /**
     * One operation, every assisted result that can be in flight and a ping fit at once, so a burst of results
     * never fails the ping or the operation for want of room.
     */
    static final ControlSocket.Limits LIMITS = new ControlSocket.Limits(MAX_FRAME_BYTES, 1024,
            MAX_ASSISTED_IN_FLIGHT + 2, 2L * MAX_FRAME_BYTES, TIMEOUT, TIMEOUT, TIMEOUT, Duration.ofSeconds(2));

    /** Serves assisted joins for the client. */
    interface AssistedJoins {
        /**
         * Prepares the answer for a join whose frame, owner and deadline were checked. Completes exceptionally to
         * refuse it.
         */
        CompletionStage<AssistedAnswer> answer(AssistedJoin join, long deadlineNanos);

        /** Holds while the authority behind the join's answer still holds; checked when the result is written. */
        OwnershipGuard authority(AssistedJoin join);

        void failed(AssistedJoin join);
    }

    @FunctionalInterface
    interface BeforeSend {
        /** Saves that the operation may have reached the provider, before any frame carries it. */
        void persist() throws IOException;
    }

    private final URI endpoint;
    private final EventLoopGroup group;
    private final SslContext tls;
    private final ScheduledExecutorService owner;
    private final ProviderClock clock;
    private final LongSupplier providerOffsetMillis;
    private final AssistedJoins assisted;
    private final Runnable lostWhileAssisting;
    private final ScheduledFuture<?> keepAlive;
    private Connection current;
    private long retryAt;
    private int failures;
    private int assistedInFlight;
    private boolean closed;

    private static final class Connection {
        final String binding;
        ControlSocket socket;
        CompletableFuture<ProviderReply> pending;
        String id;
        boolean pingOutstanding;

        Connection(String binding) {
            this.binding = binding;
        }
    }

    /**
     * @param assisted           Null when assisted joins are off, so every join is refused
     * @param lostWhileAssisting Called on the owner after a connection drops, so the client can reconnect while it
     *                           assists joins instead of waiting for its next operation
     */
    ProviderWebSocket(URI endpoint, EventLoopGroup group, SslContext tls, ScheduledExecutorService owner,
                      ProviderClock clock, LongSupplier providerOffsetMillis, AssistedJoins assisted,
                      Runnable lostWhileAssisting) {
        this.endpoint = endpoint;
        this.group = group;
        this.tls = tls;
        this.owner = owner;
        this.clock = clock;
        this.providerOffsetMillis = providerOffsetMillis;
        this.assisted = assisted;
        this.lostWhileAssisting = lostWhileAssisting;
        keepAlive = owner.scheduleWithFixedDelay(this::keepAlive, PING_INTERVAL.toNanos(), PING_INTERVAL.toNanos(),
                TimeUnit.NANOSECONDS);
    }

    /**
     * Carries one signed operation. Runs on the owner.
     *
     * @return The reply, or empty when the caller should use HTTPS at once: during the reconnect backoff or for a
     * frame over the size limit
     */
    CompletableFuture<Optional<ProviderReply>> exchange(String operation, Map<String, String> signedHeaders, String body,
                                                         Supplier<Map<String, String>> upgradeHeaders, Duration timeout,
                                                         BeforeSend beforeSend, OwnershipGuard guard) {
        JsonObject frame = new JsonObject(), headers = new JsonObject();
        for (String name : AUTH_HEADERS) {
            headers.addProperty(name, Objects.requireNonNull(signedHeaders.get(name), name));
        }
        frame.addProperty("kind", "operation");
        frame.addProperty("operation", operation);
        frame.add("headers", headers);
        frame.addProperty("body", body);
        String wire = JSON.toJson(frame);
        if (closed) {
            return CompletableFuture.failedFuture(new IOException("WebSocket carrier is closed"));
        }
        if (wire.getBytes(StandardCharsets.UTF_8).length > MAX_FRAME_BYTES) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        String binding = signedHeaders.get("nxs-instance-id") + ":" + signedHeaders.get("nxs-generation");
        if (current != null && !binding.equals(current.binding)) {
            discard(current, false);
        }
        if (current == null && clock.nanos() - retryAt < 0) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        Connection connection = current != null ? current : connect(binding, upgradeHeaders.get());
        CompletableFuture<ProviderReply> reply = new CompletableFuture<>();
        return Async.within(connection.socket.opened(), TIMEOUT.plusSeconds(1), owner, "WebSocket upgrade")
                .thenCompose(opened -> {
                    if (current != connection) {
                        throw new IllegalStateException("WebSocket connection changed");
                    }
                    if (connection.pending != null) {
                        throw new IllegalStateException("Concurrent provider operation");
                    }
                    connection.pending = reply;
                    connection.id = signedHeaders.get("idempotency-key");
                    try {
                        beforeSend.persist();
                    } catch (IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                    connection.socket.sendText(wire, guard).whenComplete((ignored, failure) -> {
                        if (failure != null) {
                            reply.completeExceptionally(failure);
                        }
                    });
                    return Async.within(reply, timeout, owner, "WebSocket operation");
                })
                .handleAsync((result, failure) -> {
                    if (failure == null) {
                        if (current == connection) {
                            connection.pending = null;
                            connection.id = null;
                            failures = 0;
                        }
                        return Optional.of(result);
                    }
                    discard(connection, true);
                    throw new java.util.concurrent.CompletionException(Async.unwrap(failure));
                }, owner);
    }

    /**
     * Opens a connection now when there is none, after the reconnect backoff. Runs on the owner; used while assisted
     * joins are on, since the provider can push joins only over a live socket.
     */
    void connectNow(String binding, Supplier<Map<String, String>> upgradeHeaders) {
        if (closed || current != null) {
            return;
        }
        long wait = retryAt - clock.nanos();
        if (wait > 0) {
            owner.schedule(lostWhileAssisting, wait, TimeUnit.NANOSECONDS);
            return;
        }
        connect(binding, upgradeHeaders.get());
    }

    private Connection connect(String binding, Map<String, String> upgradeHeaders) {
        Connection connection = new Connection(binding);
        current = connection;
        connection.socket = ControlSocket.connect(group, endpoint, ProviderCrypto.PROTOCOL, upgradeHeaders, tls, LIMITS,
                owner, text -> receive(connection, text));
        connection.socket.closed().whenCompleteAsync((close, failure) -> lost(connection), owner);
        return connection;
    }

    private CompletionStage<Void> receive(Connection connection, String text) {
        if (current != connection || closed) {
            return Async.done();
        }
        if (text.equals("pong")) {
            connection.pingOutstanding = false;
            return Async.done();
        }
        String kind;
        try {
            kind = kind(text);
        } catch (IOException | IllegalStateException | IllegalArgumentException invalid) {
            return violation(connection, "frame without kind");
        }
        switch (kind) {
            case "assisted-join" -> {
                AssistedJoin join;
                try {
                    join = AssistedJoin.decode(text);
                } catch (IllegalArgumentException invalid) {
                    return violation(connection, "invalid assisted-join");
                }
                assistedJoin(connection, join);
            }
            case "response" -> {
                Parsed parsed;
                try {
                    parsed = parse(text);
                } catch (IOException invalid) {
                    return violation(connection, "invalid response");
                }
                // A reply to an operation that already fell back to HTTPS changes nothing
                if (connection.pending != null && parsed.id().equals(connection.id)) {
                    connection.pending.complete(parsed.reply());
                }
            }
            default -> {
                // Frames of later kinds are for receivers that understand them
            }
        }
        return Async.done();
    }

    private CompletionStage<Void> violation(Connection connection, String reason) {
        discard(connection, true, () -> connection.socket.closeForViolation(reason));
        return Async.done();
    }

    private void assistedJoin(Connection connection, AssistedJoin join) {
        long remaining = join.expiresAt() - (clock.millis() + providerOffsetMillis.getAsLong());
        long longest = join.diagnostic() ? DiagnosticAdmissionCodec.MAX_ATTEMPT_MILLIS : MAX_PLAYER_ATTEMPT_MILLIS;
        if (assisted == null
                || assistedInFlight >= MAX_ASSISTED_IN_FLIGHT
                || !connection.binding.equals(join.instanceId() + ":" + join.generation())
                || remaining <= 0
                || remaining > longest + DEADLINE_TOLERANCE_MILLIS) {
            refuse(connection, join);
            return;
        }
        long deadline = clock.nanos() + TimeUnit.MILLISECONDS.toNanos(Math.min(remaining, longest));
        boolean[] reported = {false};
        Runnable failed = () -> {
            if (!reported[0]) {
                reported[0] = true;
                assisted.failed(join);
            }
        };
        assistedInFlight++;
        Async.attempt(() -> assisted.answer(join, deadline))
                .handleAsync((answer, failure) -> {
                    OwnershipGuard guard = () -> {
                        if (closed || current != connection || clock.nanos() - deadline >= 0) {
                            throw new IllegalStateException("Assisted socket expired");
                        }
                    };
                    if (failure != null) {
                        failed.run();
                        return connection.socket.sendText(result(join, null), guard);
                    }
                    return connection.socket.sendText(result(join, answer), guard.and(assisted.authority(join)));
                }, owner)
                .thenCompose(sent -> sent)
                .whenCompleteAsync((ignored, failure) -> {
                    assistedInFlight--;
                    if (failure != null) {
                        // Only this result was dropped; the socket and the other joins carry on
                        failed.run();
                    }
                }, owner);
    }

    private void refuse(Connection connection, AssistedJoin join) {
        if (assisted != null) {
            assisted.failed(join);
        }
        connection.socket.sendText(result(join, null));
    }

    private static String result(AssistedJoin join, AssistedAnswer answer) {
        JsonObject reply = new JsonObject();
        reply.addProperty("kind", "assisted-join-result");
        reply.addProperty("id", join.id());
        reply.addProperty("accepted", answer != null);
        if (answer != null) {
            reply.addProperty("answer", answer.sdp());
        }
        return JSON.toJson(reply);
    }

    private record Parsed(String id, ProviderReply reply) {
    }

    /** Selects the decoder without making JSON whitespace or member order part of the protocol. */
    private static String kind(String wire) throws IOException {
        try (JsonReader reader = new JsonReader(new StringReader(wire))) {
            reader.setStrictness(Strictness.STRICT);
            reader.beginObject();
            while (reader.hasNext()) {
                if (reader.nextName().equals("kind")) {
                    return string(reader);
                }
                reader.skipValue();
            }
            throw new IOException("Provider frame without kind");
        }
    }

    /** A closed, shallow envelope; the operation's body stays the untouched string the provider signed nothing over. */
    private static Parsed parse(String wire) throws IOException {
        try (JsonReader reader = new JsonReader(new StringReader(wire))) {
            reader.setStrictness(Strictness.STRICT);
            Set<String> fields = new HashSet<>();
            String id = null, body = null;
            int status = 0;
            Map<String, List<String>> headers = new HashMap<>();
            reader.beginObject();
            while (reader.hasNext()) {
                String name = reader.nextName();
                if (!fields.add(name)) {
                    throw new IOException("Duplicate response field");
                }
                switch (name) {
                    case "kind" -> {
                        if (!string(reader).equals("response")) {
                            throw new IOException("Invalid response kind");
                        }
                    }
                    case "id" -> {
                        id = string(reader);
                        if (!id.matches("[A-Za-z0-9_-]{16,128}")) {
                            throw new IOException("Invalid response id");
                        }
                    }
                    case "status" -> {
                        if (reader.peek() != JsonToken.NUMBER) {
                            throw new IOException("Invalid response status");
                        }
                        String token = reader.nextString();
                        if (!token.matches("[2-5][0-9]{2}")) {
                            throw new IOException("Invalid response status");
                        }
                        status = Integer.parseInt(token);
                    }
                    case "body" -> {
                        body = string(reader);
                        if (body.getBytes(StandardCharsets.UTF_8).length > ProviderHttp.MAX_BODY_BYTES) {
                            throw new IOException("Response body exceeds limit");
                        }
                    }
                    case "headers" -> {
                        reader.beginObject();
                        while (reader.hasNext()) {
                            String key = reader.nextName();
                            if (key.length() > 128 || !key.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]+")
                                    || headers.size() >= 32) {
                                throw new IOException("Invalid response header");
                            }
                            key = key.toLowerCase(Locale.ROOT);
                            if (headers.containsKey(key)) {
                                throw new IOException("Duplicate response header");
                            }
                            String value = string(reader);
                            if (value.length() > 1024 || value.chars().anyMatch(Character::isISOControl)) {
                                throw new IOException("Invalid response header value");
                            }
                            headers.put(key, List.of(value));
                        }
                        reader.endObject();
                    }
                    default -> reader.skipValue();
                }
            }
            reader.endObject();
            if (!fields.containsAll(Set.of("kind", "id", "status", "headers", "body"))
                    || reader.peek() != JsonToken.END_DOCUMENT) {
                throw new IOException("Incomplete response envelope");
            }
            return new Parsed(id, new ProviderReply(status, headers, body));
        } catch (IllegalStateException | IllegalArgumentException invalid) {
            throw new IOException("Invalid provider response", invalid);
        }
    }

    private static String string(JsonReader reader) throws IOException {
        if (reader.peek() != JsonToken.STRING) {
            throw new IOException("Expected response string");
        }
        return reader.nextString();
    }

    /** Sends a literal ping every interval and drops a socket whose previous ping is still unanswered. */
    private void keepAlive() {
        Connection connection = current;
        if (closed || connection == null || !connection.socket.opened().toCompletableFuture().isDone()) {
            return;
        }
        if (connection.pingOutstanding) {
            discard(connection, true);
            return;
        }
        connection.pingOutstanding = true;
        connection.socket.sendText("ping").whenCompleteAsync((ignored, failure) -> {
            if (failure != null) {
                lost(connection);
            }
        }, owner);
    }

    private void lost(Connection connection) {
        if (current == connection) {
            discard(connection, true);
        }
    }

    private void discard(Connection connection, boolean backoff) {
        discard(connection, backoff, () -> connection.socket.abort());
    }

    /** @param end How the socket ends: an abort, or a close frame for a violation */
    private void discard(Connection connection, boolean backoff, Runnable end) {
        if (current != connection) {
            return;
        }
        current = null; // Fences callbacks before the socket's end runs completion handlers
        if (backoff) {
            backoff();
        }
        if (connection.pending != null) {
            connection.pending.completeExceptionally(new IOException("Provider WebSocket disconnected"));
        }
        if (connection.socket != null) {
            end.run();
        }
        if (backoff && !closed) {
            lostWhileAssisting.run();
        }
    }

    private void backoff() {
        int previous = failures++;
        long delay = Math.min(30000, 500L << Math.min(previous, 6));
        retryAt = clock.nanos() + TimeUnit.MILLISECONDS.toNanos(delay + ThreadLocalRandom.current().nextLong(delay));
    }

    @Override
    public void close() {
        closed = true;
        keepAlive.cancel(false);
        Connection connection = current;
        current = null;
        if (connection != null) {
            if (connection.pending != null) {
                connection.pending.completeExceptionally(new IOException("WebSocket carrier is closed"));
            }
            connection.socket.closeGracefully();
        }
    }
}

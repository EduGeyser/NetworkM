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
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.CandidateMethod;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ProviderTransport;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A host's registration with one NXS signaling provider: it registers or recovers the saved registration, keeps the
 * host profile and admission keys published through signed heartbeats, reports outcomes, and serves assisted joins
 * when configured. One asynchronous, serialized lifecycle per host, never one poller per player.
 *
 * <p>Every operation runs on the client's own lifecycle thread, one at a time, and never blocks it: HTTP, WebSocket
 * and transport calls are asynchronous, retries are scheduled. The provider can stop routing players here but never
 * commands the listener; {@link #drain()} and {@link #stop()} are local decisions.
 */
public final class ProviderClient implements AutoCloseable {
    /** Which carrier signed operations use. */
    public enum ControlTransport {
        /** HTTPS only. */
        HTTP,
        /** The provider's WebSocket when its discovery advertises one, with HTTPS as fallback. */
        AUTO
    }

    /** Which carrier the last successful signed operation used. */
    public enum ControlCarrier {
        NONE,
        HTTP,
        WEBSOCKET
    }

    /**
     * What the client registers and runs with. The bearer token alone decides what the provider registers it as.
     */
    public static final class Configuration {
        private final URI provider;
        private final String label;
        private final String authorizationToken;
        private final ControlTransport controlTransport;
        private final boolean diagnosticAdmission;
        private final CandidateMethod connectivityMethod;
        private final boolean assistedJoins;
        private final boolean deregisterOnShutdown;
        private final boolean gameOutcomes;
        private final Location location;
        private final BedrockProtocols bedrockProtocols;

        private Configuration(Builder builder) {
            provider = Objects.requireNonNull(builder.provider);
            label = builder.label;
            authorizationToken = builder.authorizationToken;
            controlTransport = builder.controlTransport;
            diagnosticAdmission = builder.diagnosticAdmission;
            connectivityMethod = builder.connectivityMethod;
            assistedJoins = builder.assistedJoins;
            deregisterOnShutdown = builder.deregisterOnShutdown;
            gameOutcomes = builder.gameOutcomes;
            location = builder.location;
            bedrockProtocols = builder.bedrockProtocols;
            if (assistedJoins && controlTransport != ControlTransport.AUTO) {
                throw new IllegalArgumentException("Assisted joins require WebSocket control");
            }
            if (connectivityMethod != CandidateMethod.DEFINED && connectivityMethod != CandidateMethod.DISCOVERED) {
                throw new IllegalArgumentException("Hosts find candidates as defined or discovered");
            }
            if (diagnosticAdmission && !"https".equals(provider.getScheme())) {
                throw new IllegalArgumentException("Diagnostics require HTTPS");
            }
            ProviderOrigin.canonicalize(provider);
            if (label != null && (label.codePointCount(0, label.length()) > 128
                    || label.chars().anyMatch(Character::isISOControl))) {
                throw new IllegalArgumentException("A provider label has at most 128 characters and no control characters");
            }
            if (authorizationToken != null && authorizationToken.isBlank()) {
                throw new IllegalArgumentException("Blank bearer token");
            }
        }

        public static Builder builder(URI provider) {
            return new Builder(provider);
        }

        public URI provider() {
            return provider;
        }

        /** The instance's initial name at the provider, or null to send none. */
        public String label() {
            return label;
        }

        public String authorizationToken() {
            return authorizationToken;
        }

        public ControlTransport controlTransport() {
            return controlTransport;
        }

        public boolean diagnosticAdmission() {
            return diagnosticAdmission;
        }

        public CandidateMethod connectivityMethod() {
            return connectivityMethod;
        }

        public boolean assistedJoins() {
            return assistedJoins;
        }

        public boolean deregisterOnShutdown() {
            return deregisterOnShutdown;
        }

        public boolean gameOutcomes() {
            return gameOutcomes;
        }

        public Location location() {
            return location;
        }

        public BedrockProtocols bedrockProtocols() {
            return bedrockProtocols;
        }

        String authorizationScheme() {
            return authorizationToken == null ? "anonymous-proof-of-work" : "bearer-token";
        }

        @Override
        public String toString() {
            return "Configuration[provider=" + provider + ", authorizationScheme=" + authorizationScheme() + "]";
        }

        public static final class Builder {
            private final URI provider;
            private String label;
            private String authorizationToken;
            private ControlTransport controlTransport = ControlTransport.HTTP;
            private boolean diagnosticAdmission;
            private CandidateMethod connectivityMethod = CandidateMethod.DISCOVERED;
            private boolean assistedJoins;
            private boolean deregisterOnShutdown;
            private boolean gameOutcomes;
            private Location location;
            private BedrockProtocols bedrockProtocols;

            private Builder(URI provider) {
                this.provider = provider;
            }

            /**
             * @param label The instance's initial name at the provider, at most 128 characters, or null to send none.
             *              No host name is sent unless the host passes one here.
             */
            public Builder label(String label) {
                this.label = label;
                return this;
            }

            /** @param authorizationToken A bearer token, or null to register anonymously with proof of work */
            public Builder authorizationToken(String authorizationToken) {
                this.authorizationToken = authorizationToken;
                return this;
            }

            public Builder controlTransport(ControlTransport controlTransport) {
                this.controlTransport = Objects.requireNonNull(controlTransport);
                return this;
            }

            /** Lets the provider check reachability from its regions, which the transport must support. */
            public Builder diagnosticAdmission(boolean diagnosticAdmission) {
                this.diagnosticAdmission = diagnosticAdmission;
                return this;
            }

            /** @param connectivityMethod {@link CandidateMethod#DEFINED} for configured endpoints, otherwise discovered */
            public Builder connectivityMethod(CandidateMethod connectivityMethod) {
                this.connectivityMethod = Objects.requireNonNull(connectivityMethod);
                return this;
            }

            /** Lets the provider push joins over its WebSocket; requires {@link ControlTransport#AUTO}. */
            public Builder assistedJoins(boolean assistedJoins) {
                this.assistedJoins = assistedJoins;
                return this;
            }

            /** For hosts whose state directory does not outlive the process, which could never recover. */
            public Builder deregisterOnShutdown(boolean deregisterOnShutdown) {
                this.deregisterOnShutdown = deregisterOnShutdown;
                return this;
            }

            /**
             * Declares that the application reports every admitted player's join or rejection through
             * {@link ProviderClient#reportGameOutcome}.
             */
            public Builder gameOutcomes(boolean gameOutcomes) {
                this.gameOutcomes = gameOutcomes;
                return this;
            }

            /** @param location Where the operator says the host runs, or null to let the provider estimate it */
            public Builder location(Location location) {
                this.location = location;
                return this;
            }

            /**
             * @param bedrockProtocols The protocols the host accepts, sent with each host profile, or null to never send
             *                         them, which leaves any value an earlier run reported in place
             */
            public Builder bedrockProtocols(BedrockProtocols bedrockProtocols) {
                this.bedrockProtocols = bedrockProtocols;
                return this;
            }

            public Configuration build() {
                return new Configuration(this);
            }
        }
    }

    /** The assigned runtime identity. {@code serviceId} and {@code publicAddress} are both present or both null. */
    public record Registration(String registrationId, String instanceId, String keyId, long leaseGeneration,
                               String serviceId, String publicAddress) {
    }

    /** The game's verdict on an admitted player. */
    public enum GameOutcome {
        JOINED(OutcomeStage.GAME_JOINED),
        REJECTED(OutcomeStage.GAME_REJECTED);

        private final OutcomeStage stage;

        GameOutcome(OutcomeStage stage) {
            this.stage = stage;
        }

        OutcomeStage stage() {
            return stage;
        }
    }

    /** Sampled actual players on this runtime; existing players keep counting while no new ones are accepted. */
    public record ConnectedPlayers(int count, long sampledAt) {
        public ConnectedPlayers {
            if (count < 0 || count > 1000000 || sampledAt < 0 || sampledAt > 9007199254740991L) {
                throw new IllegalArgumentException("Invalid player count sample");
            }
        }
    }

    /** The host software and its version, as the provider should show them. */
    public record Software(String name, String version) {
        public Software {
            if (name == null || name.isBlank() || name.codePointCount(0, name.length()) > 64 || version == null
                    || version.isBlank() || version.codePointCount(0, version.length()) > 128) {
                throw new IllegalArgumentException("Invalid host software");
            }
        }
    }

    /** Where the operator says the host runs: a country, coordinates or both. Coordinates come together. */
    public record Location(String country, String city, Double latitude, Double longitude) {
        public Location {
            if (country != null && !country.matches("[A-Z]{2}") || city != null && (city.isEmpty()
                    || city.codePointCount(0, city.length()) > 120 || city.chars().anyMatch(Character::isISOControl))
                    || (latitude == null) != (longitude == null)
                    || latitude != null && !(Math.abs(latitude) <= 90 && Math.abs(longitude) <= 180)
                    || country == null && latitude == null) {
                throw new IllegalArgumentException("Invalid location");
            }
        }

        /**
         * Reads the host configuration keys {@code country}, {@code city}, {@code latitude} and {@code longitude}.
         *
         * @return null when nothing is declared
         */
        public static Location of(Map<String, String> settings) {
            if (settings.isEmpty()) {
                return null;
            }
            if (!Set.of("country", "city", "latitude", "longitude").containsAll(settings.keySet())) {
                throw new IllegalArgumentException("Unknown location key");
            }
            String country = settings.get("country"), city = settings.get("city"),
                    latitude = settings.get("latitude"), longitude = settings.get("longitude");
            return new Location(country == null ? null : country.trim().toUpperCase(Locale.ROOT),
                    city == null ? null : city.trim(), latitude == null ? null : Double.valueOf(latitude.trim()),
                    longitude == null ? null : Double.valueOf(longitude.trim()));
        }
    }

    /**
     * The Bedrock protocols the host accepts at login, which a provider may advertise from.
     *
     * @param accepted Every accepted protocol in strictly ascending order, 1 to 512 of them. Not a range, since
     *                 protocol numbers have gaps
     * @param version  The game version of the highest accepted protocol
     */
    public record BedrockProtocols(List<Integer> accepted, String version) {
        public BedrockProtocols {
            Integer[] protocols = accepted == null ? new Integer[0] : accepted.toArray(Integer[]::new);
            if (protocols.length == 0 || protocols.length > 512 || version == null || version.isEmpty()
                    || version.codePointCount(0, version.length()) > 64 || version.codePoints().anyMatch(
                    c -> Character.getType(c) == Character.CONTROL || Character.getType(c) == Character.SURROGATE)) {
                throw new IllegalArgumentException("Invalid Bedrock protocols");
            }
            int previous = 0;
            for (Integer protocol : protocols) {
                if (protocol == null || protocol <= previous) {
                    throw new IllegalArgumentException("Accepted Bedrock protocols must ascend from 1");
                }
                previous = protocol;
            }
            accepted = List.of(protocols);
        }
    }

    /**
     * A link the provider offers the host's operator. It may carry a one-time credential: show it, never open or
     * save it.
     */
    public record OperatorAction(URI url, String text, long expiresAt) {
        @Override
        public String toString() {
            return "OperatorAction[expiresAt=" + expiresAt + "]";
        }
    }

    /**
     * What the provider last acknowledged to hand joining players.
     *
     * @param endpoints        The endpoints players are offered
     * @param assistedFamilies The families the provider assists joins for, empty when it assists none
     */
    public record PublishedEndpoints(List<InetSocketAddress> endpoints, Set<AddressFamily> assistedFamilies) {
        public PublishedEndpoints {
            endpoints = List.copyOf(endpoints);
            assistedFamilies = Set.copyOf(assistedFamilies);
        }
    }

    /** Capacity and connectedPlayers describe the same observation. */
    public record Health(boolean acceptingPlayers, int capacity, Software software, ConnectedPlayers connectedPlayers) {
        public Health {
            if (capacity < 0 || capacity > 1000000 || connectedPlayers == null) {
                throw new IllegalArgumentException("Invalid health");
            }
        }
    }

    /**
     * Whether the provider routes players here, as its last heartbeat reply observed. Informational: the host acts
     * on none of it.
     *
     * @param reasons Provider-defined lowercase codes for why the host is not routable
     */
    public record Readiness(boolean routable, List<String> reasons) {
        public Readiness {
            reasons = List.copyOf(reasons);
        }
    }

    /**
     * Optional provider or application metadata under a reverse-DNS namespace.
     *
     * @param data A JSON object, as text; its meaning belongs to the namespace
     */
    public record ProtocolExtension(int version, boolean critical, String data) {
        public ProtocolExtension {
            Objects.requireNonNull(data, "data");
            if (version < 1) {
                throw new IllegalArgumentException("Extension versions start at 1");
            }
        }
    }

    /** A provider refusal, with its words for the operator. */
    public static final class ProviderException extends IOException {
        private final int status;
        private final String code;
        private final String providerMessage;
        private final URI actionUrl;
        private final Duration retryAfter;

        ProviderException(int status, String code, String providerMessage, URI actionUrl, Duration retryAfter) {
            super(describe(status, code, providerMessage, actionUrl));
            this.status = status;
            this.code = code;
            this.providerMessage = providerMessage;
            this.actionUrl = actionUrl;
            this.retryAfter = retryAfter;
        }

        /** Such as "Add a token. (403 capacity_reached) https://provider.example/tokens" */
        private static String describe(int status, String code, String providerMessage, URI actionUrl) {
            String description = providerMessage == null
                    ? "Provider request failed: " + status + " " + code
                    : providerMessage + " (" + status + " " + code + ")";
            return actionUrl == null ? description : description + " " + actionUrl;
        }

        public int status() {
            return status;
        }

        /** The provider's machine code; see the wire reference for the codes clients act on. */
        public String code() {
            return code;
        }

        /** The provider's explanation for the host's operator, to show as its words and never parse. */
        public Optional<String> providerMessage() {
            return Optional.ofNullable(providerMessage);
        }

        /** A page where the operator can act. It carries no credential, so it may be logged, but is never opened. */
        public Optional<URI> actionUrl() {
            return Optional.ofNullable(actionUrl);
        }

        /** How long the provider asked the client to wait before repeating the request, at most a day. */
        public Optional<Duration> retryAfter() {
            return Optional.ofNullable(retryAfter);
        }
    }

    /**
     * Where the client runs and how it reaches the network. A production client owns all of it; tests substitute a
     * manual clock and executor.
     */
    record Environment(ProviderClock clock, ScheduledExecutorService executor, boolean ownsExecutor,
                       HttpClient http, Supplier<EventLoopGroup> eventLoops, SslContext tls) {
        static Environment system() {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "nethernet-provider");
                thread.setDaemon(true);
                return thread;
            });
            try {
                return new Environment(ProviderClock.SYSTEM, executor, true, ProviderHttp.defaultClient(),
                        () -> new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()),
                        SslContextBuilder.forClient().endpointIdentificationAlgorithm("HTTPS").build());
            } catch (SSLException unavailable) {
                executor.shutdown();
                throw new IllegalStateException("No TLS client context", unavailable);
            }
        }
    }

    private final ClientRuntime runtime;
    private final Environment environment;
    private final OperationQueue queue;
    private final Registrar registrar;
    private final KeyManager keys;
    private final OutcomeQueue outcomes;
    private final Heartbeats heartbeats;
    private final ControlChannel control;
    private final AtomicBoolean refreshQueued = new AtomicBoolean();
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private ScheduledFuture<?> timer;
    private boolean tickQueued;
    /** Ends a start that is waiting out a refusal, failing it with that refusal. */
    private Runnable abandonStart;

    /**
     * @param statusSupplier The public listing, or null when the host lists nothing; asked on the client's thread
     * @param healthSupplier Acceptance, capacity and the actual player count; asked on the client's thread
     * @param diagnostics    Receives operator messages; route its levels to the matching logger methods
     */
    public ProviderClient(Configuration config, ProviderStateStore store, ProviderTransport transport,
                          Supplier<ServerStatus> statusSupplier, Supplier<Health> healthSupplier,
                          Consumer<ProviderDiagnostic> diagnostics) {
        this(config, store, transport, statusSupplier, healthSupplier, diagnostics, Environment.system());
    }

    ProviderClient(Configuration config, ProviderStateStore store, ProviderTransport transport,
                   Supplier<ServerStatus> statusSupplier, Supplier<Health> healthSupplier,
                   Consumer<ProviderDiagnostic> diagnostics, Environment environment) {
        this.environment = environment;
        ProviderLog log = new ProviderLog(diagnostics, environment.clock()::nanos);
        runtime = new ClientRuntime(config, environment.clock(), environment.executor(), log, transport, store);
        runtime.stop = this::stop;
        queue = new OperationQueue(environment.executor());
        RequestSigner signer = new RequestSigner(runtime);
        control = new ControlChannel(runtime, signer, environment);
        Exchanger exchanger = new Exchanger(runtime, signer, new ProviderHttp(environment.http()), control::socket,
                control::upgradeHeaders, new Exchanger.Relinquish() {
            @Override
            public void fenced() {
                relinquish(log::replaced);
            }

            @Override
            public void gone(ProviderException refusal) {
                relinquish(() -> log.registrationGone(refusal.getMessage()));
            }
        });
        Diagnostics checks = new Diagnostics(runtime);
        keys = new KeyManager(runtime, checks);
        checks.keys(keys);
        outcomes = new OutcomeQueue(runtime, exchanger);
        OperatorActions actions = new OperatorActions();
        registrar = new Registrar(runtime, exchanger, signer, keys, outcomes, control, actions);
        Assistance assistance = new Assistance(runtime, control);
        control.assistance(assistance);
        heartbeats = new Heartbeats(runtime, exchanger, keys, checks, assistance, statusSupplier, healthSupplier,
                new ConnectivityLog(diagnostics, config.assistedJoins(), config.diagnosticAdmission(),
                        environment.clock()::millis),
                actions, () -> registrar.recoverAmbiguous().thenCompose(ignored -> keys.install()));
    }

    /** The carrier of the last successful signed operation, independent of the configured one. */
    public ControlCarrier lastControlCarrier() {
        return runtime.lastCarrier();
    }

    /**
     * How far this machine's clock is from the provider's, as its replies tell it: positive when this machine is
     * behind. Zero until a reply says otherwise, or while the difference is within tolerance. Requests stay signed
     * with this machine's clock, so one too far off has them refused; the difference is measured and warned about,
     * and corrects only the deadlines of assisted joins, which the provider sets on its own clock.
     */
    public long clockOffsetMillis() {
        return runtime.clockOffsetMillis();
    }

    /**
     * The endpoints the provider last acknowledged to hand joining players. Safe from any thread; never waits.
     *
     * @return Empty before the first acknowledgement, while a changed profile is being published again, and once
     * stopping
     */
    public Optional<PublishedEndpoints> publishedEndpoints() {
        return heartbeats.published();
    }

    /**
     * Registers, or recovers the saved registration, and starts checking in. When the provider refuses a
     * registration only for now, with {@code rate_limited} or {@code capacity_reached} and a {@code Retry-After}, it
     * warns and tries again once that has passed. Stopping ends the wait, failing the returned future with that
     * refusal.
     */
    public CompletableFuture<Registration> start() {
        CompletableFuture<Registration> registered = new CompletableFuture<>();
        runtime.executor.execute(() -> attemptStart(registered));
        return registered;
    }

    private void attemptStart(CompletableFuture<Registration> registered) {
        abandonStart = null;
        queue.submit(this::startOnce).whenCompleteAsync((registration, failure) -> {
            if (failure == null) {
                registered.complete(registration);
                return;
            }
            Optional<Duration> wait = runtime.closing ? Optional.empty() : ProviderFailures.registrationRetry(failure);
            if (wait.isEmpty()) {
                registered.completeExceptionally(failure);
                return;
            }
            runtime.log.registrationDeferred(ProviderFailures.describe(failure), wait.get());
            ScheduledFuture<?> retry = runtime.executor.schedule(() -> attemptStart(registered), wait.get().toNanos(),
                    TimeUnit.NANOSECONDS);
            abandonStart = () -> {
                retry.cancel(false);
                registered.completeExceptionally(failure);
            };
        }, runtime.executor);
    }

    /** One attempt: read the provider's discovery, recover or register, then start checking in. */
    private CompletionStage<Registration> startOnce() {
        if (runtime.closed) {
            return CompletableFuture.failedFuture(new IOException("Provider is closed"));
        }
        if (runtime.started) {
            return CompletableFuture.failedFuture(new IllegalStateException("Already started"));
        }
        return registrar.register()
                .thenCompose(ignored -> Async.attempt(() -> {
                    control.configure();
                    return keys.install();
                }))
                .thenCompose(ignored -> {
                    runtime.started = true;
                    return heartbeats.beat();
                })
                .thenApply(ignored -> {
                    if (timer != null) {
                        timer.cancel(false);
                    }
                    timer = runtime.executor.scheduleWithFixedDelay(this::tick, 1, 1, TimeUnit.SECONDS);
                    return registrar.view();
                });
    }

    /** Looks every second for a heartbeat or an outcome batch that is due. */
    private void tick() {
        if (runtime.closed || !runtime.started || tickQueued) {
            return;
        }
        tickQueued = true;
        queue.submit(() -> heartbeats.due()
                        .thenCompose(due -> due ? heartbeats.beat() : Async.done())
                        .exceptionally(failure -> {
                            heartbeats.failed(failure);
                            return null;
                        })
                        // A heartbeat can stop the client, for example when its registration is gone
                        .thenCompose(ignored -> runtime.started && outcomes.due()
                                ? outcomes.flush(false).thenApply(more -> (Void) null)
                                : Async.done())
                        .exceptionally(failure -> {
                            outcomes.failed(failure);
                            return null;
                        }))
                .whenCompleteAsync((ignored, failure) -> tickQueued = false, runtime.executor);
    }

    /** Wakes local observation after a join, a leave or a reload; an unchanged snapshot sends nothing. */
    public void requestStatusRefresh() {
        if (!runtime.closing && refreshQueued.compareAndSet(false, true)) {
            queue.submit(() -> {
                refreshQueued.set(false);
                if (runtime.closed || !runtime.started || !heartbeats.statusChanged()) {
                    return Async.done();
                }
                return heartbeats.beat().exceptionally(failure -> {
                    heartbeats.failed(failure);
                    return null;
                });
            });
        }
    }

    /** Sends a heartbeat and returns the provider's readiness observation from its reply. */
    public CompletableFuture<Readiness> readiness() {
        return submit(() -> heartbeats.beat().thenApply(ignored -> heartbeats.lastReadiness()));
    }

    /**
     * Replaces the application's optional observations and sends them promptly while running. Omitting an extension
     * does not tell the provider to clear an earlier observation.
     */
    public CompletableFuture<Void> updateHeartbeatExtensions(Map<String, ProtocolExtension> extensions) {
        Map<String, ProtocolExtension> copy = Map.copyOf(extensions);
        JsonObject document = new JsonObject();
        document.add("extensions", WireCodec.extensionsJson(copy));
        ProtocolExtensions.validate(document);
        if (copy.containsKey(Wire.CONNECTIVITY)) {
            throw new IllegalArgumentException("The connectivity extension is owned by this client");
        }
        return submit(() -> {
            heartbeats.extensions(copy);
            return runtime.started ? heartbeats.beat() : Async.done();
        });
    }

    /**
     * Queues a game outcome for an admitted ticket without blocking. Requires
     * {@link Configuration.Builder#gameOutcomes}.
     *
     * @param ticketId The ticket ID from the admission's principal
     * @param reason   A short lowercase code, or null; never player identity or disconnect text
     * @return false when the client is stopping or the queue is full, so the outcome was dropped
     * @throws IllegalArgumentException If the reason is not a short lowercase code, so the provider would refuse it
     */
    public boolean reportGameOutcome(TicketId ticketId, GameOutcome outcome, String reason) {
        if (!runtime.config.gameOutcomes()) {
            throw new IllegalStateException("Game outcomes are not enabled");
        }
        Objects.requireNonNull(ticketId, "ticketId");
        Objects.requireNonNull(outcome, "outcome");
        if (reason != null && !TransportOutcome.REASON.matcher(reason).matches()) {
            throw new IllegalArgumentException("An outcome reason is a short lowercase code");
        }
        if (runtime.closing) {
            return false;
        }
        return outcomes.offer(new Wire.OutcomeEvent(ticketId, outcome.stage(), runtime.clock.millis(), reason, null));
    }

    /**
     * The provider's current link for the host's operator, requesting a fresh one when a heartbeat said it is
     * offered.
     */
    public CompletableFuture<Optional<OperatorAction>> operatorAction() {
        return submit(heartbeats::operatorAction);
    }

    /** The registration's opaque extensions; the application decides what they mean. */
    public CompletableFuture<Map<String, ProtocolExtension>> extensions() {
        return submit(() -> CompletableFuture.completedFuture(registrar.extensions()));
    }

    /**
     * Calls an operation an extension advertised, only on the application's explicit request.
     *
     * @param body A JSON object as text, or null for none
     * @return The provider's reply, a JSON object as text
     */
    public CompletableFuture<String> extensionRequest(String namespace, String operation, String method, String body) {
        if (!Set.of("GET", "POST").contains(method)) {
            return CompletableFuture.failedFuture(new IOException("Unsupported extension method"));
        }
        return submit(() -> registrar.extensionRequest(namespace, operation, method, body));
    }

    /** Ends this instance's registration for good and drains the listener. */
    public CompletableFuture<Void> deregister() {
        return submit(() -> heartbeats.disableDiagnostics()
                .thenCompose(ignored -> registrar.deregister())
                .thenCompose(ignored -> {
                    heartbeats.forgetPublished();
                    control.close();
                    return Async.within(runtime.transport.drain(), Duration.ofSeconds(10), runtime.executor,
                            "Transport drain");
                }));
    }

    /** Asks the provider for a replacement admission key epoch and publishes it. */
    public CompletableFuture<Registration> rotateAdmissionKey() {
        return submit(() -> keys.requestReplacement()
                .thenCompose(ignored -> heartbeats.beat())
                .thenApply(ignored -> registrar.view()));
    }

    /** Replaces the machine signing key: the replacement is saved, proven, confirmed, then the old key retired. */
    public CompletableFuture<Registration> rotateMachineKey() {
        return submit(registrar::rotateMachineKey);
    }

    /** Stops admitting new players while still counting existing ones, and sends waiting outcomes at once. */
    public CompletableFuture<Void> drain() {
        return submit(this::drainAndReport);
    }

    private CompletableFuture<Void> drainAndReport() {
        heartbeats.forgetPublished();
        return heartbeats.disableDiagnostics()
                .thenCompose(ignored -> Async.within(runtime.transport.drain(), Duration.ofSeconds(10),
                        runtime.executor, "Transport drain"))
                .thenCompose(ignored -> {
                    runtime.draining = true;
                    return heartbeats.beat();
                })
                // The host may stop next, so outcomes do not wait for their batch
                .thenCompose(ignored -> outcomes.flushAll().exceptionally(failure -> {
                    runtime.log.failed(ProviderLog.Operation.EVENTS, ProviderFailures.describe(failure));
                    return null;
                }));
    }

    /** Another process owns the registration now, or the provider no longer has it: stop without draining. */
    private void relinquish(Runnable announce) {
        if (!runtime.fenced) {
            runtime.fenced = true;
            runtime.started = false;
            announce.run();
            stop();
        }
    }

    /**
     * Drains, tells the provider the host stopped accepting players, sends waiting outcomes, deregisters when
     * configured to, then closes the transport and releases the state directory.
     */
    public CompletionStage<Void> stop() {
        synchronized (stopped) {
            if (runtime.closing) {
                return stopped.minimalCompletionStage();
            }
            runtime.closing = true;
        }
        queue.submit(() -> {
            if (!runtime.started) {
                return Async.done();
            }
            CompletableFuture<Void> drained = drainAndReport();
            return runtime.config.deregisterOnShutdown()
                    ? drained.thenCompose(ignored -> registrar.deregister().exceptionally(failure -> {
                runtime.log.failed(ProviderLog.Operation.DEREGISTER, ProviderFailures.describe(failure));
                return null;
            }))
                    : drained;
        }).whenCompleteAsync((ignored, failure) -> {
            if (failure != null) {
                runtime.log.failed(ProviderLog.Operation.DRAIN, ProviderFailures.describe(failure));
            }
            finish();
        }, runtime.executor);
        return stopped.minimalCompletionStage();
    }

    private void finish() {
        runtime.closed = true;
        runtime.started = false;
        heartbeats.forgetPublished();
        control.close();
        if (timer != null) {
            timer.cancel(false);
        }
        Async.within(runtime.transport.close(), Duration.ofSeconds(10), runtime.executor, "Transport close")
                .whenCompleteAsync((ignored, failure) -> {
                    if (failure != null) {
                        runtime.log.failed(ProviderLog.Operation.TRANSPORT_CLOSE, ProviderFailures.describe(failure));
                    }
                    try {
                        runtime.keeper.close();
                    } catch (Exception closing) {
                        runtime.log.failed(ProviderLog.Operation.STATE_CLOSE, ProviderFailures.describe(closing));
                    }
                    if (abandonStart != null) {
                        abandonStart.run();
                        abandonStart = null;
                    }
                    control.shutdown();
                    stopped.complete(null);
                    if (environment.ownsExecutor()) {
                        runtime.executor.shutdown();
                    }
                }, runtime.executor);
    }

    @Override
    public void close() {
        stop();
    }

    /** Whether no operation runs or waits, for tests that run the client's schedule on a manual executor. */
    boolean idle() {
        return queue.idle() && !tickQueued;
    }

    private <T> CompletableFuture<T> submit(Async.Step<T> work) {
        return queue.submit(() -> runtime.closed
                ? CompletableFuture.failedFuture(new IOException("Provider is closed"))
                : Async.attempt(work));
    }
}

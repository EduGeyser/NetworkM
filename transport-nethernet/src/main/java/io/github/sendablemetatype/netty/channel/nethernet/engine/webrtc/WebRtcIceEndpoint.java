package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.webrtc.IceUdpEndpoint;
import io.github.sendablemetatype.webrtc.IceUdpEndpoint.PendingRequest;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An {@link IceEndpoint} on webrtc-java's {@link IceUdpEndpoint}. Every native
 * call waits for the endpoint's own thread, so each runs on the engine's
 * control thread: settings in call order, queries as stages.
 */
final class WebRtcIceEndpoint implements IceEndpoint {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(WebRtcIceEndpoint.class);
    /** How long a failed bind on one address stays at debug after it logged a warning. */
    private static final Duration BIND_FAILURE_QUIET = Duration.ofMinutes(10);

    private final WebRtcEngine engine;
    private final IceUdpEndpoint endpoint;
    private final int port;
    private final Executor control;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    // Control thread only.
    private final BindFailureLimiter bindFailureLimiter = new BindFailureLimiter(BIND_FAILURE_QUIET);

    WebRtcIceEndpoint(WebRtcEngine engine, IceUdpEndpoint endpoint, int port, Executor control) {
        this.engine = engine;
        this.endpoint = endpoint;
        this.port = port;
        this.control = control;
    }

    /** The native endpoint, for peers of the engine that opened it. */
    IceUdpEndpoint nativeEndpoint(WebRtcEngine owner) {
        if (owner != engine) {
            throw new IllegalArgumentException("The ICE endpoint belongs to another engine");
        }
        return endpoint;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public void onHeldCheck(Consumer<HeldCheck> handler, Executor executor) {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(executor, "executor");
        apply("setting the held check handler",
                () -> endpoint.setPendingRequestHandler(request -> handler.accept(new Held(request)), executor));
    }

    @Override
    public void clearHeldCheckHandler() {
        apply("clearing the held check handler", endpoint::clearPendingRequestHandler);
    }

    @Override
    public void heldCheckLimits(int maxPending, Duration timeout) {
        if (maxPending < 1) {
            throw new IllegalArgumentException("maxPending must be at least 1: " + maxPending);
        }
        if (timeout.toMillis() < 1) {
            throw new IllegalArgumentException("timeout must be at least 1 ms: " + timeout);
        }
        apply("setting held check limits", () -> endpoint.setPendingLimits(maxPending, timeout));
    }

    @Override
    public void derivedPasswords(byte[] context, Map<String, byte[]> keys) {
        byte[] contextCopy = context.clone();
        Map<String, byte[]> keyCopies = new LinkedHashMap<>();
        keys.forEach((prefix, key) -> {
            if (prefix.isEmpty()) {
                throw new IllegalArgumentException("Empty ufrag prefix");
            }
            keyCopies.put(prefix, key.clone());
        });
        apply("setting derived passwords", () -> {
            try {
                endpoint.setDerivedPasswords(contextCopy, keyCopies);
            } finally {
                Arrays.fill(contextCopy, (byte) 0);
                keyCopies.values().forEach(key -> Arrays.fill(key, (byte) 0));
            }
        });
    }

    @Override
    public void clearDerivedPasswords() {
        apply("clearing derived passwords", endpoint::clearDerivedPasswords);
    }

    @Override
    public void proxyRelays(List<String> relays) {
        String[] entries = relays.toArray(String[]::new);
        for (String entry : entries) {
            requireRelay(entry);
        }
        apply("setting PROXY protocol relays", () -> endpoint.setProxyRelays(entries));
    }

    /** Checks an entry the way webrtc-java parses it: an IP literal or a CIDR range, never a name. */
    static void requireRelay(String entry) {
        Objects.requireNonNull(entry, "relay");
        int slash = entry.indexOf('/');
        try {
            InetAddress address = InetAddress.ofLiteral(slash < 0 ? entry : entry.substring(0, slash));
            if (slash >= 0) {
                int prefix = Integer.parseInt(entry.substring(slash + 1));
                if (prefix < 0 || prefix > address.getAddress().length * 8) {
                    throw new IllegalArgumentException("Prefix out of range");
                }
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Not a numeric address or CIDR range: " + entry, e);
        }
    }

    @Override
    public CompletionStage<Optional<InetSocketAddress>> relayOf(InetSocketAddress client) {
        Objects.requireNonNull(client, "client");
        return query(() -> Optional.ofNullable(endpoint.getProxyRelay(client)));
    }

    @Override
    public CompletionStage<StunMonitor> monitorStun(InetSocketAddress server) {
        Objects.requireNonNull(server, "server");
        return query(() -> new Monitor(endpoint.monitorStun(server)));
    }

    @Override
    public CompletionStage<Stats> stats() {
        return query(() -> {
            IceUdpEndpoint.Stats stats = endpoint.getStats();
            return new Stats(stats.datagramsReceived(), stats.bytesReceived(), stats.datagramsSent(),
                    stats.bytesSent(), stats.unroutable(), stats.unauthenticated(), stats.pendingDropped(),
                    stats.announced(), stats.replayed(), stats.expired(), stats.rejected(), stats.relayDropped(),
                    stats.relayFiltered(), stats.routes(), stats.sessions(), stats.pending());
        });
    }

    @Override
    public CompletionStage<Sockets> sockets() {
        return query(() -> {
            IceUdpEndpoint.Sockets sockets = endpoint.getSockets();
            return new Sockets(sockets.bound(), sockets.failed().stream()
                    .map(failure -> new BindFailure(failure.address(), reason(failure)))
                    .toList());
        });
    }

    /**
     * The system's reason alone. webrtc-java's message also names the address
     * and port, as in {@code Cannot bind 192.0.2.1 port 19132: Address already
     * in use}; a message in another form is kept whole.
     */
    static String reason(IceUdpEndpoint.BindFailure failure) {
        String message = String.valueOf(failure.failure().getMessage());
        InetSocketAddress address = failure.address();
        // The host address names a link-local address's scope, as webrtc-java's message does
        String prefix = "Cannot bind " + address.getAddress().getHostAddress() + " port " + address.getPort() + ": ";
        return message.startsWith(prefix) ? message.substring(prefix.length()) : message;
    }

    /**
     * Logs the port's failed binds from now on. Call it on the control thread
     * before a peer can use the endpoint, so that no failure goes unlogged.
     */
    void logBindFailures() {
        applyHere("setting the bind failure handler", () -> endpoint.setBindFailureHandler(this::bindFailed, control));
    }

    /**
     * On the control thread. webrtc-java reports a failed bind each time a peer
     * gathers, or a STUN monitor starts, on an address where it cannot bind the
     * port, so an address that stays taken would log once per join. Each
     * address logs a warning at most once per {@link #BIND_FAILURE_QUIET}, and
     * the failures in between log at debug. Nothing reports a later successful
     * bind, so a failure that lasts logs a warning again after the quiet time.
     */
    private void bindFailed(IceUdpEndpoint.BindFailure failure) {
        // The host address names a link-local address's scope
        String address = failure.address().getAddress().getHostAddress();
        String reason = reason(failure);
        if (bindFailureLimiter.warn(failure.address(), System.nanoTime())) {
            log.warn("Peers on the shared ICE port gather no host candidate on {} (further failures there log at "
                    + "debug for {} min): {}", address, BIND_FAILURE_QUIET.toMinutes(), reason);
        } else {
            log.debug("Peers on the shared ICE port gather no host candidate on {}: {}", address, reason);
        }
    }

    /**
     * On the control thread: stops the bind failure log, dropping failures
     * still queued for it, and releases the native endpoint.
     */
    void dispose() {
        try {
            endpoint.clearBindFailureHandler();
        } finally {
            endpoint.dispose();
        }
    }

    @Override
    public CompletionStage<Void> close() {
        if (closing.compareAndSet(false, true)) {
            try {
                control.execute(() -> {
                    try {
                        dispose();
                    } catch (RuntimeException | Error e) {
                        if (!NativeErrors.isCallFailure(e)) {
                            throw e;
                        }
                        log.debug("Error closing an ICE endpoint: {}", e.toString());
                    } finally {
                        closed.complete(null);
                        engine.endpointClosed(this);
                    }
                });
            } catch (RejectedExecutionException e) {
                closed.complete(null);
            }
        }
        return closed.minimalCompletionStage();
    }

    private void apply(String what, Runnable setting) {
        try {
            control.execute(() -> applyHere(what, setting));
        } catch (RejectedExecutionException e) {
            log.debug("The WebRTC engine is closed; not {}", what);
        }
    }

    /** On the control thread: a setting whose failed call is logged, not thrown. */
    private void applyHere(String what, Runnable setting) {
        try {
            setting.run();
        } catch (RuntimeException | Error e) {
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            log.warn("Error {} on the ICE endpoint: {}", what, e.toString());
        }
    }

    private <T> CompletionStage<T> query(Supplier<T> query) {
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            control.execute(() -> {
                try {
                    result.complete(query.get());
                } catch (RuntimeException | Error e) {
                    if (!NativeErrors.isCallFailure(e)) {
                        throw e;
                    }
                    result.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(new IllegalStateException("The WebRTC engine is closed", e));
        }
        return result.minimalCompletionStage();
    }

    private static StunReading reading(IceUdpEndpoint.StunReading reading) {
        StunReading.State state = switch (reading.state()) {
            case PENDING -> StunReading.State.PENDING;
            case SUCCEEDED -> StunReading.State.SUCCEEDED;
            case FAILED -> StunReading.State.FAILED;
        };
        return new StunReading(reading.server(), reading.local(), reading.mapped(), state,
                reading.successfulResponses(), reading.failedTransactions(), reading.mappingRevision(),
                reading.lastSuccessAge());
    }

    private final class Monitor implements StunMonitor {
        private final IceUdpEndpoint.StunMonitor monitor;
        private final AtomicBoolean closed = new AtomicBoolean();

        Monitor(IceUdpEndpoint.StunMonitor monitor) {
            this.monitor = monitor;
        }

        @Override
        public CompletionStage<StunReading> read() {
            return query(() -> reading(monitor.read()));
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                apply("closing a STUN monitor", monitor::close);
            }
        }
    }

    /** A held check; rejecting one only posts to the endpoint's thread. */
    private record Held(PendingRequest request) implements HeldCheck {

        @Override
        public InetSocketAddress localAddress() {
            return request.localAddress;
        }

        @Override
        public InetSocketAddress remoteAddress() {
            return request.remoteAddress;
        }

        @Override
        public String localUfrag() {
            return request.localUfrag;
        }

        @Override
        public String remoteUfrag() {
            return request.remoteUfrag;
        }

        @Override
        public void reject() {
            request.reject();
        }
    }
}

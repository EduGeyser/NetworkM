package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EndpointConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.webrtc.IceUdpEndpoint;
import io.github.sendablemetatype.webrtc.PeerConnectionFactory;
import io.github.sendablemetatype.webrtc.PortAllocatorConfig;
import io.github.sendablemetatype.webrtc.RTCBundlePolicy;
import io.github.sendablemetatype.webrtc.RTCCertificatePEM;
import io.github.sendablemetatype.webrtc.RTCConfiguration;
import io.github.sendablemetatype.webrtc.RTCIceServer;
import io.github.sendablemetatype.webrtc.RTCTcpCandidatePolicy;
import io.github.sendablemetatype.webrtc.logging.Logging.Severity;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A {@link NetherNetEngine} on libwebrtc through webrtc-java.
 *
 * <p>The engine keeps a pool of webrtc-java factories. Each factory runs its
 * own network and signaling threads, shared by the peers assigned to it in
 * turn, so a pool spreads DTLS and SCTP work over several threads. Each
 * factory also gets one control thread, on which the engine makes every
 * webrtc-java call that may wait for libwebrtc's threads; callers never wait.
 * Factories and their threads start when the first peer needs them.
 *
 * <p>Whoever builds an engine owns it and closes it. Closing closes every
 * peer and endpoint first and releases the factories after the last peer's
 * close stage completed.
 */
public final class WebRtcEngine implements NetherNetEngine {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(WebRtcEngine.class);
    private static final AtomicInteger ENGINE_IDS = new AtomicInteger();

    /**
     * The port allocator flags NetworkM's channels have used so far: no TCP,
     * IPv6 also on Wi-Fi, any-address ports, one socket per network, no costly
     * or link-local networks. A peer on a shared ICE endpoint gathers without
     * any-address ports, see {@link #allocator}.
     */
    public static final int DEFAULT_PORT_ALLOCATOR_FLAGS = PortAllocatorConfig.PORTALLOCATOR_DISABLE_TCP
            | PortAllocatorConfig.PORTALLOCATOR_ENABLE_IPV6
            | PortAllocatorConfig.PORTALLOCATOR_ENABLE_IPV6_ON_WIFI
            | PortAllocatorConfig.PORTALLOCATOR_ENABLE_ANY_ADDRESS_PORTS
            | PortAllocatorConfig.PORTALLOCATOR_ENABLE_SHARED_SOCKET
            | PortAllocatorConfig.PORTALLOCATOR_DISABLE_COSTLY_NETWORKS
            | PortAllocatorConfig.PORTALLOCATOR_DISABLE_LINK_LOCAL_NETWORKS;

    private final List<Slot> slots;
    private final ExecutorService endpointControl;
    private final int portAllocatorFlags;
    private final Severity nativeLogThreshold;
    private final AtomicBoolean nativeLogAcquired = new AtomicBoolean();
    private final AtomicInteger nextSlot = new AtomicInteger();

    private final Object lock = new Object();
    // Guarded by lock.
    private CompletableFuture<Void> closed;
    private final Set<WebRtcPeer> peers = new HashSet<>();
    private final Set<WebRtcIceEndpoint> endpoints = new HashSet<>();

    WebRtcEngine(int factories, Supplier<NativePeer.Factory> factory, int portAllocatorFlags, Severity nativeLogThreshold) {
        if (factories < 1) {
            throw new IllegalArgumentException("An engine needs at least one factory: " + factories);
        }
        int id = ENGINE_IDS.incrementAndGet();
        List<Slot> created = new ArrayList<>(factories);
        for (int i = 0; i < factories; i++) {
            created.add(new Slot(controlThread("nethernet-webrtc-" + id + "-control-" + i), factory));
        }
        this.slots = List.copyOf(created);
        this.endpointControl = controlThread("nethernet-webrtc-" + id + "-endpoints");
        this.portAllocatorFlags = portAllocatorFlags;
        this.nativeLogThreshold = Objects.requireNonNull(nativeLogThreshold, "nativeLogThreshold");
    }

    /** A builder with NetworkM's defaults: one factory, its allocator flags, native warnings and errors logged. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Peer createPeer(PeerConfig config, PeerListener listener) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(listener, "listener");
        RTCConfiguration configuration = configuration(config);
        Slot slot = slots.get(Math.floorMod(nextSlot.getAndIncrement(), slots.size()));
        WebRtcPeer peer = new WebRtcPeer(config.role(), listener, slot.control, config.localCredentials());
        synchronized (lock) {
            if (closed != null) {
                throw new IllegalStateException("The WebRTC engine is closed");
            }
            peers.add(peer);
        }
        peer.closeStage().whenComplete((ignored, error) -> {
            synchronized (lock) {
                peers.remove(peer);
            }
        });
        peer.start(observer -> slot.factory().createPeer(configuration, observer));
        return peer;
    }

    /** The webrtc-java configuration of a peer: MAX_BUNDLE, UDP only, the policy's networks and ports. */
    RTCConfiguration configuration(PeerConfig config) {
        RTCConfiguration configuration = new RTCConfiguration();
        configuration.bundlePolicy = RTCBundlePolicy.MAX_BUNDLE;
        configuration.tcpCandidatePolicy = RTCTcpCandidatePolicy.DISABLED;
        for (IceServer server : config.iceServers()) {
            RTCIceServer iceServer = new RTCIceServer();
            iceServer.urls.addAll(server.urls());
            iceServer.username = server.username();
            iceServer.password = server.credential();
            configuration.iceServers.add(iceServer);
        }

        configuration.portAllocatorConfig = allocator(portAllocatorFlags, config.candidates(), config.endpoint() != null);

        DtlsIdentity identity = config.identity();
        if (identity != null) {
            configuration.certificates.add(new RTCCertificatePEM(identity.privateKeyPem(), identity.certificatePem(),
                    RTCCertificatePEM.EXPIRES_UNKNOWN));
        }
        if (config.endpoint() != null) {
            if (!(config.endpoint() instanceof WebRtcIceEndpoint endpoint)) {
                throw new IllegalArgumentException("The ICE endpoint belongs to another engine");
            }
            configuration.iceEndpoint = endpoint.nativeEndpoint(this);
        }
        return configuration;
    }

    /**
     * The port allocator of a peer: the engine's flags without TCP, IPv6 as the policy says, the
     * policy's addresses, and its ports unless the peer is on a shared endpoint.
     *
     * <p>A peer on a shared endpoint gathers without any-address ports. Each one asks the endpoint
     * for a wildcard socket on the port the per-address sockets already hold: libwebrtc's UDP
     * sockets set no {@code SO_REUSEADDR}, so on Linux and macOS that bind fails for every peer,
     * and if it ever bound first, the per-address binds would fail instead. The per-address ports
     * cover every network the allocator enumerates, and when it finds none, libwebrtc falls back to
     * the any address without the flag ({@code basic_port_allocator.cc}). Peers with ports of their
     * own keep the flag.
     */
    static PortAllocatorConfig allocator(int flags, CandidatePolicy policy, boolean sharedEndpoint) {
        PortAllocatorConfig allocator = new PortAllocatorConfig();
        allocator.flags = flags | PortAllocatorConfig.PORTALLOCATOR_DISABLE_TCP;
        if (policy.ipv6()) {
            allocator.setFlag(PortAllocatorConfig.PORTALLOCATOR_ENABLE_IPV6);
        } else {
            allocator.clearFlag(PortAllocatorConfig.PORTALLOCATOR_ENABLE_IPV6);
            allocator.clearFlag(PortAllocatorConfig.PORTALLOCATOR_ENABLE_IPV6_ON_WIFI);
        }
        allocator.allowedAddresses = new ArrayList<>(policy.addresses());
        if (sharedEndpoint) {
            allocator.clearFlag(PortAllocatorConfig.PORTALLOCATOR_ENABLE_ANY_ADDRESS_PORTS);
        } else if (policy.ports() != null) {
            allocator.minPort = policy.ports().min();
            allocator.maxPort = policy.ports().max();
        }
        return allocator;
    }

    @Override
    public CompletionStage<IceEndpoint> openEndpoint(EndpointConfig config) {
        Objects.requireNonNull(config, "config");
        synchronized (lock) {
            if (closed != null) {
                return CompletableFuture.failedStage(closedError());
            }
            // Queued under the lock, so it runs before the task close()
            // queues on the same thread to wait for it.
            return CompletableFuture.supplyAsync(() -> createEndpoint(config), endpointControl).minimalCompletionStage();
        }
    }

    /** On the endpoint thread. An endpoint the engine closed in the meantime is never created, or disposed again. */
    private IceEndpoint createEndpoint(EndpointConfig config) {
        synchronized (lock) {
            if (closed != null) {
                throw closedError();
            }
        }
        requireFreePort(config);
        prepareNatives();
        IceUdpEndpoint created = new IceUdpEndpoint(config.port(), config.addresses().toArray(InetAddress[]::new));
        WebRtcIceEndpoint endpoint = new WebRtcIceEndpoint(this, created, config.port(), endpointControl);
        endpoint.logBindFailures();
        synchronized (lock) {
            if (closed == null) {
                endpoints.add(endpoint);
                return endpoint;
            }
        }
        endpoint.dispose();
        throw closedError();
    }

    private static IllegalStateException closedError() {
        return new IllegalStateException("The WebRTC engine is closed");
    }

    /**
     * webrtc-java binds the endpoint's sockets only when the first peer
     * gathers on an address, and a socket it cannot bind only costs that
     * address its candidates and logs a warning. Binding each address once
     * beforehand turns a taken port into a failure here, where a server can
     * still report it.
     */
    private static void requireFreePort(EndpointConfig config) {
        List<InetSocketAddress> addresses = config.addresses().isEmpty()
                ? List.of(new InetSocketAddress(config.port()))
                : config.addresses().stream().map(address -> new InetSocketAddress(address, config.port())).toList();
        for (InetSocketAddress address : addresses) {
            try (DatagramSocket probe = new DatagramSocket(null)) {
                probe.setReuseAddress(false);
                probe.bind(address);
            } catch (IOException e) {
                BindException taken = new BindException("UDP " + address + " is not available: " + e.getMessage());
                taken.initCause(e);
                throw new UncheckedIOException(taken);
            }
        }
    }

    void endpointClosed(WebRtcIceEndpoint endpoint) {
        synchronized (lock) {
            endpoints.remove(endpoint);
        }
    }

    /** Sets up the native log bridge before the engine's first use of webrtc-java loads the library. */
    private void prepareNatives() {
        if (nativeLogAcquired.compareAndSet(false, true)) {
            NativeLogBridge.acquire(nativeLogThreshold);
        }
    }

    @Override
    public CompletionStage<Void> close() {
        List<WebRtcPeer> open;
        List<WebRtcIceEndpoint> openEndpoints;
        CompletableFuture<Void> result;
        synchronized (lock) {
            if (closed != null) {
                return closed.minimalCompletionStage();
            }
            result = closed = new CompletableFuture<>();
            open = List.copyOf(peers);
            openEndpoints = List.copyOf(endpoints);
        }
        // Runs after every endpoint creation queued before the engine closed.
        // Each of them has then registered its endpoint, which closes below,
        // or created none; either way it took its native resources and its
        // log bridge share before the engine releases them.
        CompletableFuture<Void> creations = CompletableFuture.runAsync(() -> {
        }, endpointControl);
        CompletableFuture<Void> peersClosed = CompletableFuture.allOf(open.stream()
                .map(peer -> peer.close().toCompletableFuture())
                .toArray(CompletableFuture[]::new));
        CompletableFuture.allOf(creations, peersClosed)
                .thenCompose(ignored -> CompletableFuture.allOf(openEndpoints.stream()
                        .map(endpoint -> endpoint.close().toCompletableFuture())
                        .toArray(CompletableFuture[]::new)))
                .thenCompose(ignored -> CompletableFuture.allOf(slots.stream()
                        .map(Slot::dispose)
                        .toArray(CompletableFuture[]::new)))
                .whenComplete((ignored, error) -> {
                    endpointControl.shutdown();
                    if (nativeLogAcquired.get()) {
                        NativeLogBridge.release(nativeLogThreshold);
                    }
                    if (error != null) {
                        log.warn("Closing the WebRTC engine failed", error);
                    }
                    result.complete(null);
                });
        return result.minimalCompletionStage();
    }

    private static ExecutorService controlThread(String name) {
        // Platform threads: they block in JNI calls, which would pin a virtual thread's carrier.
        return Executors.newSingleThreadExecutor(Thread.ofPlatform().name(name).daemon().factory());
    }

    /** One pooled factory and its control thread. */
    private final class Slot {
        final ExecutorService control;
        private final Supplier<NativePeer.Factory> creator;
        // Control thread only.
        private NativePeer.Factory factory;

        Slot(ExecutorService control, Supplier<NativePeer.Factory> creator) {
            this.control = control;
            this.creator = creator;
        }

        /** On the control thread: the factory, created on first use. */
        NativePeer.Factory factory() {
            if (factory == null) {
                prepareNatives();
                factory = creator.get();
            }
            return factory;
        }

        /** Disposes the factory once every peer is closed, then stops the control thread. */
        CompletableFuture<Void> dispose() {
            CompletableFuture<Void> disposed = CompletableFuture.runAsync(() -> {
                if (factory != null) {
                    try {
                        factory.dispose();
                    } catch (RuntimeException | Error e) {
                        // A native reference left behind is a plain Error.
                        if (!NativeErrors.isCallFailure(e)) {
                            throw e;
                        }
                        log.warn("Disposing a WebRTC factory failed: {}", e.toString());
                    }
                    factory = null;
                }
            }, control);
            disposed.whenComplete((ignored, error) -> control.shutdown());
            return disposed;
        }
    }

    /**
     * The SLF4J logger libwebrtc's native messages go to, for a host that sets
     * logging levels itself. Read it here rather than copying it: a shaded
     * build relocates it.
     *
     * @return the logger name
     */
    public static String nativeLoggerName() {
        return NativeLogBridge.LOGGER_NAME;
    }

    /**
     * The system property webrtc-java reads its native threshold from, once,
     * when it loads its library (see {@link Builder#nativeLogThreshold}).
     *
     * @return the property name
     */
    public static String nativeSeverityProperty() {
        return NativeLogBridge.SEVERITY_PROPERTY;
    }

    /**
     * Engine-specific tuning. Everything a peer needs that is not specific to
     * libwebrtc is in {@link PeerConfig}.
     */
    public static final class Builder {
        private int factories = 1;
        private int portAllocatorFlags = DEFAULT_PORT_ALLOCATOR_FLAGS;
        private Map<String, String> fieldTrials = Map.of();
        private Severity nativeLogThreshold = Severity.WARNING;

        private Builder() {
        }

        /**
         * How many webrtc-java factories the engine pools. Each brings its own
         * network, signaling and control threads; peers go to them in turn.
         *
         * @param factories at least 1; the default is 1
         * @return this builder
         */
        public Builder factories(int factories) {
            if (factories < 1) {
                throw new IllegalArgumentException("An engine needs at least one factory: " + factories);
            }
            this.factories = factories;
            return this;
        }

        /**
         * The port allocator flags of every peer, a combination of the
         * {@code PortAllocatorConfig.PORTALLOCATOR_*} constants. A peer's
         * {@link CandidatePolicy} applies on top: its IPv6 choice sets or
         * clears the IPv6 flags, and TCP stays disabled.
         *
         * @param flags the flags; the default is {@link #DEFAULT_PORT_ALLOCATOR_FLAGS}
         * @return this builder
         */
        public Builder portAllocatorFlags(int flags) {
            this.portAllocatorFlags = flags;
            return this;
        }

        /**
         * WebRTC field trials for every factory, for example
         * {@code Map.of("WebRTC-Foo", "Enabled")}.
         *
         * @param fieldTrials the trials; copied
         * @return this builder
         */
        public Builder fieldTrials(Map<String, String> fieldTrials) {
            this.fieldTrials = Map.copyOf(fieldTrials);
            return this;
        }

        /**
         * The lowest severity of libwebrtc's native messages that reach the
         * SLF4J logger {@value NativeLogBridge#LOGGER_NAME}, or
         * {@link Severity#NONE} for none. Which of them are written stays with
         * the host's logging configuration: errors log as ERROR, warnings as
         * WARN, INFO as DEBUG and VERBOSE as TRACE. A few errors and warnings
         * that report routine events, such as the abort every client quit
         * logs, log as DEBUG.
         *
         * <p>webrtc-java fixes its own threshold when it loads its library,
         * from the system property
         * {@code io.github.sendablemetatype.webrtc.logging.severity}. If the
         * host has not set that property, the engine sets it to this value
         * before it first uses webrtc-java; a lower value set later, or by
         * another engine after the library loaded, does not lower it.
         *
         * @param threshold the threshold; the default is {@link Severity#WARNING}
         * @return this builder
         */
        public Builder nativeLogThreshold(Severity threshold) {
            this.nativeLogThreshold = Objects.requireNonNull(threshold, "threshold");
            return this;
        }

        /**
         * Builds the engine. No native resources exist until the first peer
         * or endpoint needs them.
         *
         * @return the engine, owned by the caller
         */
        public WebRtcEngine build() {
            Map<String, String> trials = fieldTrials;
            return new WebRtcEngine(factories, () -> new NativePeer.RtcFactory(trials.isEmpty()
                    ? new PeerConnectionFactory()
                    : new PeerConnectionFactory(trials)), portAllocatorFlags, nativeLogThreshold);
        }
    }
}

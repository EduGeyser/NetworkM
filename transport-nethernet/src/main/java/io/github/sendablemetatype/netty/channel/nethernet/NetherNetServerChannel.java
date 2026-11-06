package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressTracker;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherServerChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerThrottle;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EndpointConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AdvertisedAddresses;
import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateAnnouncement;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidateIntake;
import io.github.sendablemetatype.netty.channel.nethernet.ice.StunObserver;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSink;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SharedIcePort;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.IdentityRefusal;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.OfferPolicy;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.channel.AbstractServerChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A NetherNet server endpoint. It takes joins from any number of {@link JoinSource}s (HTTP, Xbox
 * RPC, LAN discovery), admits each through one admission stage, and emits a
 * {@link NetherNetChildChannel} per admitted join, like any Netty server channel.
 *
 * <p><b>Sources.</b> {@link #addSource} adds a source before or after bind; it starts once the
 * endpoint is bound and its ICE port is open. {@link #removeSource} stops and closes one.
 * Neither touches connected players, since every join replies through its own
 * {@link io.github.sendablemetatype.netty.channel.nethernet.signaling.ReplyPath}.
 * A source is started once; to restart one, remove it and add a new instance. Closing the
 * endpoint closes its sources.
 *
 * <p><b>Admission.</b> Every join passes the same stage, whatever its source, on this channel's
 * event loop: the per-address throttle ({@link NetherChannelOption#NETHER_SERVER_THROTTLE}), one
 * budget for sessions and pending handshakes shared by all sources
 * ({@link NetherChannelOption#NETHER_SERVER_MAX_SESSIONS},
 * {@link NetherChannelOption#NETHER_SERVER_MAX_PENDING_JOINS}), and the offer's identity check
 * ({@link NetherChannelOption#NETHER_SERVER_OFFER_CHECK}) under the source's {@link OfferPolicy}.
 * The check runs off the event loop with a deadline. A join whose source authenticated the client
 * itself ({@link Join.Authenticated}, NXS) skips the check and brings the binding that decides its
 * login; the throttle and the budget apply to it all the same. A refused join gets a typed
 * {@link JoinRefusal} through its reply path and creates nothing. A join holds its share of the
 * budget and its throttle entry until its child's {@linkplain NetherNetChannel#termination()
 * termination} completed.
 *
 * <p><b>Identity.</b> One {@link ServerIdentity}, set with
 * {@link NetherChannelOption#NETHER_SERVER_IDENTITY}, signs the answers of every source. Binding
 * fails without one.
 *
 * <p><b>ICE.</b> By default every peer gathers its host candidates on one UDP port at the address
 * and port this channel binds (port 0 picks a free one). A taken port fails the bind. The engine
 * opens the port after bind returned, and the sources start once it is open; a port that fails
 * to open then closes the endpoint, with the error logged. With
 * {@link NetherChannelOption#NETHER_SERVER_SHARED_ICE_PORT} false, each session takes ports of
 * its own from {@link NetherChannelOption#NETHER_CANDIDATE_POLICY}'s window instead. On the shared
 * port, {@link NetherChannelOption#NETHER_SERVER_STUN_SERVERS} keeps STUN bindings alive from it and
 * answers announce the fresh mappings.
 *
 * <p>The child is exposed as soon as its join is admitted. It owns its engine peer from creation
 * and negotiates in its own initializer once registered. The engine is either the caller's, which
 * this channel never closes, or one this channel creates when it binds and closes with itself.
 */
public class NetherNetServerChannel extends AbstractServerChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetServerChannel.class);
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);

    private final DefaultNetherServerChannelConfig config;
    private final EngineRef engine;
    private final Supplier<? extends ExecutorService> checkExecutors;
    private final AdvertisedLookup names;
    private final AdmissionGate gate = new AdmissionGate();
    /** Children whose termination has not completed yet. */
    private final Set<NetherNetChildChannel> live = ConcurrentHashMap.newKeySet();
    /** Joins in the admission stage, which closing the endpoint or removing their source ends. */
    private final Set<Admission> admitting = ConcurrentHashMap.newKeySet();
    /** The one source's hold on the shared port's held checks, if any source took them. */
    private final AtomicReference<HeldCheckClaim> heldChecks = new AtomicReference<>();

    private final Object sourcesLock = new Object();
    // Guarded by sourcesLock.
    private final Map<JoinSource, Registration> sources = new LinkedHashMap<>();
    private boolean sourcesStarted;

    private volatile InetSocketAddress localAddress;
    /**
     * The shared ICE port, opening from bind on and owned by this channel from then: whatever it
     * yields closes with the channel, also when it opens after the channel closed. Null when each
     * session takes its own ports.
     */
    private volatile CompletionStage<IceEndpoint> iceEndpoint;
    /** What answers announce: the literal advertised entries, then every entry once names resolved. */
    private volatile List<InetSocketAddress> advertised = List.of();
    private volatile Bound bound;
    /** Polls STUN discovery on the event loop; null without discovery. Event loop only. */
    private ScheduledFuture<?> discoveryRefresh;
    private volatile boolean open = true;

    /** Creates an endpoint with an engine of its own, created when it binds and closed with it. */
    public NetherNetServerChannel() {
        this(EngineRef.owned(DefaultEngine::create), OfferCheck::newExecutor);
    }

    /**
     * Creates an endpoint on the caller's engine, which the endpoint never closes.
     *
     * @param engine the engine; shared engines are closed by their owner
     */
    public NetherNetServerChannel(NetherNetEngine engine) {
        this(EngineRef.shared(engine), OfferCheck::newExecutor);
    }

    /**
     * Creates an endpoint that owns the engine the supplier returns. The supplier runs once, when
     * the endpoint binds; the endpoint closes the engine with itself.
     *
     * @param engineSupplier creates the engine
     */
    public NetherNetServerChannel(Supplier<? extends NetherNetEngine> engineSupplier) {
        this(EngineRef.owned(engineSupplier), OfferCheck::newExecutor);
    }

    /**
     * @param checkExecutors creates the bounded executor identity checks run on, once per bind
     */
    NetherNetServerChannel(EngineRef engine, Supplier<? extends ExecutorService> checkExecutors) {
        this(engine, checkExecutors, AdvertisedAddresses::resolve);
    }

    /**
     * @param checkExecutors creates the bounded executor identity checks run on, once per bind
     * @param names looks up the advertised names, off the event loop
     */
    NetherNetServerChannel(EngineRef engine, Supplier<? extends ExecutorService> checkExecutors,
                           AdvertisedLookup names) {
        this.engine = engine;
        this.checkExecutors = checkExecutors;
        this.names = names;
        this.config = new DefaultNetherServerChannelConfig(this);
    }

    /** Looks up advertised names, as {@link AdvertisedAddresses#resolve(List)} does. */
    @FunctionalInterface
    interface AdvertisedLookup {
        List<InetSocketAddress> resolve(List<InetSocketAddress> endpoints) throws UnknownHostException;
    }

    /**
     * What bind decided before the shared ICE port opened.
     *
     * @param answers what every join answers with
     * @param relays the PROXY protocol relays in front of the ICE port, empty for none
     * @param offerCheck checks every join's identity
     * @param stunServers the STUN servers discovery asks, empty for none
     * @param stunMaxAge how long a discovered mapping counts
     */
    private record BindPlan(ServerJoin.Settings answers, List<String> relays, OfferCheck offerCheck,
                            List<InetSocketAddress> stunServers, Duration stunMaxAge) {
    }

    /**
     * What the endpoint takes joins with, fixed for its life once its ICE port is open.
     *
     * @param answers what every join answers with
     * @param iceEndpoint the shared ICE port, or null when each session takes its own
     * @param offerCheck checks every join's identity
     * @param checkExecutor runs the checks, off the event loop
     * @param discovery the STUN mappings answers announce, or null without discovery
     */
    private record Bound(ServerJoin.Settings answers, IceEndpoint iceEndpoint, OfferCheck offerCheck,
                         ExecutorService checkExecutor, StunDiscovery discovery) {
    }

    // ---- Sources ----

    /**
     * Adds a join source whose offers must prove their key, attested or not
     * ({@link OfferPolicy#REQUIRE_KEY_PROVEN}).
     *
     * @param source the source; this endpoint starts it and closes it with itself
     * @see #addSource(JoinSource, OfferPolicy)
     */
    public void addSource(JoinSource source) {
        addSource(source, OfferPolicy.REQUIRE_KEY_PROVEN);
    }

    /**
     * Adds a join source. An endpoint that takes joins already starts it at once, otherwise the
     * endpoint starts it once it is bound and its ICE port is open.
     *
     * @param source the source; this endpoint starts it and closes it with itself
     * @param policy which identity check results this source's joins need
     * @throws IllegalStateException if the endpoint is closed or already has this source
     */
    public void addSource(JoinSource source, OfferPolicy policy) {
        Registration registration = new Registration(source, policy);
        synchronized (sourcesLock) {
            if (!open) {
                throw new IllegalStateException("The NetherNet endpoint is closed");
            }
            if (sources.putIfAbsent(source, registration) != null) {
                throw new IllegalStateException("The NetherNet endpoint already has this source");
            }
            if (!sourcesStarted) {
                return;
            }
        }
        registration.start();
    }

    /**
     * Stops and closes a source. Its players stay connected; joins it delivered that have no child
     * yet are refused at once and give back their share of the budget.
     *
     * @param source the source
     * @return whether the endpoint had it
     */
    public boolean removeSource(JoinSource source) {
        Registration registration;
        synchronized (sourcesLock) {
            registration = sources.remove(source);
        }
        if (registration == null) {
            return false;
        }
        registration.removed = true;
        // Before the source closes, so the refusals can still reach their clients.
        withdrawAdmissions(registration);
        registration.releaseHeldChecks();
        closeQuietly(source);
        return true;
    }

    /** The endpoint's sources, in the order they were added. */
    public List<JoinSource> sources() {
        synchronized (sourcesLock) {
            return List.copyOf(sources.keySet());
        }
    }

    /** One source's place in the endpoint: the sink it delivers to and the policy its joins get. */
    private final class Registration implements JoinSink {
        private final JoinSource source;
        private final OfferPolicy policy;
        private volatile boolean removed;
        private volatile HeldCheckClaim claim;

        private Registration(JoinSource source, OfferPolicy policy) {
            this.source = Objects.requireNonNull(source, "source");
            this.policy = Objects.requireNonNull(policy, "policy");
        }

        void start() {
            try {
                source.start(this);
            } catch (RuntimeException e) {
                log.warn("A NetherNet join source failed to start: {}", e.toString());
            }
        }

        @Override
        public void join(Join join) {
            try {
                eventLoop().execute(() -> admit(this, join));
            } catch (RejectedExecutionException | IllegalStateException closing) {
                refuse(join, JoinRefusal.CAPACITY);
            }
        }

        @Override
        public boolean admits(SourcedAddress address) {
            return NetherNetServerChannel.this.admits(this, address);
        }

        @Override
        public Optional<SharedIcePort> sharedIcePort() {
            Bound settings = bound;
            IceEndpoint endpoint = settings == null ? null : settings.iceEndpoint();
            return endpoint == null ? Optional.empty() : Optional.of(new SourcePort(this, endpoint));
        }

        void releaseHeldChecks() {
            HeldCheckClaim current = claim;
            if (current != null) {
                current.close();
            }
        }
    }

    /** The shared port as one source sees it. */
    private final class SourcePort implements SharedIcePort {
        private final Registration from;
        private final IceEndpoint endpoint;

        private SourcePort(Registration from, IceEndpoint endpoint) {
            this.from = from;
            this.endpoint = endpoint;
        }

        @Override
        public InetSocketAddress localAddress() {
            return NetherNetServerChannel.this.localAddress;
        }

        @Override
        public CompletionStage<List<InetSocketAddress>> gatherHostAddresses() {
            NetherNetEngine current = engine.current();
            if (current == null || !open || from.removed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("The NetherNet endpoint or the join source is closed"));
            }
            return HostAddressProbe.gather(current, endpoint, config.getCandidatePolicy());
        }

        @Override
        public HeldChecks claimHeldChecks(Consumer<IceEndpoint.HeldCheck> handler) {
            Objects.requireNonNull(handler, "handler");
            HeldCheckClaim claim = new HeldCheckClaim(endpoint, handler);
            if (!open || from.removed) {
                throw new IllegalStateException("The NetherNet endpoint or the join source is closed");
            }
            if (!heldChecks.compareAndSet(null, claim)) {
                throw new IllegalStateException("Another join source holds the shared ICE port's held checks");
            }
            from.claim = claim;
            if (!open || from.removed) {
                // Removed or closed meanwhile, after the release that would have found it.
                claim.close();
                throw new IllegalStateException("The NetherNet endpoint or the join source is closed");
            }
            return claim;
        }

        @Override
        public CompletionStage<IceEndpoint.StunMonitor> monitorStun(InetSocketAddress server) {
            return endpoint.monitorStun(server);
        }
    }

    /**
     * A source's hold on the held checks. The engine passes a check for which no peer exists on unverified when no
     * derived passwords are set, so the handler is set only while keys are, and derived passwords always go in
     * before the handler and come out after it.
     */
    private final class HeldCheckClaim implements SharedIcePort.HeldChecks {
        private final IceEndpoint endpoint;
        private final Consumer<IceEndpoint.HeldCheck> handler;
        // Guarded by this.
        private boolean handling;
        private volatile boolean closed;

        private HeldCheckClaim(IceEndpoint endpoint, Consumer<IceEndpoint.HeldCheck> handler) {
            this.endpoint = endpoint;
            this.handler = handler;
        }

        @Override
        public synchronized void admit(byte[] context, Map<String, byte[]> keys) {
            if (closed) {
                return;
            }
            if (keys.isEmpty()) {
                stopHandling();
                return;
            }
            endpoint.derivedPasswords(context, keys);
            if (!handling) {
                endpoint.onHeldCheck(this::held, eventLoop());
                handling = true;
            }
        }

        private void held(IceEndpoint.HeldCheck check) {
            if (closed || !open) {
                check.reject();
                return;
            }
            try {
                handler.accept(check);
            } catch (RuntimeException e) {
                log.debug("A held check handler failed: {}", e.toString());
                check.reject();
            }
        }

        private void stopHandling() {
            if (handling) {
                endpoint.clearHeldCheckHandler();
                handling = false;
            }
            endpoint.clearDerivedPasswords();
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            stopHandling();
            heldChecks.compareAndSet(this, null);
        }
    }

    // ---- Bind and close ----

    @Override
    protected void doBind(SocketAddress requested) throws Exception {
        if (!(requested instanceof InetSocketAddress address)) {
            throw new IllegalArgumentException("Unsupported address type");
        }
        ServerIdentity identity = config.getServerIdentity();
        if (identity == null) {
            throw new IllegalStateException("No NetherNet server identity: set "
                    + "NetherChannelOption.NETHER_SERVER_IDENTITY to ServerIdentity.loadOrCreate(keyFile, domain), "
                    + "or to ServerIdentity.ephemeral(domain) if plain HTTP players may be asked to trust the "
                    + "server again after every restart");
        }
        TrustedProxies proxies = config.getTrustedProxies();
        boolean relayed = !proxies.proxyProtocol().isEmpty();
        boolean shared = config.isSharedIcePort();
        if (relayed && !shared) {
            throw new IllegalStateException("PROXY protocol relays for media need the shared ICE port");
        }
        List<InetSocketAddress> configured = config.getAdvertisedAddresses();
        // Literals count against the limit now; this looks nothing up.
        List<InetSocketAddress> literals = AdvertisedAddresses.resolve(
                configured.stream().filter(endpoint -> !endpoint.isUnresolved()).toList());
        List<InetSocketAddress> stunServers = config.getStunServers();
        if (!stunServers.isEmpty()) {
            if (!shared) {
                throw new IllegalStateException("STUN discovery needs the shared ICE port: a port window has no fixed "
                        + "port whose mapping it could learn");
            }
            if (relayed) {
                throw new IllegalStateException("PROXY protocol relays front the ICE port, so STUN discovery would "
                        + "announce an address players cannot reach");
            }
            if (!configured.isEmpty()) {
                throw new IllegalStateException("STUN discovery and advertised addresses cannot both be set");
            }
        }
        if (relayed && configured.isEmpty()) {
            log.warn(relaysWithoutAddressesWarning(config.getIceServers().stream().anyMatch(IceServer::isTurn)));
        }

        NetherNetEngine current = engine.get();
        InetSocketAddress local = address;
        int icePort = 0;
        CompletionStage<IceEndpoint> opening = null;
        if (shared) {
            InetAddress host = address.getAddress();
            icePort = probeUdpPort(address);
            List<InetAddress> addresses = host == null || host.isAnyLocalAddress() ? List.of() : List.of(host);
            opening = current.openEndpoint(new EndpointConfig(icePort, addresses));
            this.iceEndpoint = opening;
            local = new InetSocketAddress(host, icePort);
        }
        this.localAddress = local;
        this.advertised = literals;
        if (configured.stream().anyMatch(InetSocketAddress::isUnresolved)) {
            lookUpAdvertisedNames(configured);
        }

        OfferCheck offerCheck = config.getOfferCheck();
        // Off the event loop, and off the bounded check executor, whose slots belong to joins: the
        // first join should not wait for the issuers' keys.
        Thread.ofVirtual().name("nethernet-issuer-keys").start(offerCheck::prefetch);
        ServerJoin.Settings answers = new ServerJoin.Settings(identity, config.isInferPeerCandidates(), icePort,
                relayed);
        BindPlan plan = new BindPlan(answers, relayed ? proxies.proxyProtocol().entries() : List.of(), offerCheck,
                stunServers, config.getStunMaxAge());

        CompletableFuture<IceEndpoint> opened = opening == null ? CompletableFuture.completedFuture(null)
                : opening.toCompletableFuture();
        if (opened.isDone()) {
            // Ports per session, or an engine that opened the port at once: a failure fails the bind.
            IceEndpoint endpoint;
            try {
                endpoint = opened.join();
            } catch (CompletionException e) {
                throw e.getCause() instanceof Exception cause ? cause : e;
            }
            ready(plan, endpoint);
            return;
        }
        // The engine opens the port on its own thread; this loop does not wait for it (ruling A-4).
        opening.whenComplete((endpoint, failure) -> {
            try {
                eventLoop().execute(() -> opened(plan, endpoint, failure));
            } catch (RejectedExecutionException gone) {
                // No loop left to close this channel on, so nothing else would close the port.
                if (endpoint != null) {
                    endpoint.close();
                }
            }
        });
    }

    /**
     * The bind warning for PROXY protocol relays that no advertised address names. Answers then
     * carry only TURN relay candidates, since anything else would name this host, so players
     * cannot use the relays at all.
     *
     * @param turnConfigured whether the host configured a TURN server; Xbox signaling hands out
     *                       its own per join, which a bind cannot know about
     */
    private static String relaysWithoutAddressesWarning(boolean turnConfigured) {
        String fix = " Set NETHER_SERVER_ADVERTISED_ADDRESSES to the relays' public addresses.";
        return turnConfigured
                ? "PROXY protocol relays front the ICE port, but no advertised addresses name them: answers carry "
                        + "only TURN relay candidates, so players connect through TURN, not through the relays." + fix
                : "PROXY protocol relays front the ICE port, but no advertised addresses name them and no TURN server "
                        + "is configured: answers carry no candidates, so players cannot reach this server through "
                        + "the relays at all, and only joins whose signaling hands out TURN servers (Xbox) can "
                        + "connect." + fix;
    }

    /**
     * Binds the shared ICE port once and lets it go again, so a taken port fails the bind before
     * doBind returns, as Netty needs; the engine opens the port later, on its own thread, and
     * checks it again then. A plain socket bind, which waits on nothing. Port 0 picks a free port.
     */
    private static int probeUdpPort(InetSocketAddress address) throws IOException {
        InetAddress host = address.getAddress();
        // Where the engine binds: every address for the wildcard, else the one address.
        InetSocketAddress probed = host == null || host.isAnyLocalAddress()
                ? new InetSocketAddress(address.getPort()) : address;
        try (DatagramSocket probe = new DatagramSocket(null)) {
            probe.setReuseAddress(false);
            probe.bind(probed);
            return probe.getLocalPort();
        }
    }

    /**
     * Looks the advertised names up once, on a thread of its own, since DNS can take seconds; the
     * event loop does not wait for it. Joins until then announce the literal entries only. A name
     * that does not resolve closes the endpoint, as it failed the bind before.
     */
    private void lookUpAdvertisedNames(List<InetSocketAddress> configured) {
        Thread.ofVirtual().name("nethernet-advertised-names").start(() -> {
            List<InetSocketAddress> resolved = null;
            Exception failure = null;
            try {
                resolved = names.resolve(configured);
            } catch (UnknownHostException | IllegalArgumentException e) {
                failure = e;
            }
            List<InetSocketAddress> found = resolved;
            Exception failed = failure;
            try {
                eventLoop().execute(() -> advertisedNamesResolved(found, failed));
            } catch (RejectedExecutionException gone) {
                // Nothing left to announce them to.
            }
        });
    }

    private void advertisedNamesResolved(List<InetSocketAddress> resolved, Exception failure) {
        if (!open) {
            return;
        }
        if (failure != null) {
            log.error("The NetherNet endpoint at {} could not resolve its advertised addresses, closing it: {}",
                    localAddress, failure.getMessage());
            close();
            return;
        }
        advertised = resolved;
    }

    /** The shared ICE port opened, or failed to, after bind returned. Event loop. */
    private void opened(BindPlan plan, IceEndpoint endpoint, Throwable failure) {
        if (!open) {
            // doClose closes the port, now that it opened.
            return;
        }
        if (failure != null) {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause() : failure;
            log.error("The NetherNet endpoint at {} could not open its ICE port, closing it: {}", localAddress,
                    cause.toString());
            close();
            return;
        }
        try {
            ready(plan, endpoint);
        } catch (RuntimeException e) {
            log.error("The NetherNet endpoint at {} could not set up its ICE port, closing it: {}", localAddress,
                    e.toString());
            close();
        }
    }

    /** The ICE port is open, if it is shared: the endpoint takes joins from now on. Event loop. */
    private void ready(BindPlan plan, IceEndpoint endpoint) {
        if (endpoint != null && !plan.relays().isEmpty()) {
            endpoint.proxyRelays(plan.relays());
        }
        StunDiscovery discovery = null;
        if (endpoint != null && !plan.stunServers().isEmpty()) {
            discovery = new StunDiscovery(plan.stunServers(), plan.stunMaxAge(), endpoint.port(),
                    StunObserver.sharedPort(endpoint::monitorStun,
                            task -> Thread.ofVirtual().name("nethernet-stun-lookup").start(task)),
                    System::nanoTime);
            long period = StunDiscovery.REFRESH.toNanos();
            discoveryRefresh = eventLoop().scheduleAtFixedRate(discovery::refresh, period, period, TimeUnit.NANOSECONDS);
        }
        // NXS stateless joins arrive at the shared port as checks it holds; the source that claims them
        // (SharedIcePort.claimHeldChecks) turns each into an authenticated join, which takes the same
        // throttle and budget as every other join.
        this.bound = new Bound(plan.answers(), endpoint, plan.offerCheck(), checkExecutors.get(), discovery);
        log.info("NetherNet endpoint bound to {} ({}), server identity SHA-256 {}", localAddress,
                endpoint != null ? "shared ICE port" : "ICE ports per session",
                plan.answers().identity().fingerprint());

        List<Registration> toStart;
        synchronized (sourcesLock) {
            sourcesStarted = true;
            toStart = List.copyOf(sources.values());
        }
        toStart.forEach(Registration::start);
    }

    @Override
    protected void doClose() throws Exception {
        List<Registration> closing;
        synchronized (sourcesLock) {
            open = false;
            closing = List.copyOf(sources.values());
            sources.clear();
        }
        for (Registration registration : closing) {
            registration.removed = true;
            registration.releaseHeldChecks();
        }
        // Before the sources close, so the refusals can still reach their clients. Without this,
        // a check the executor drops below would keep its share and throttle entry for good.
        List.copyOf(admitting).forEach(Admission::withdraw);
        for (Registration registration : closing) {
            closeQuietly(registration.source);
        }

        List<CompletableFuture<Void>> terminations = new ArrayList<>();
        for (NetherNetChildChannel child : live) {
            terminations.add(child.termination().toCompletableFuture());
            child.close();
        }
        Bound settings = bound;
        if (settings != null) {
            settings.checkExecutor().shutdownNow();
            if (settings.discovery() != null) {
                // Before the port closes, so the monitors close on an endpoint that is still there.
                discoveryRefresh.cancel(false);
                settings.discovery().close();
            }
        }
        // The shared port closes once no peer uses it anymore, also when it is still opening, then
        // an owned engine releases its threads; a shared engine stays with its owner.
        CompletionStage<IceEndpoint> port = iceEndpoint;
        CompletableFuture.allOf(terminations.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> port == null ? CompletableFuture.<IceEndpoint>completedFuture(null)
                        : port.handle((endpoint, failure) -> endpoint))
                .thenCompose(endpoint -> endpoint == null ? CompletableFuture.<Void>completedFuture(null)
                        : endpoint.close())
                .whenComplete((ignored, failure) -> engine.release());
    }

    private static void closeQuietly(JoinSource source) {
        try {
            source.close();
        } catch (RuntimeException e) {
            log.debug("Closing a NetherNet join source failed: {}", e.toString());
        }
    }

    // ---- Admission ----

    /** Whether a join from this address would get past the throttle and the budget now. */
    private boolean admits(Registration from, SourcedAddress address) {
        if (!open || bound == null || from.removed
                || !gate.admits(config.getMaxSessions(), config.getMaxPendingJoins())) {
            return false;
        }
        NetherServerThrottle throttle = config.getThrottle();
        InetSocketAddress key = throttleKey(address);
        return key == null || throttle.admits(key);
    }

    /** The address to throttle on, or null when the source does not know where the client is. */
    private static InetSocketAddress throttleKey(SourcedAddress source) {
        InetSocketAddress address = source.address();
        if (source.source() == AddressSource.UNKNOWN || address.getAddress().isAnyLocalAddress()) {
            return null;
        }
        return address;
    }

    /** The first step of the admission stage, on this channel's event loop. */
    private void admit(Registration from, Join join) {
        Bound settings = bound;
        if (!open || settings == null || from.removed) {
            refuse(join, JoinRefusal.CAPACITY);
            return;
        }
        NetherServerThrottle throttle = config.getThrottle();
        InetSocketAddress throttled = throttleKey(join.source());
        if (throttled != null && !throttle.accept(throttled)) {
            log.debug("Refused join {} from {}: throttled", join.connectionId(), throttled);
            serverMetrics(metrics -> metrics.joinThrottled(throttled));
            refuse(join, JoinRefusal.CAPACITY);
            return;
        }
        AdmissionGate.Reservation reservation = gate.reserve(config.getMaxSessions(), config.getMaxPendingJoins());
        if (reservation == null) {
            if (throttled != null) {
                throttle.closed(throttled);
            }
            log.debug("Refused join {}: the endpoint is at capacity", join.connectionId());
            refuse(join, JoinRefusal.CAPACITY);
            return;
        }
        new Admission(from, join, settings, reservation, throttled == null ? null : throttle, throttled).start();
    }

    /** Refuses a join that never became a child. */
    private void refuse(Join join, JoinRefusal refusal) {
        serverMetrics(metrics -> metrics.joinRefused(refusal.httpStatus()));
        try {
            join.reply().refuse(refusal).whenComplete((ignored, failure) -> {
                if (failure != null) {
                    log.debug("Could not refuse join {}: {}", join.connectionId(),
                            failure.toString());
                }
            });
        } catch (RuntimeException e) {
            log.debug("Could not refuse join {}: {}", join.connectionId(), e.toString());
        }
        join.reply().finished();
    }

    /**
     * One join from the moment it holds its share of the budget until it becomes a child or is
     * refused. Its steps run on the event loop. It ends exactly once, whoever ends it: its check,
     * its deadline, its client leaving, or {@link #withdraw} when the endpoint closes or its source
     * is removed, which can happen on any thread.
     */
    private final class Admission {
        private final Registration from;
        private final Join join;
        private final String id;
        private final Bound settings;
        private final AdmissionGate.Reservation reservation;
        private final NetherServerThrottle throttle;
        private final InetSocketAddress throttled;
        private final AtomicBoolean settled = new AtomicBoolean();
        private int maxMessageSize;
        private volatile CompletableFuture<OfferCheckResult> check;
        private volatile ScheduledFuture<?> deadline;
        /** When the check started and how long it may take, on the event loop's clock. */
        private long startedNanos;
        private long timeoutNanos;

        private Admission(Registration from, Join join, Bound settings, AdmissionGate.Reservation reservation,
                          NetherServerThrottle throttle, InetSocketAddress throttled) {
            this.from = from;
            this.join = join;
            this.id = join.connectionId();
            this.settings = settings;
            this.reservation = reservation;
            this.throttle = throttle;
            this.throttled = throttled;
        }

        void start() {
            try {
                maxMessageSize = NetherNetConstants.parseMaxMessageSize(join.offer());
            } catch (IllegalArgumentException e) {
                log.debug("Refusing join {}: {}", id, e.getMessage());
                fail(JoinRefusal.INTERNAL);
                return;
            }
            if (join.authenticated() != null) {
                // The source authenticated the client, and its binding decides the login: no offer check.
                if (!open || from.removed) {
                    fail(JoinRefusal.CAPACITY);
                } else if (settle()) {
                    createChild(this, null);
                }
                return;
            }
            admitting.add(this);
            if (from.removed) {
                // Removed on another thread after this join got in, too late to find it in admitting.
                withdraw();
                return;
            }
            join.reply().abandoned().whenComplete((ignored, failure) -> onLoop(this::abandoned));
            timeoutNanos = TimeUnit.MILLISECONDS.toNanos(config.getOfferCheckTimeoutMillis());
            startedNanos = eventLoop().ticker().nanoTime();
            CompletableFuture<OfferCheckResult> started = settings.offerCheck().checkAsync(join.offer(),
                    settings.checkExecutor());
            check = started;
            // A check that finishes after this admits nothing: its stage is already complete.
            ScheduledFuture<?> timer = eventLoop().schedule(() -> {
                started.completeExceptionally(new TimeoutException());
            }, timeoutNanos, TimeUnit.NANOSECONDS);
            deadline = timer;
            if (settled.get()) {
                // Withdrawn on another thread while the check was being set up.
                started.cancel(true);
                timer.cancel(false);
                return;
            }
            started.whenComplete((result, failure) -> onLoop(() -> checked(result, failure)));
        }

        private void checked(OfferCheckResult result, Throwable failure) {
            if (settled.get()) {
                return;
            }
            // The deadline only fires once this loop gets to it: a loop that ran late can see a
            // result that came in past the deadline before the deadline fired.
            if (failure == null && eventLoop().ticker().nanoTime() - startedNanos >= timeoutNanos) {
                log.debug("Refusing join {}: its identity check finished after its deadline", id);
                fail(JoinRefusal.NO_ANSWER_IN_TIME);
                return;
            }
            if (failure != null) {
                Throwable cause = failure instanceof CompletionException ? failure.getCause() : failure;
                JoinRefusal refusal = switch (cause) {
                    case TimeoutException late -> JoinRefusal.NO_ANSWER_IN_TIME;
                    case RejectedExecutionException full -> JoinRefusal.CAPACITY;
                    default -> JoinRefusal.INTERNAL;
                };
                log.debug("Refusing join {}: its identity check did not finish ({})", id, cause.toString());
                fail(refusal);
                return;
            }
            Optional<IdentityRefusal> refused = from.policy.refusal(result);
            if (refused.isPresent()) {
                // Diagnostics hold no token, and their text is sanitized for logs.
                log.info("Refused join {} from {}: {} ({}, {})", id, join.source().address(), refused.get(),
                        result.reason(), result.diagnostics());
                fail(refusalOf(refused.get()));
                return;
            }
            if (!open || from.removed) {
                fail(JoinRefusal.CAPACITY);
                return;
            }
            if (settle()) {
                createChild(this, result);
            }
        }

        /** The client left before the check finished: nothing to answer, nothing to create. */
        private void abandoned() {
            if (settle()) {
                cancelCheck();
                release();
                join.reply().finished();
            }
        }

        /**
         * Ends a join that has no child yet because the endpoint closed or its source was removed:
         * withdraws its check, gives back what it holds and refuses it. Any thread.
         */
        void withdraw() {
            if (settle()) {
                cancelCheck();
                release();
                refuse(join, JoinRefusal.CAPACITY);
            }
        }

        /** Withdraws the check: a queued one never runs, a running one is interrupted. */
        private void cancelCheck() {
            CompletableFuture<OfferCheckResult> pending = check;
            if (pending != null) {
                pending.cancel(true);
            }
        }

        private void fail(JoinRefusal refusal) {
            if (settle()) {
                release();
                refuse(join, refusal);
            }
        }

        /** Ends the admission stage. True for the one caller that ended it, who settles the join. */
        private boolean settle() {
            if (!settled.compareAndSet(false, true)) {
                return false;
            }
            admitting.remove(this);
            ScheduledFuture<?> timer = deadline;
            if (timer != null) {
                timer.cancel(false);
            }
            return true;
        }

        /** Gives back everything the join holds. Any thread, once the child's termination completed. */
        void release() {
            reservation.release();
            if (throttle != null) {
                throttle.closed(throttled);
            }
        }

        private void onLoop(Runnable task) {
            try {
                eventLoop().execute(task);
            } catch (RejectedExecutionException closing) {
                if (settle()) {
                    release();
                    join.reply().finished();
                }
            }
        }
    }

    /** Ends the admission of every join from this source that has no child yet. Any thread. */
    private void withdrawAdmissions(Registration from) {
        for (Admission admission : admitting) {
            if (admission.from == from) {
                admission.withdraw();
            }
        }
    }

    private static JoinRefusal refusalOf(IdentityRefusal refusal) {
        return switch (refusal) {
            case IDENTITY_MISSING -> JoinRefusal.IDENTITY_MISSING;
            case IDENTITY_INVALID, NOT_ATTESTED -> JoinRefusal.IDENTITY_INVALID;
            case TRUST_SOURCE_UNREACHABLE -> JoinRefusal.TRUST_SOURCE_UNREACHABLE;
        };
    }

    // ---- Children ----

    /**
     * Creates the child for a join whose admission ended in its favor, and exposes it. Event loop.
     *
     * @param offerCheck the offer check's result, null for an authenticated join
     */
    private void createChild(Admission admission, OfferCheckResult offerCheck) {
        Join join = admission.join;
        Bound settings = admission.settings;
        NetherNetEngine current = engine.current();
        if (current == null) {
            admission.release();
            refuse(join, JoinRefusal.CAPACITY);
            return;
        }
        boolean relayed = settings.answers().relayed();
        RemoteCandidateIntake intake = relayed ? RemoteCandidateIntake.relayed() : RemoteCandidateIntake.server();
        AdmissionGate.Reservation reservation = admission.reservation;
        ServerJoin negotiation = new ServerJoin(join, settings.answers(), intake,
                announcement(join, settings, advertised), reservation::handshakeDone);
        RemoteAddressTracker tracker = new RemoteAddressTracker(join.source(), config.isPreferProxiedAddress());

        NetherNetChildChannel child = new NetherNetChildChannel(this, join.source().address(), localAddress, negotiation);
        child.setMaxOutboundMessageSize(admission.maxMessageSize);
        child.attr(NetherNetChildChannel.CONNECTION_ID).set(negotiation.id());
        Join.Authenticated authenticated = join.authenticated();
        if (authenticated == null) {
            child.setOfferCheck(offerCheck);
        } else {
            child.installBinding(authenticated.binding());
        }
        child.trackAddress(tracker, intake, relayed ? settings.iceEndpoint() : null);
        live.add(child);
        child.closeFuture().addListener(future -> negotiation.closed());
        // Capacity follows the peer's teardown, not the close call: a handshake that closes stays
        // pending until its peer is gone, or join and close could pile up native teardown.
        child.termination().whenComplete((ignored, failure) -> {
            live.remove(child);
            admission.release();
        });

        try {
            child.openPeer(current, peerConfig(admission.from.source, settings, authenticated));
        } catch (Exception | LinkageError e) {
            log.error("Failed to create the peer for join {}: {}", negotiation.id(), e.toString(), e);
            negotiation.refuse(JoinRefusal.INTERNAL);
            child.close();
            return;
        }
        negotiation.attach(child, eventLoop());

        int handshakeTimeoutSeconds = config.getServerRtcHandshakeTimeoutSeconds();
        ScheduledFuture<?> handshakeTimeout = eventLoop().schedule(() -> {
            if (child.isOpen() && !child.isActive()) {
                child.connectionFailed(NetherConnectionFailure.HANDSHAKE_TIMEOUT);
                negotiation.timedOut(handshakeTimeoutSeconds);
                child.close();
            }
        }, handshakeTimeoutSeconds, TimeUnit.SECONDS);
        // Any close makes the timeout obsolete, or a connection that ends within it would get a
        // bogus warning.
        child.closeFuture().addListener(future -> handshakeTimeout.cancel(false));

        // Negotiate only once registered: initializer failure, timeout and disconnect all close
        // through the child, which owns the peer.
        child.pipeline().addLast(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) {
                negotiation.negotiate();
            }
        });

        if (authenticated != null) {
            try {
                authenticated.created().accept(child);
            } catch (RuntimeException e) {
                log.warn("The source of join {} failed to take its child, closing it: {}", negotiation.id(),
                        e.toString());
                negotiation.refuse(JoinRefusal.INTERNAL);
                child.close();
                return;
            }
        }
        // An authenticated join was attested by its source, if at all, not by this endpoint.
        boolean attested = offerCheck != null && offerCheck.attested();
        serverMetrics(metrics -> metrics.connectionAccepted(join.remoteNetworkId(), attested));
        pipeline().fireChannelRead(child);
        if (!open) {
            // Closed while this join was set up, after doClose went through the children.
            child.close();
        }
    }

    private PeerConfig peerConfig(JoinSource source, Bound settings, Join.Authenticated authenticated) {
        return new PeerConfig(PeerConfig.Role.ANSWERER,
                NetherNetChannel.iceServers(config.getIceServers(), source.iceServers()),
                config.getCandidatePolicy(), settings.iceEndpoint(),
                authenticated == null ? null : authenticated.identity(),
                authenticated == null ? null : authenticated.credentials());
    }

    /**
     * What a join's answer announces: the advertised addresses (as relays behind PROXY protocol
     * relays), or else the fresh STUN mappings and the address the player typed, when the source
     * knows it.
     */
    private static CandidateAnnouncement announcement(Join join, Bound settings, List<InetSocketAddress> advertised) {
        int icePort = settings.answers().icePort();
        Optional<InetSocketAddress> typed = Optional.empty();
        if (join.addressed() instanceof Join.Addressed addressed) {
            typed = CandidateAnnouncement.typedEndpoint(addressed.host(), addressed.viaTrustedProxy(), icePort,
                    addressed.localPort() == icePort);
        }
        List<InetSocketAddress> discovered = settings.discovery() == null ? List.of() : settings.discovery().fresh();
        return CandidateAnnouncement.of(advertised, settings.answers().relayed(), discovered, typed);
    }

    private void serverMetrics(Consumer<NetherServerMetrics> event) {
        NetherServerMetrics metrics = config.getServerMetrics();
        if (metrics == null) {
            return;
        }
        try {
            event.accept(metrics);
        } catch (RuntimeException e) {
            log.debug("NetherNet server metrics failed", e);
        }
    }

    AdmissionGate gate() {
        return gate;
    }

    @Override
    protected void doBeginRead() {
        // A server channel reads nothing itself.
    }

    @Override
    protected SocketAddress localAddress0() {
        return this.localAddress;
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return true;
    }

    @Override
    public ChannelConfig config() {
        return config;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public boolean isActive() {
        return isOpen() && localAddress0() != null;
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }
}

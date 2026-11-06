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
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AdvertisedAddresses;
import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateAnnouncement;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidateIntake;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSink;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A NetherNet server endpoint. It takes joins from any number of {@link JoinSource}s (HTTP, Xbox
 * RPC, LAN discovery), admits each through one admission stage, and emits a
 * {@link NetherNetChildChannel} per admitted join, like any Netty server channel.
 *
 * <p><b>Sources.</b> {@link #addSource} adds a source before or after bind; it starts once the
 * endpoint is bound. {@link #removeSource} stops and closes one. Neither touches connected
 * players, since every join replies through its own {@link io.github.sendablemetatype.netty.channel.nethernet.signaling.ReplyPath}.
 * A source is started once; to restart one, remove it and add a new instance. Closing the
 * endpoint closes its sources.
 *
 * <p><b>Admission.</b> Every join passes the same stage, whatever its source, on this channel's
 * event loop: the per-address throttle ({@link NetherChannelOption#NETHER_SERVER_THROTTLE}), one
 * budget for sessions and pending handshakes shared by all sources
 * ({@link NetherChannelOption#NETHER_SERVER_MAX_SESSIONS},
 * {@link NetherChannelOption#NETHER_SERVER_MAX_PENDING_JOINS}), and the offer's identity check
 * ({@link NetherChannelOption#NETHER_SERVER_OFFER_CHECK}) under the source's {@link OfferPolicy}.
 * The check runs off the event loop with a deadline. A refused join gets a typed
 * {@link JoinRefusal} through its reply path and creates nothing. A join holds its share of the
 * budget and its throttle entry until its child's {@linkplain NetherNetChannel#termination()
 * termination} completed.
 *
 * <p><b>Identity.</b> One {@link ServerIdentity}, set with
 * {@link NetherChannelOption#NETHER_SERVER_IDENTITY}, signs the answers of every source. Binding
 * fails without one.
 *
 * <p><b>ICE.</b> By default every peer gathers its host candidates on one UDP port at the address
 * and port this channel binds (port 0 picks a free one). With
 * {@link NetherChannelOption#NETHER_SERVER_SHARED_ICE_PORT} false, each session takes ports of
 * its own from {@link NetherChannelOption#NETHER_CANDIDATE_POLICY}'s window instead.
 *
 * <p>The child is exposed as soon as its join is admitted. It owns its engine peer from creation
 * and negotiates in its own initializer once registered. The engine is either the caller's, which
 * this channel never closes, or one this channel creates when it binds and closes with itself.
 */
public class NetherNetServerChannel extends AbstractServerChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetServerChannel.class);
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    /**
     * How long bind waits for the engine to open the shared ICE port. Netty's bind completes
     * when doBind returns, and a taken port has to fail it, so this is the one place the event
     * loop waits for the engine.
     */
    private static final long ENDPOINT_OPEN_TIMEOUT_SECONDS = 10;

    private final DefaultNetherServerChannelConfig config;
    private final EngineRef engine;
    private final Supplier<? extends ExecutorService> checkExecutors;
    private final AdmissionGate gate = new AdmissionGate();
    /** Children whose termination has not completed yet. */
    private final Set<NetherNetChildChannel> live = ConcurrentHashMap.newKeySet();

    private final Object sourcesLock = new Object();
    // Guarded by sourcesLock.
    private final Map<JoinSource, Registration> sources = new LinkedHashMap<>();
    private boolean sourcesStarted;

    private volatile InetSocketAddress localAddress;
    private volatile Bound bound;
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
        this.engine = engine;
        this.checkExecutors = checkExecutors;
        this.config = new DefaultNetherServerChannelConfig(this);
    }

    /**
     * What bind decided, fixed for the endpoint's life.
     *
     * @param answers what every join answers with
     * @param iceEndpoint the shared ICE port, or null when each session takes its own
     * @param advertised the advertised addresses, resolved
     * @param trustedProxies the proxies trusted in front of signaling and the ICE port
     * @param offerCheck checks every join's identity
     * @param checkExecutor runs the checks, off the event loop
     */
    private record Bound(ServerJoin.Settings answers, IceEndpoint iceEndpoint, List<InetSocketAddress> advertised,
                         TrustedProxies trustedProxies, OfferCheck offerCheck, ExecutorService checkExecutor) {
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
     * Adds a join source. A bound endpoint starts it at once, otherwise binding starts it.
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
     * Stops and closes a source. Its players stay connected; joins it delivered that are still
     * being admitted are refused.
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
        // Names are looked up once, here; literals return at once.
        List<InetSocketAddress> advertised = AdvertisedAddresses.resolve(config.getAdvertisedAddresses());
        if (relayed && advertised.isEmpty()) {
            log.warn("PROXY protocol relays front the ICE port, but no advertised addresses name them: answers "
                    + "name this host, and players can only connect if something in front of signaling rewrites them");
        }

        NetherNetEngine current = engine.get();
        IceEndpoint iceEndpoint = null;
        InetSocketAddress local = address;
        if (shared) {
            int port = address.getPort() != 0 ? address.getPort() : freeUdpPort(address);
            InetAddress host = address.getAddress();
            List<InetAddress> addresses = host == null || host.isAnyLocalAddress() ? List.of() : List.of(host);
            iceEndpoint = await(current.openEndpoint(new EndpointConfig(port, addresses)));
            if (relayed) {
                iceEndpoint.proxyRelays(proxies.proxyProtocol().entries());
            }
            local = new InetSocketAddress(host, port);
        }

        OfferCheck offerCheck = config.getOfferCheck();
        ExecutorService checkExecutor = checkExecutors.get();
        // Off the event loop, and off the bounded check executor, whose slots belong to joins: the
        // first join should not wait for the issuers' keys.
        Thread.ofVirtual().name("nethernet-issuer-keys").start(offerCheck::prefetch);
        ServerJoin.Settings answers = new ServerJoin.Settings(identity, config.isInferPeerCandidates(),
                iceEndpoint == null ? 0 : iceEndpoint.port(), relayed);
        this.bound = new Bound(answers, iceEndpoint, advertised, proxies, offerCheck, checkExecutor);
        this.localAddress = local;
        log.info("NetherNet endpoint bound to {} ({}), server identity SHA-256 {}", local,
                shared ? "shared ICE port" : "ICE ports per session", identity.fingerprint());

        List<Registration> toStart;
        synchronized (sourcesLock) {
            sourcesStarted = true;
            toStart = List.copyOf(sources.values());
        }
        toStart.forEach(Registration::start);
    }

    /**
     * A free UDP port for a bind that asked for any. The engine binds the shared port lazily, so it
     * needs a fixed one; it checks again that the port is free before the bind succeeds.
     */
    private static int freeUdpPort(InetSocketAddress address) throws IOException {
        try (DatagramSocket probe = new DatagramSocket(new InetSocketAddress(address.getAddress(), 0))) {
            return probe.getLocalPort();
        }
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        try {
            return stage.toCompletableFuture().get(ENDPOINT_OPEN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
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
        }
        // The shared port closes once no peer uses it anymore, then an owned engine releases its
        // threads; a shared engine stays with its owner.
        IceEndpoint iceEndpoint = settings == null ? null : settings.iceEndpoint();
        CompletableFuture.allOf(terminations.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> iceEndpoint == null ? CompletableFuture.completedFuture(null)
                        : iceEndpoint.close())
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
            log.debug("Refused join {} from {}: throttled", Long.toUnsignedString(join.connectionId()), throttled);
            serverMetrics(metrics -> metrics.joinThrottled(throttled));
            refuse(join, JoinRefusal.CAPACITY);
            return;
        }
        AdmissionGate.Reservation reservation = gate.reserve(config.getMaxSessions(), config.getMaxPendingJoins());
        if (reservation == null) {
            if (throttled != null) {
                throttle.closed(throttled);
            }
            log.debug("Refused join {}: the endpoint is at capacity", Long.toUnsignedString(join.connectionId()));
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
                    log.debug("Could not refuse join {}: {}", Long.toUnsignedString(join.connectionId()),
                            failure.toString());
                }
            });
        } catch (RuntimeException e) {
            log.debug("Could not refuse join {}: {}", Long.toUnsignedString(join.connectionId()), e.toString());
        }
        join.reply().finished();
    }

    /**
     * One join from the moment it holds its share of the budget until it becomes a child or is
     * refused. Event loop only, apart from what the check's own thread completes.
     */
    private final class Admission {
        private final Registration from;
        private final Join join;
        private final String id;
        private final Bound settings;
        private final AdmissionGate.Reservation reservation;
        private final NetherServerThrottle throttle;
        private final InetSocketAddress throttled;
        private int maxMessageSize;
        private CompletableFuture<OfferCheckResult> check;
        private ScheduledFuture<?> deadline;
        private boolean settled;

        private Admission(Registration from, Join join, Bound settings, AdmissionGate.Reservation reservation,
                          NetherServerThrottle throttle, InetSocketAddress throttled) {
            this.from = from;
            this.join = join;
            this.id = Long.toUnsignedString(join.connectionId());
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
            join.reply().abandoned().whenComplete((ignored, failure) -> onLoop(this::abandoned));
            check = settings.offerCheck().checkAsync(join.offer(), settings.checkExecutor());
            // A check that finishes after this admits nothing: its stage is already complete.
            deadline = eventLoop().schedule(() -> {
                check.completeExceptionally(new TimeoutException());
            }, config.getOfferCheckTimeoutMillis(), TimeUnit.MILLISECONDS);
            check.whenComplete((result, failure) -> onLoop(() -> checked(result, failure)));
        }

        private void checked(OfferCheckResult result, Throwable failure) {
            if (settled) {
                return;
            }
            deadline.cancel(false);
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
            settled = true;
            createChild(this, result);
        }

        /** The client left before the check finished: nothing to answer, nothing to create. */
        private void abandoned() {
            if (settled) {
                return;
            }
            settled = true;
            check.cancel(true);
            deadline.cancel(false);
            release();
            join.reply().finished();
        }

        private void fail(JoinRefusal refusal) {
            settled = true;
            release();
            refuse(join, refusal);
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
                if (!settled) {
                    settled = true;
                    release();
                    join.reply().finished();
                }
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

    /** Creates the child for an admitted join and exposes it. Event loop. */
    private void createChild(Admission admission, OfferCheckResult offerCheck) {
        Join join = admission.join;
        Bound settings = admission.settings;
        NetherNetEngine current = engine.current();
        if (current == null) {
            admission.fail(JoinRefusal.CAPACITY);
            return;
        }
        boolean relayed = settings.answers().relayed();
        RemoteCandidateIntake intake = relayed ? RemoteCandidateIntake.relayed() : RemoteCandidateIntake.server();
        AdmissionGate.Reservation reservation = admission.reservation;
        ServerJoin negotiation = new ServerJoin(join, settings.answers(), intake, announcement(join, settings),
                reservation::handshakeDone);
        RemoteAddressTracker tracker = new RemoteAddressTracker(join.source(), config.isPreferProxiedAddress());

        NetherNetChildChannel child = new NetherNetChildChannel(this, join.source().address(), localAddress, negotiation);
        child.setMaxOutboundMessageSize(admission.maxMessageSize);
        child.attr(NetherNetChildChannel.CONNECTION_ID).set(negotiation.id());
        child.setOfferCheck(offerCheck);
        child.trackAddress(tracker, intake, relayed ? settings.iceEndpoint() : null);
        live.add(child);
        child.closeFuture().addListener(future -> {
            reservation.handshakeDone();
            negotiation.closed();
        });
        // Capacity follows the peer's teardown, not the close call.
        child.termination().whenComplete((ignored, failure) -> {
            live.remove(child);
            admission.release();
        });

        try {
            child.openPeer(current, peerConfig(admission.from.source, settings));
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

        serverMetrics(metrics -> metrics.connectionAccepted(join.remoteNetworkId(), offerCheck.attested()));
        pipeline().fireChannelRead(child);
        if (!open) {
            // Closed while this join was set up, after doClose went through the children.
            child.close();
        }
    }

    private PeerConfig peerConfig(JoinSource source, Bound settings) {
        return new PeerConfig(PeerConfig.Role.ANSWERER,
                NetherNetChannel.iceServers(config.getIceServers(), source.iceServers()),
                config.getCandidatePolicy(), settings.iceEndpoint(), null, null);
    }

    /**
     * What a join's answer announces: the advertised addresses (as relays behind PROXY protocol
     * relays), or else the address the player typed, when the source knows it.
     */
    private static CandidateAnnouncement announcement(Join join, Bound settings) {
        int icePort = settings.answers().icePort();
        Optional<InetSocketAddress> typed = Optional.empty();
        if (join.reached() != null) {
            SourcedAddress source = join.source();
            boolean viaProxy = source.proxied() || settings.trustedProxies().isProxy(source.address());
            typed = CandidateAnnouncement.typedEndpoint(join.reached().host(), viaProxy, icePort,
                    icePort != 0 && join.reached().localPort() == icePort);
        }
        // STUN discovery (ruling G-6, item 1) would add its fresh mappings here.
        return CandidateAnnouncement.of(settings.advertised(), settings.answers().relayed(), List.of(), typed);
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

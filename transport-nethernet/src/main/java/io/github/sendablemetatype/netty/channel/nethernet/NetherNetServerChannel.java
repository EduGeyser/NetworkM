package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressTracker;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherServerChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerJoinFilter;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerThrottle;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EndpointConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AdvertisedAddresses;
import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateAnnouncement;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidateIntake;
import io.github.sendablemetatype.netty.channel.nethernet.ice.StunObserver;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinAlreadySettledException;
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
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
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
import java.util.function.BiConsumer;
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
 * event loop: one pending join per network ID across the sources that ask for it
 * ({@link JoinSource#oneJoinPerNetworkId()}, HTTP signaling), the per-address throttle
 * ({@link NetherChannelOption#NETHER_SERVER_THROTTLE}), one
 * budget for sessions and pending handshakes shared by all sources
 * ({@link NetherChannelOption#NETHER_SERVER_MAX_SESSIONS},
 * {@link NetherChannelOption#NETHER_SERVER_MAX_PENDING_JOINS}), and the offer's identity check
 * ({@link NetherChannelOption#NETHER_SERVER_OFFER_CHECK}) under the source's {@link OfferPolicy},
 * then the host's join filter ({@link NetherChannelOption#NETHER_SERVER_JOIN_FILTER}), if one is
 * set. The check and the filter run off the event loop under one deadline. A join whose source
 * authenticated the client itself ({@link Join.Authenticated}, NXS) skips the check and brings the
 * binding that decides its login; the throttle, the budget and the filter apply to it all the
 * same. A refused join gets a typed
 * {@link JoinRefusal} through its reply path and creates nothing. A join holds its share of the
 * budget and its throttle entry until its child's {@linkplain NetherNetChannel#termination()
 * termination} completed. A source may also open peers on the shared ICE port that carry no player
 * ({@link SharedIcePort#openPeer}, NXS connectivity checks): no child, no session, one pending
 * handshake each until the peer's termination completed.
 *
 * <p><b>Identity.</b> One {@link ServerIdentity}, set with
 * {@link NetherChannelOption#NETHER_SERVER_IDENTITY}, signs the answers of every source. Binding
 * fails without one.
 *
 * <p><b>ICE.</b> By default every peer gathers its host candidates on one UDP port at the address
 * and port this channel binds (port 0 picks a free one). A taken port fails the bind. The engine
 * sets the port up after bind returned, and the sources start once it is set up, which
 * {@link #ready()} reports (with what that guarantees); a port that cannot be set up then closes
 * the endpoint, with the error logged. With
 * {@link NetherChannelOption#NETHER_SERVER_SHARED_ICE_PORT} false, each session takes ports of
 * its own from {@link NetherChannelOption#NETHER_CANDIDATE_POLICY}'s window instead. On the shared
 * port, {@link NetherChannelOption#NETHER_SERVER_STUN_SERVERS} keeps STUN bindings alive from it and
 * answers announce the fresh mappings.
 *
 * <p>The child is exposed as soon as its join is admitted. It owns its engine peer from creation
 * and negotiates in its own initializer once registered. The engine is either the caller's, which
 * this channel never closes, or one this channel creates when it binds and closes with itself.
 *
 * <p><b>Close.</b> The close future completes before the endpoint has let go of everything:
 * peers, the ICE port and an owned engine close afterwards, on the engine's threads.
 * {@link #termination()} completes once they did, so a host that binds the same address again, as
 * on a reload, waits for it.
 */
public class NetherNetServerChannel extends AbstractServerChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetServerChannel.class);
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);

    private final DefaultNetherServerChannelConfig config;
    private final EngineRef engine;
    private final Supplier<? extends ExecutorService> checkExecutors;
    private final AdvertisedLookup names;
    private final AdmissionGate gate = new AdmissionGate();
    private final PendingNetworkIds pendingNetworkIds = new PendingNetworkIds();
    /** Children whose termination has not completed yet. */
    private final Set<NetherNetChildChannel> live = ConcurrentHashMap.newKeySet();
    /** Peers without a child that sources opened on the shared port, until their termination completed. */
    private final Set<ChildlessPeer> childless = ConcurrentHashMap.newKeySet();
    /** Joins in the admission stage, which closing the endpoint or removing their source ends. */
    private final Set<Admission> admitting = ConcurrentHashMap.newKeySet();
    /** The one source's hold on the shared port's held checks, if any source took them. */
    private final AtomicReference<HeldCheckClaim> heldChecks = new AtomicReference<>();
    private final AtomicBoolean joinFilterWarned = new AtomicBoolean();

    private final Object sourcesLock = new Object();
    // Guarded by sourcesLock.
    private final Map<JoinSource, Registration> sources = new LinkedHashMap<>();
    private boolean sourcesStarted;
    /**
     * Removed sources still letting go of their ports, one stage each until it completes. Added to
     * under sourcesLock with the removal, so the close sees every source either here or in sources.
     */
    private final Set<CompletableFuture<Void>> removedSources = ConcurrentHashMap.newKeySet();

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
    /** Completes once the endpoint takes joins; see {@link #ready()}. */
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    /** Completes once the closed endpoint released everything; see {@link #termination()}. */
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
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
     * yet are refused at once and give back their share of the budget. The endpoint's
     * {@link #termination()} still waits for the source's own until it completes.
     *
     * @param source the source
     * @return whether the endpoint had it
     */
    public boolean removeSource(JoinSource source) {
        Registration registration;
        CompletableFuture<Void> released = new CompletableFuture<>();
        synchronized (sourcesLock) {
            registration = sources.remove(source);
            if (registration == null) {
                return false;
            }
            // The endpoint's termination waits for it, also when the endpoint closes meanwhile
            removedSources.add(released);
        }
        released.whenComplete((ignored, failure) -> removedSources.remove(released));
        registration.removed = true;
        // Before the source closes, so the refusals can still reach their clients.
        withdrawAdmissions(registration);
        registration.releaseHeldChecks();
        closeQuietly(source);
        settled(source::termination).whenComplete((ignored, failure) -> released.complete(null));
        for (ChildlessPeer peer : childless) {
            if (peer.from == registration) {
                peer.close();
            }
        }
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

        @Override
        public Optional<Peer> openPeer(IceCredentials credentials, DtlsIdentity identity, PeerListener listener) {
            Objects.requireNonNull(credentials, "credentials");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(listener, "listener");
            NetherNetEngine current = engine.current();
            if (current == null || !open || from.removed) {
                return Optional.empty();
            }
            AdmissionGate.Reservation slot = gate.reservePending(config.getMaxPendingJoins());
            if (slot == null) {
                return Optional.empty();
            }
            Peer peer;
            try {
                peer = current.createPeer(new PeerConfig(PeerConfig.Role.ANSWERER, List.of(),
                        config.getCandidatePolicy(), endpoint, identity, credentials), listener);
            } catch (IllegalStateException engineClosed) {
                slot.release();
                return Optional.empty();
            } catch (RuntimeException e) {
                slot.release();
                throw e;
            }
            ChildlessPeer held = new ChildlessPeer(peer, slot, from);
            childless.add(held);
            if (!open || from.removed) {
                // Closed or removed meanwhile, after the close that would have found it.
                held.close();
                return Optional.empty();
            }
            return Optional.of(held);
        }
    }

    /** A peer without a child, which holds its pending handshake until its termination completed. */
    private final class ChildlessPeer implements Peer {
        private final Peer peer;
        private final AdmissionGate.Reservation slot;
        private final Registration from;
        private final AtomicBoolean closing = new AtomicBoolean();

        private ChildlessPeer(Peer peer, AdmissionGate.Reservation slot, Registration from) {
            this.peer = peer;
            this.slot = slot;
            this.from = from;
        }

        @Override
        public CompletionStage<String> answer(String offer) {
            return peer.answer(offer);
        }

        @Override
        public CompletionStage<String> offer() {
            return peer.offer();
        }

        @Override
        public CompletionStage<Void> applyAnswer(String answer) {
            return peer.applyAnswer(answer);
        }

        @Override
        public void addRemoteCandidate(String candidate, String mid) {
            peer.addRemoteCandidate(candidate, mid);
        }

        @Override
        public CompletionStage<String> gatheredLocalDescription() {
            return peer.gatheredLocalDescription();
        }

        @Override
        public DataLink openChannel(ChannelParameters parameters) {
            return peer.openChannel(parameters);
        }

        @Override
        public CompletionStage<TransportMetrics> metrics() {
            return peer.metrics();
        }

        @Override
        public CompletionStage<Void> close() {
            CompletionStage<Void> terminated = peer.close();
            if (closing.compareAndSet(false, true)) {
                terminated.whenComplete((ignored, failure) -> {
                    childless.remove(this);
                    slot.release();
                });
            }
            return terminated;
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

    // ---- Announced addresses ----

    /**
     * The advertised addresses answers announce, as resolved at bind: the IP literals of
     * {@link NetherChannelOption#NETHER_SERVER_ADVERTISED_ADDRESSES} from bind on, then every entry
     * once its names resolved. Ports stay as configured, so port 0 still stands for the media port.
     * Unlike the option, this never holds an unresolved name. Safe to call from any thread.
     *
     * @return an immutable snapshot, empty before bind, once the endpoint is closed, and when no
     *         addresses are configured
     */
    public List<InetSocketAddress> advertisedAddresses() {
        return open ? advertised : List.of();
    }

    /**
     * The public addresses STUN discovery currently finds
     * ({@link NetherChannelOption#NETHER_SERVER_STUN_SERVERS}), as answers announce them. A mapping
     * on another port than the shared ICE port is followed by its address on the ICE port, the twin
     * answers announce too. Refreshed once per second; a mapping drops out after
     * {@link NetherChannelOption#NETHER_SERVER_STUN_MAX_AGE} without an answer. Safe to call from any
     * thread.
     *
     * @return an immutable snapshot, empty without discovery, before the ICE port is open and a
     *         server answered, and once the endpoint is closed
     */
    public List<InetSocketAddress> discoveredAddresses() {
        Bound settings = bound;
        if (!open || settings == null || settings.discovery() == null) {
            return List.of();
        }
        return settings.discovery().fresh();
    }

    // ---- Bind and close ----

    /**
     * Completes once the endpoint takes joins: it is bound, the engine has set up its shared ICE
     * port, and its sources have started. The bind future completes before that, once the bind
     * found the port free; the engine sets the port up afterwards, on its own threads. With ports
     * per session there is no port to set up, and this completes with the bind.
     *
     * <p>For the port this guarantees only that it was free when the bind and then the engine
     * probed it, and that the engine's endpoint for it exists. It does not hold the port: the
     * WebRTC engine binds the UDP socket on each local address when the first peer gathers there.
     * A process that takes the port in between costs the joins that gather while it holds it their
     * host candidates on that address, and nothing here reports it.
     *
     * <p>Fails with the reason the endpoint did not get there: the bind's failure, the reason the
     * ICE port did not open (such as a {@link java.net.BindException} when another process took
     * the port after the bind found it free), or a {@link ClosedChannelException} when the endpoint
     * was closed first. After a failure that follows the bind, the endpoint closes itself.
     * Actions that depend on this stage run on the endpoint's event loop and must not block.
     *
     * @return the stage
     */
    public CompletionStage<Void> ready() {
        return ready.minimalCompletionStage();
    }

    /**
     * Completes once this endpoint is closed and has released everything it held: the termination
     * of every child and of every peer a source opened, the ports of its sources
     * ({@link JoinSource#termination()}), also of sources removed before the close that have not let
     * go of them yet, its shared ICE port, and the engine it owns, if it owns one. A host that binds the same address again, as on a reload, waits for this first: the
     * close future completes before any of it. Never completes exceptionally; a part that never
     * finishes its teardown keeps it pending, so wait with a timeout.
     *
     * @return the stage
     */
    public CompletionStage<Void> termination() {
        return termination.minimalCompletionStage();
    }

    @Override
    protected void doBind(SocketAddress requested) throws Exception {
        try {
            bindEndpoint(requested);
        } catch (Throwable failure) {
            ready.completeExceptionally(failure);
            throw failure;
        }
    }

    private void bindEndpoint(SocketAddress requested) throws Exception {
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
                switch (reasonOf(e)) {
                    case Exception reason -> throw reason;
                    case Error reason -> throw reason;
                    default -> throw e;
                }
            }
            takeJoins(plan, endpoint);
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
        // A copy, so advertisedAddresses() hands out an immutable list whatever the lookup returned.
        advertised = List.copyOf(resolved);
    }

    /** The shared ICE port opened, or failed to, after bind returned. Event loop. */
    private void opened(BindPlan plan, IceEndpoint endpoint, Throwable failure) {
        if (!open) {
            // doClose closes the port, now that it opened.
            return;
        }
        if (failure != null) {
            Throwable reason = reasonOf(failure);
            log.error("The NetherNet endpoint at {} could not open its ICE port, closing it: {}", localAddress,
                    reason.toString());
            ready.completeExceptionally(reason);
            close();
            return;
        }
        try {
            takeJoins(plan, endpoint);
        } catch (RuntimeException e) {
            log.error("The NetherNet endpoint at {} could not set up its ICE port, closing it: {}", localAddress,
                    e.toString());
            ready.completeExceptionally(e);
            close();
        }
    }

    /**
     * Why opening the ICE port failed, without the wrappers of the stage and of the engine's
     * thread, which cannot throw a checked exception.
     */
    private static Throwable reasonOf(Throwable failure) {
        Throwable reason = failure;
        while ((reason instanceof CompletionException || reason instanceof UncheckedIOException)
                && reason.getCause() != null) {
            reason = reason.getCause();
        }
        return reason;
    }

    /** The ICE port is open, if it is shared: the endpoint takes joins from now on. Event loop. */
    private void takeJoins(BindPlan plan, IceEndpoint endpoint) {
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
        ready.complete(null);
    }

    @Override
    protected void doClose() throws Exception {
        ready.completeExceptionally(new ClosedChannelException());
        List<Registration> closing;
        List<CompletableFuture<Void>> stillRemoving;
        synchronized (sourcesLock) {
            open = false;
            closing = List.copyOf(sources.values());
            sources.clear();
            stillRemoving = List.copyOf(removedSources);
        }
        for (Registration registration : closing) {
            registration.removed = true;
            registration.releaseHeldChecks();
        }
        // Before the sources close, so the refusals can still reach their clients. Without this,
        // a check the executor drops below would keep its share and throttle entry for good.
        List.copyOf(admitting).forEach(Admission::withdraw);
        // What termination waits for: the sources' ports, removed sources included, and the chain
        // from the peers to the engine.
        List<CompletableFuture<Void>> released = new ArrayList<>(stillRemoving);
        for (Registration registration : closing) {
            closeQuietly(registration.source);
            released.add(settled(registration.source::termination));
        }

        List<CompletableFuture<Void>> terminations = new ArrayList<>();
        for (NetherNetChildChannel child : live) {
            terminations.add(settled(child::termination));
            child.close();
        }
        for (ChildlessPeer peer : childless) {
            terminations.add(settled(peer::close));
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
        released.add(CompletableFuture.allOf(terminations.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> port == null ? CompletableFuture.<IceEndpoint>completedFuture(null)
                        : port.handle((endpoint, failure) -> endpoint))
                .thenCompose(endpoint -> endpoint == null ? CompletableFuture.<Void>completedFuture(null)
                        : settled(endpoint::close))
                .thenCompose(ignored -> engine.release())
                .toCompletableFuture());
        CompletableFuture.allOf(released.toArray(CompletableFuture[]::new))
                .whenComplete((ignored, failure) -> termination.complete(null));
    }

    /**
     * A teardown stage that completes, never exceptionally, once the teardown ended either way,
     * also when asking for it throws: a failed part must not hold up the rest of the close.
     */
    private static CompletableFuture<Void> settled(Supplier<? extends CompletionStage<Void>> teardown) {
        try {
            return teardown.get().toCompletableFuture().handle((ignored, failure) -> {
                if (failure != null) {
                    log.debug("A NetherNet endpoint teardown step failed: {}", failure.toString());
                }
                return null;
            });
        } catch (RuntimeException e) {
            log.debug("A NetherNet endpoint teardown step failed: {}", e.toString());
            return CompletableFuture.completedFuture(null);
        }
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
    private void admit(Registration from, Join offered) {
        Bound settings = bound;
        if (!open || settings == null || from.removed) {
            refuse(offered, JoinRefusal.CAPACITY);
            return;
        }
        Join join = offered;
        if (from.source.oneJoinPerNetworkId()) {
            join = pendingNetworkIds.hold(offered);
            if (join == null) {
                log.debug("Refused join {}: another join for network {} is pending", offered.connectionId(),
                        offered.remoteNetworkId());
                refuse(offered, JoinRefusal.DUPLICATE);
                return;
            }
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
        sendRefusal(join, refusal);
        join.reply().finished();
    }

    /**
     * Sends a join its refusal and reports it once the source took it, unless the join was
     * settled before: whoever settled it reported that, so one join counts once.
     */
    private void sendRefusal(Join join, JoinRefusal refusal) {
        CompletionStage<Void> sent;
        try {
            sent = join.reply().refuse(refusal);
        } catch (RuntimeException e) {
            log.debug("Could not refuse join {}: {}", join.connectionId(), e.toString());
            serverMetrics(metrics -> metrics.joinRefused(refusal.httpStatus()));
            return;
        }
        sent.whenComplete((ignored, failure) -> {
            if (JoinAlreadySettledException.settledBefore(failure)) {
                log.debug("Did not refuse join {}: it was settled before ({})", join.connectionId(),
                        failure.toString());
                return;
            }
            serverMetrics(metrics -> metrics.joinRefused(refusal.httpStatus()));
            if (failure != null) {
                log.debug("Could not refuse join {}: {}", join.connectionId(), failure.toString());
            }
        });
    }

    /**
     * One join from the moment it holds its share of the budget until it becomes a child or is
     * refused. Its steps run on the event loop, and its off-loop steps (the identity check, then the
     * host's join filter) share one deadline. It ends exactly once, whoever ends it: a step, its
     * deadline, its client leaving, or {@link #withdraw} when the endpoint closes or its source is
     * removed, which can happen on any thread.
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
        /** The off-loop step running now, which the deadline times out and a withdrawal cancels. */
        private volatile CompletableFuture<?> step;
        private volatile ScheduledFuture<?> deadline;
        /** When the check started and how long it and the filter may take, on the event loop's clock. */
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
                fail(JoinRefusal.BAD_OFFER);
                return;
            }
            NetherServerJoinFilter filter = config.getJoinFilter();
            if (join.authenticated() != null && filter == null) {
                // The source authenticated the client, and its binding decides the login: no offer
                // check, and with no filter to ask, nothing to wait for.
                admit(null);
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
            // A step that finishes after this admits nothing: its stage is already complete.
            deadline = eventLoop().schedule(() -> {
                CompletableFuture<?> current = step;
                if (current != null) {
                    current.completeExceptionally(new TimeoutException());
                }
            }, timeoutNanos, TimeUnit.NANOSECONDS);
            if (join.authenticated() != null) {
                filter(filter, null);
            } else {
                await(settings.offerCheck().checkAsync(join.offer(), settings.checkExecutor()), this::checked);
            }
        }

        /** Waits for an off-loop step, whose result then comes back on the event loop. */
        private <T> void await(CompletableFuture<T> started, BiConsumer<T, Throwable> next) {
            step = started;
            if (settled.get()) {
                // Withdrawn on another thread while the step was being set up.
                started.cancel(true);
                ScheduledFuture<?> timer = deadline;
                if (timer != null) {
                    timer.cancel(false);
                }
                return;
            }
            started.whenComplete((result, failure) -> onLoop(() -> next.accept(result, failure)));
        }

        private void checked(OfferCheckResult result, Throwable failure) {
            if (settled.get()) {
                return;
            }
            if (failure == null && late()) {
                log.debug("Refusing join {}: its identity check finished after its deadline", id);
                fail(JoinRefusal.NO_ANSWER_IN_TIME);
                return;
            }
            if (failure != null) {
                Throwable cause = unwrap(failure);
                log.debug("Refusing join {}: its identity check did not finish ({})", id, cause.toString());
                fail(stepRefusal(cause));
                return;
            }
            Optional<IdentityRefusal> refused = from.policy.refusal(result);
            if (refused.isPresent()) {
                // Diagnostics hold no token, and their text is sanitized for logs.
                log.info("Refused join {} from {}: {} ({}, {})", id, join.source().address(), refused.get(),
                        result.reason(), result.diagnostics());
                fail(refusalOf(refused.get(), result));
                return;
            }
            NetherServerJoinFilter filter = config.getJoinFilter();
            if (filter == null) {
                admit(result);
            } else {
                filter(filter, result);
            }
        }

        /** Asks the host's filter off the event loop, on the check executor. */
        private void filter(NetherServerJoinFilter filter, OfferCheckResult result) {
            Join.Addressed addressed = join.addressed();
            NetherServerJoinFilter.Request request = new NetherServerJoinFilter.Request(from.source,
                    join.remoteNetworkId(), join.source(), addressed == null ? null : addressed.host(),
                    addressed == null ? null : addressed.client(), result);
            CompletableFuture<Boolean> filtering;
            try {
                filtering = CompletableFuture.supplyAsync(() -> filter.allows(request), settings.checkExecutor());
            } catch (RejectedExecutionException full) {
                log.debug("Refusing join {}: no room to run the join filter", id);
                fail(JoinRefusal.CAPACITY);
                return;
            }
            await(filtering, (allowed, failure) -> filtered(result, allowed, failure));
        }

        private void filtered(OfferCheckResult result, Boolean allowed, Throwable failure) {
            if (settled.get()) {
                return;
            }
            if (failure == null && late()) {
                log.debug("Refusing join {}: the join filter answered after the deadline", id);
                fail(JoinRefusal.NO_ANSWER_IN_TIME);
                return;
            }
            if (failure != null) {
                Throwable cause = unwrap(failure);
                if (cause instanceof TimeoutException) {
                    log.debug("Refusing join {}: the join filter did not answer in time", id);
                } else if (!(cause instanceof RejectedExecutionException)) {
                    joinFilterFailed(id, cause);
                }
                fail(stepRefusal(cause));
                return;
            }
            if (!allowed) {
                log.debug("Refused join {} from {}: the join filter turned it away", id, join.source().address());
                fail(JoinRefusal.HOST_REFUSED);
                return;
            }
            admit(result);
        }

        /**
         * The deadline only fires once this loop gets to it: a loop that ran late can see a result
         * that came in past the deadline before the deadline fired.
         */
        private boolean late() {
            return eventLoop().ticker().nanoTime() - startedNanos >= timeoutNanos;
        }

        /** Creates the child, unless the endpoint or the source went away meanwhile. */
        private void admit(OfferCheckResult result) {
            if (!open || from.removed) {
                fail(JoinRefusal.CAPACITY);
            } else if (settle()) {
                createChild(this, result);
            }
        }

        /** The client left before its admission ended: nothing to answer, nothing to create. */
        private void abandoned() {
            if (settle()) {
                cancelStep();
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
                cancelStep();
                release();
                refuse(join, JoinRefusal.CAPACITY);
            }
        }

        /**
         * Withdraws the running step: a queued check or filter never runs, a running check is
         * interrupted, and a running filter's answer is ignored.
         */
        private void cancelStep() {
            CompletableFuture<?> pending = step;
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

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
    }

    /** What a join whose off-loop step did not finish gets: the filter follows the check's rules. */
    private static JoinRefusal stepRefusal(Throwable cause) {
        return switch (cause) {
            case TimeoutException _ -> JoinRefusal.NO_ANSWER_IN_TIME;
            case RejectedExecutionException _ -> JoinRefusal.CAPACITY;
            default -> JoinRefusal.INTERNAL;
        };
    }

    /**
     * A throwing filter is a host bug that every join would repeat, so it warns once and logs the
     * rest at debug.
     */
    private void joinFilterFailed(String id, Throwable cause) {
        if (joinFilterWarned.compareAndSet(false, true)) {
            log.warn("The NetherNet join filter failed, refusing join {} (further failures log at debug)", id, cause);
        } else {
            log.debug("The NetherNet join filter failed, refusing join {}: {}", id, cause.toString());
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

    /**
     * The refusal for an offer the source's policy turned away. An SDP the check could not read
     * is a bad offer whatever the policy wanted of its identity.
     */
    private static JoinRefusal refusalOf(IdentityRefusal refusal, OfferCheckResult result) {
        if (result.reason() == OfferCheckResult.Reason.MALFORMED_SDP) {
            return JoinRefusal.BAD_OFFER;
        }
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
                announcement(join, settings, advertised), reservation::handshakeDone,
                refusal -> sendRefusal(join, refusal));
        RemoteAddressTracker tracker = new RemoteAddressTracker(join.source(), config.isPreferProxiedAddress());

        NetherNetChildChannel child = new NetherNetChildChannel(this, localAddress, negotiation, tracker, intake,
                relayed ? settings.iceEndpoint() : null, join.addressed());
        child.setMaxOutboundMessageSize(admission.maxMessageSize);
        child.attr(NetherNetChildChannel.CONNECTION_ID).set(negotiation.id());
        Join.Authenticated authenticated = join.authenticated();
        if (authenticated == null) {
            child.setOfferCheck(offerCheck);
        } else {
            child.installBinding(authenticated.binding());
        }
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

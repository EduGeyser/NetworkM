package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherServerChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetServerSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetSignaling;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.channel.AbstractServerChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.EventLoop;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * NetherNet server channel: accepts connection offers arriving over a
 * {@link NetherNetServerSignaling} implementation and emits a
 * {@link NetherNetChildChannel} for each, like any Netty server channel.
 *
 * <p>The child is exposed as soon as the join is accepted. It owns its engine
 * peer from creation and negotiates in its own initializer once registered;
 * candidates the client trickles before the peer exists wait in a list of
 * {@value #MAX_EARLY_SIGNALS} per join. Only the engine interface appears here;
 * the engine is either the caller's, which this channel never closes, or one
 * this channel creates after its signaling bound and closes with itself.
 */
public class NetherNetServerChannel extends AbstractServerChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetServerChannel.class);
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    /** Far more signals than one join sends before its peer exists, so only a flood reaches it. */
    static final int MAX_EARLY_SIGNALS = 64;

    private final DefaultNetherServerChannelConfig config;
    private final EngineRef engine;
    private final NetherNetServerSignaling signaling;
    private final ChannelGroup children = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);

    private InetSocketAddress localAddress;
    private volatile boolean open = true;

    /**
     * The built in self signed identity backing the default answer
     * decoration. Generated for every channel regardless of constructor so
     * no construction path can reach an answer without an identity to sign
     * it with.
     */
    private final ServerIdentity serverIdentity = generateDefaultIdentity();

    /**
     * Creates a server channel with an engine of its own, created once the
     * signaling bound and closed with the channel.
     *
     * @param signaling the signaling the joins arrive over
     */
    public NetherNetServerChannel(NetherNetServerSignaling signaling) {
        this(EngineRef.owned(DefaultEngine::create), signaling);
    }

    /**
     * Creates a server channel on the caller's engine, which the channel never
     * closes.
     *
     * @param engine the engine; shared engines are closed by their owner
     * @param signaling the signaling the joins arrive over
     */
    public NetherNetServerChannel(NetherNetEngine engine, NetherNetServerSignaling signaling) {
        this(EngineRef.shared(engine), signaling);
    }

    /**
     * Creates a server channel that owns the engine the supplier returns. The
     * supplier runs once, in bind, after the signaling bound, so a bind that
     * fails (a taken TCP port, a refused WebSocket) creates no engine state.
     * The channel closes the engine with itself.
     *
     * @param engineSupplier creates the engine
     * @param signaling the signaling the joins arrive over
     */
    public NetherNetServerChannel(Supplier<? extends NetherNetEngine> engineSupplier, NetherNetServerSignaling signaling) {
        this(EngineRef.owned(engineSupplier), signaling);
    }

    private NetherNetServerChannel(EngineRef engine, NetherNetServerSignaling signaling) {
        this.engine = engine;
        this.signaling = signaling;
        this.config = new DefaultNetherServerChannelConfig(this);
    }

    private static ServerIdentity generateDefaultIdentity() {
        try {
            return ServerIdentity.generate("self");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        if (!(localAddress instanceof InetSocketAddress)) throw new IllegalArgumentException("Unsupported address type");
        this.localAddress = (InetSocketAddress) localAddress;

        this.signaling.setNewConnectionHandler((connectionId, remoteNetworkId, offerSdp) ->
                acceptConnection(connectionId, offerSdp, remoteNetworkId));

        // Bind the signaling endpoint before an owned engine exists: binding
        // is the step that fails in the wild (a taken TCP port), and that
        // failure then creates no engine state at all. Offers cannot outrun
        // the engine: they hop onto this event loop, which is still running
        // doBind.
        this.signaling.bind(localAddress);

        try {
            engine.get();
        } catch (Exception | LinkageError e) {
            // The signaling endpoint is up but the engine never came to be;
            // close signaling so the failed channel holds nothing.
            try {
                signaling.close();
            } catch (Exception closeError) {
                log.debug("Failed to close signaling after engine creation failed: {}", closeError.getMessage());
            }
            throw e;
        }
    }

    /**
     * Accepts an incoming connection offer. Runs on the signaling I/O thread:
     * only the signal handler registration happens here (a cheap map put, so
     * candidates arriving right behind the offer wait for the peer instead of
     * being dropped), then the join hops onto this server channel's event
     * loop.
     */
    public void acceptConnection(long connectionId, String offerSdp, String remoteNetworkId) {
        if (!isOpen()) {
            return;
        }
        Join join = new Join(connectionId, remoteNetworkId, offerSdp, signaling.fullIceAnswers());
        signaling.setSignalHandler(connectionId, join);
        try {
            eventLoop().execute(() -> accept(join));
        } catch (RuntimeException e) {
            signaling.removeSignalHandler(connectionId);
            throw e;
        }
    }

    /** Creates the child for an accepted offer and exposes it. Runs on this server channel's event loop. */
    private void accept(Join join) {
        long connectionId = join.connectionId;
        // Capture identity before checking liveness: cancellation can remove the HTTP exchange.
        ClientIdentity identity = signaling.clientIdentityOf(connectionId);
        NetherNetEngine current = engine.current();
        // Offers racing a failed doBind: signaling briefly accepted
        // connections but the engine never materialized.
        if (!isOpen() || current == null || !signaling.isConnectionPending(connectionId)) {
            log.debug("Dropping offer {} while the server engine is unavailable", join.id);
            signaling.removeSignalHandler(connectionId);
            return;
        }
        int maxMessageSize;
        try {
            maxMessageSize = NetherNetConstants.parseMaxMessageSize(join.offer);
        } catch (IllegalArgumentException e) {
            log.debug("Refusing offer {}: {}", join.id, e.getMessage());
            join.signalFailure("Failed to establish connection");
            signaling.removeSignalHandler(connectionId);
            return;
        }

        // An HTTP front end knows the peer's address from the request; ICE
        // later moves it to the selected candidate pair.
        InetSocketAddress signaledAddress = signaling.remoteAddressOf(connectionId);
        NetherNetChildChannel child = new NetherNetChildChannel(this,
                signaledAddress != null ? signaledAddress : generatePlaceholderAddress(), localAddress, join);
        child.setMaxOutboundMessageSize(maxMessageSize);
        child.attr(NetherNetChildChannel.CONNECTION_ID).set(join.id);
        child.setClientIdentity(identity);
        children.add(child);
        child.closeFuture().addListener(future -> signaling.removeSignalHandler(connectionId));

        try {
            child.openPeer(current, peerConfig());
        } catch (Exception | LinkageError e) {
            log.error("Failed to establish connection {}: {}", join.id, e.getMessage(), e);
            join.signalFailure("Failed to establish connection");
            child.close();
            return;
        }
        join.attach(child);

        int handshakeTimeoutSeconds = config.getOption(NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS);
        ScheduledFuture<?> handshakeTimeout = eventLoop().schedule(() -> {
            if (child.isOpen() && !child.isActive()) {
                log.warn("Connection {} timed out during handshake ({}s)", join.id, handshakeTimeoutSeconds);
                child.connectionFailed(NetherConnectionFailure.HANDSHAKE_TIMEOUT);
                child.close();
            }
        }, handshakeTimeoutSeconds, TimeUnit.SECONDS);
        // Any close makes the timeout obsolete, or a connection that ends
        // within it would get a bogus timed out warning.
        child.closeFuture().addListener(future -> handshakeTimeout.cancel(false));

        // Negotiate only once registered: initializer failure, timeout and
        // disconnect all close through the child, which owns the peer.
        child.pipeline().addLast(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) {
                join.negotiate(child);
            }
        });

        NetherServerMetrics serverMetrics = config.getServerMetrics();
        if (serverMetrics != null) {
            try {
                serverMetrics.connectionAccepted(join.remoteNetworkId, identity != null);
            } catch (RuntimeException e) {
                log.debug("NetherNet server metrics failed", e);
            }
        }
        pipeline().fireChannelRead(child);
        if (!isOpen()) {
            // Closed while this join was set up, after doClose went through the children.
            child.close();
        }
    }

    private PeerConfig peerConfig() {
        return new PeerConfig(PeerConfig.Role.ANSWERER,
                NetherNetChannel.iceServers(config.getIceServers(), signaling.getIceServers()),
                config.getCandidatePolicy(), null, null, null);
    }

    private NetherNetAnswerDecorator answerDecorator() {
        // The built in self signed identity keeps answers acceptable to 26.40
        // clients out of the box; consumers replace it to own the keys and
        // domain of the assertion.
        NetherNetAnswerDecorator decorator = config.getOption(NetherChannelOption.NETHER_SERVER_ANSWER_DECORATOR);
        return decorator != null ? decorator : serverIdentity::augmentAnswer;
    }

    /**
     * One join's signaling. Remote signals arrive on the signaling thread;
     * answer and local candidates go out from the child's event loop, so local
     * candidates follow the answer.
     */
    private final class Join implements NetherNetChildChannel.Negotiation, NetherNetSignaling.SignalHandler {
        private final long connectionId;
        private final String id;
        private final String remoteNetworkId;
        private final String offer;
        private final boolean fullIce;
        private final String mid;

        // Guarded by this: signals held until the child's peer exists.
        private final List<String> early = new ArrayList<>();
        private NetherNetChildChannel child;

        private final AtomicBoolean failureSignaled = new AtomicBoolean();

        // Child event loop only.
        private boolean answerSent;
        private final List<String> pendingCandidates = new ArrayList<>();

        private Join(long connectionId, String remoteNetworkId, String offer, boolean fullIce) {
            this.connectionId = connectionId;
            this.id = Long.toUnsignedString(connectionId);
            this.remoteNetworkId = remoteNetworkId;
            this.offer = offer;
            this.fullIce = fullIce;
            this.mid = NetherNetConstants.applicationMid(offer);
        }

        @Override
        public synchronized void onSignal(String signal) {
            if (child != null) {
                apply(child, signal);
            } else if (early.size() < MAX_EARLY_SIGNALS) {
                early.add(signal);
            } else {
                log.debug("Dropping a signal for {}: {} are already waiting", id, MAX_EARLY_SIGNALS);
            }
        }

        /** The child's peer exists: replays the waiting signals, then passes later ones straight on. */
        synchronized void attach(NetherNetChildChannel child) {
            this.child = child;
            for (String signal : early) {
                apply(child, signal);
            }
            early.clear();
        }

        private void apply(NetherNetChildChannel child, String signal) {
            String[] parts = signal.split(" ", 3);
            if (parts.length < 3) {
                return;
            }
            switch (parts[0]) {
                case NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD -> {
                    Peer peer = child.peer();
                    if (peer != null) {
                        log.trace("Applying remote candidate for {}", id);
                        // The engine holds candidates until the offer applied.
                        peer.addRemoteCandidate(parts[2], mid);
                    }
                }
                case NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR -> {
                    log.debug("Received CONNECT_ERROR for {}", id);
                    child.connectErrorReceived();
                }
                default -> log.trace("Ignoring signal {} for {}", parts[0], id);
            }
        }

        /** Child event loop, from the child's initializer. */
        void negotiate(NetherNetChildChannel child) {
            Peer peer = child.peer();
            if (peer == null) {
                return;
            }
            CompletionStage<String> local = peer.answer(offer);
            if (fullIce) {
                // Request/response signaling carries every candidate in the answer.
                local = local.thenCompose(ignored -> peer.gatheredLocalDescription());
            }
            local.whenComplete((sdp, failure) -> onChildLoop(child, () -> answered(child, sdp, failure)));
        }

        private void answered(NetherNetChildChannel child, String sdp, Throwable failure) {
            if (!child.isOpen() || failureSignaled.get()) {
                return;
            }
            if (failure != null) {
                log.debug("Negotiation for {} failed: {}", id, failure.toString());
                child.connectionFailed(NetherConnectionFailure.NEGOTIATION_FAILED);
                fail(child, "Negotiation failed");
                return;
            }
            String answer;
            try {
                answer = answerDecorator().decorate(sdp);
            } catch (Exception e) {
                // Peers require the decoration (the identity assertion); an
                // undecorated answer would only be refused by the peer after
                // parsing. Fail the exchange instead: the signaling layer
                // reports the error (a 400 on HTTP) and the peer falls back
                // immediately.
                log.warn("Answer decoration failed for {}; failing the exchange: {}", id, e.getMessage());
                fail(child, "answer decoration failed");
                return;
            }
            try {
                signaling.sendSignal(remoteNetworkId, NetherNetConstants.buildSignalConnectResponse(connectionId, answer));
            } catch (Exception e) {
                // Signaling dropped mid handshake; the client cannot receive
                // the answer, so the handshake timeout reaps this connection.
                log.warn("Failed to send answer for {} (signaling unavailable): {}", id, e.getMessage());
                return;
            }
            answerSent = true;
            for (String candidate : pendingCandidates) {
                sendCandidate(candidate);
            }
            pendingCandidates.clear();
        }

        @Override
        public void localCandidate(String candidate, String mid) {
            if (fullIce) {
                // The full answer carries the candidates; there is no trickle channel.
                return;
            }
            NetherNetChildChannel target;
            synchronized (this) {
                target = child;
            }
            if (target != null) {
                onChildLoop(target, () -> {
                    if (!target.isOpen()) {
                        return;
                    }
                    if (answerSent) {
                        sendCandidate(candidate);
                    } else {
                        pendingCandidates.add(candidate);
                    }
                });
            }
        }

        private void sendCandidate(String candidate) {
            try {
                signaling.sendSignal(remoteNetworkId, NetherNetConstants.buildSignalCandidateAdd(connectionId, candidate));
            } catch (Exception e) {
                // Established connections do not signal candidates, so only
                // this in flight handshake is affected; the handshake timeout
                // cleans it up if it cannot complete.
                log.debug("Failed to signal ICE candidate for {} (signaling unavailable): {}", id, e.getMessage());
            }
        }

        /** The child closes itself after this. */
        @Override
        public void failed(String reason) {
            signalFailure(reason);
        }

        private void fail(NetherNetChildChannel child, String reason) {
            signalFailure(reason);
            child.close();
        }

        /** Tells the remote peer the join failed, once. Any thread. */
        void signalFailure(String reason) {
            if (!failureSignaled.compareAndSet(false, true)) {
                return;
            }
            try {
                signaling.sendSignal(remoteNetworkId,
                        NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR + " " + id + " " + reason);
            } catch (Exception e) {
                log.debug("Could not signal negotiation failure for {}: {}", id, e.getMessage());
            }
        }

        private void onChildLoop(NetherNetChildChannel child, Runnable task) {
            try {
                child.eventLoop().execute(task);
            } catch (RejectedExecutionException shuttingDown) {
                log.debug("Dropping a negotiation step for {}: its event loop is shutting down", id);
            }
        }
    }

    /**
     * Generates a unique placeholder address in the 10.x.x.x range for a
     * connection whose address signaling did not see, until ICE selects a
     * pair. The 10.0.0.0/8 range is private (RFC 1918) and will not collide
     * with real public client addresses.
     */
    private static InetSocketAddress generatePlaceholderAddress() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        String ip = "10." + (r.nextInt(1, 256)) + "." + (r.nextInt(256)) + "." + (r.nextInt(1, 256));
        return new InetSocketAddress(ip, 0);
    }

    @Override
    protected void doClose() throws Exception {
        this.open = false;
        children.close();

        try {
            signaling.close();
        } finally {
            // Every peer's close stage completes before an owned engine
            // releases its threads; a shared engine stays with its owner.
            engine.release();
        }
    }

    @Override
    protected void doBeginRead() throws Exception {
        // Server channel doesn't read data directly
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
    public ChannelConfig config() { return config; }

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

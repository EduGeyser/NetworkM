package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherClientChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherNetAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetClientSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingFailure;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jose4j.lang.JoseException;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ConnectionPendingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A NetherNet client: offers a connection over its signaling and opens the
 * two data channels. Over signaling that trickles, candidates follow the
 * offer one by one; otherwise the offer waits for gathering and carries them
 * all. A handshake that does not finish in time is retried with a new peer
 * and connection ID; once connected, a failing peer ends the connection like
 * on the server.
 */
public class NetherNetClientChannel extends NetherNetChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetClientChannel.class);

    private final EngineRef engine;
    private final NetherNetClientSignaling signaling;
    private final DefaultNetherClientChannelConfig clientConfig;
    private final Object attemptLock = new Object();

    private volatile long connectionId; // Session ID (Long)
    private volatile String targetNetworkId; // Peer ID (String, for Realms)

    private volatile boolean handshakeComplete = false;

    private ChannelPromise connectPromise;

    private volatile ScheduledFuture<?> handshakeTimeoutTask;

    private int retryCount = 0;

    // Monotonic attempt marker, bumped on every handshake retry and close.
    // Engine callbacks of a previous attempt capture their generation and
    // bail once a retry has moved past them, so a delayed stale callback can
    // no longer mutate the replacement attempt's state.
    private volatile int attemptGeneration;

    // Event loop confined, per attempt: local candidates wait until the offer is out.
    private boolean offerSent;
    private List<String> pendingLocalCandidates = new ArrayList<>();
    private String mid = "0";

    /**
     * Creates a client with an engine of its own, created when the first
     * attempt needs it and closed with the channel.
     *
     * @param signaling the signaling to connect over; closed with the channel
     */
    public NetherNetClientChannel(NetherNetClientSignaling signaling) {
        this(EngineRef.owned(DefaultEngine::create), signaling);
    }

    /**
     * Creates a client on the caller's engine, which the channel never closes.
     *
     * @param engine the engine; shared engines are closed by their owner
     * @param signaling the signaling to connect over; closed with the channel
     */
    public NetherNetClientChannel(NetherNetEngine engine, NetherNetClientSignaling signaling) {
        this(EngineRef.shared(engine), signaling);
    }

    /**
     * Creates a client that owns the engine the supplier returns, created when
     * the first attempt needs it and closed with the channel.
     *
     * @param engineSupplier creates the engine
     * @param signaling the signaling to connect over; closed with the channel
     */
    public NetherNetClientChannel(Supplier<? extends NetherNetEngine> engineSupplier, NetherNetClientSignaling signaling) {
        this(EngineRef.owned(engineSupplier), signaling);
    }

    private NetherNetClientChannel(EngineRef engine, NetherNetClientSignaling signaling) {
        super(null, null, null);
        this.engine = engine;
        this.signaling = signaling;
        this.connectionId = this.cycleConnectionId();
        this.clientConfig = new DefaultNetherClientChannelConfig(this);
        this.config = clientConfig;
    }

    public void setTargetNetworkId(String id) {
        this.targetNetworkId = id;
    }

    @Override
    public boolean isActive() {
        return super.isActive() && handshakeComplete;
    }

    @Override
    protected void doClose() throws Exception {
        if (!isOpen()) {
            return;
        }
        synchronized (attemptLock) {
            attemptGeneration++;
            handshakeComplete = false;
            super.doClose();
        }
        cancelHandshakeTimeout();
        try {
            if (signaling != null) {
                try {
                    signaling.removeSignalHandler(this.connectionId);
                } finally {
                    signaling.close();
                }
            }
        } finally {
            if (connectPromise != null) {
                connectPromise.tryFailure(new ClosedChannelException());
            }
            engine.release();
        }
    }

    @Override
    protected NetherNetUnsafe newUnsafe() {
        return new NetherNetClientUnsafe();
    }

    private class NetherNetClientUnsafe extends NetherNetUnsafe {
        @Override
        public void connect(SocketAddress remote, SocketAddress local, ChannelPromise promise) {
            if (!promise.setUncancellable() || !ensureOpen(promise)) return;
            if (handshakeComplete) {
                promise.tryFailure(new AlreadyConnectedException());
                return;
            }
            if (connectPromise != null) {
                promise.tryFailure(new ConnectionPendingException());
                return;
            }

            if (remote instanceof NetherNetAddress address) {
                NetherNetClientChannel.this.setTargetNetworkId(address.getNetworkId());
                NetherNetClientChannel.this.remoteAddress = remote;
            } else if (remote instanceof InetSocketAddress address) {
                NetherNetClientChannel.this.remoteAddress = address;
                NetherNetClientChannel.this.setTargetNetworkId("0"); // "0" triggers auto-discovery in signaling
            } else {
                promise.setFailure(new IllegalArgumentException("Unsupported address: " + remote.getClass()));
                return;
            }

            NetherNetClientChannel.this.connectPromise = promise;
            eventLoop().execute(() -> startHandshake());
        }
    }

    private void startHandshake() {
        if (!isOpen() || handshakeComplete || connectPromise == null || connectPromise.isDone()) return;
        final int gen = attemptGeneration;
        clearFailureReported();

        log.debug("Starting Handshake with Connection ID: {}", Long.toUnsignedString(this.connectionId));

        armHandshakeTimeout(gen, handshakeTimeoutMillis() + signalingWaitMillis());

        try {
            signaling.setNotFoundHandler(reason -> executeForAttempt(gen, () ->
                    failHandshake(gen, "Target Network ID " + this.targetNetworkId + " not found or offline", null)));
            signaling.setSignalHandler(this.connectionId, new NetherNetSignaling.SignalHandler() {
                @Override
                public void onSignal(String signal) {
                    if (isCurrentAttempt(gen)) {
                        handleSignal(signal);
                    }
                }

                @Override
                public void onFailure(long id, SignalingFailure failure) {
                    executeForAttempt(gen, () -> {
                        if (isCurrentHandshake(gen)) {
                            connectionFailed(failure.failure());
                            failHandshake(gen, "The signaling gave up on the offer: " + failure.reason(), null);
                        }
                    });
                }
            });
            signaling.connect(remoteAddress).whenComplete((iceServers, failure) -> executeForAttempt(gen, () -> {
                if (!isCurrentHandshake(gen)) return;
                if (failure != null) {
                    failHandshake(gen, "Signaling connection failed", failure);
                    return;
                }
                try {
                    if (peer() == null) {
                        startPeer(iceServers, gen);
                    }
                } catch (Exception | LinkageError e) {
                    failHandshake(gen, "Failed to start WebRTC handshake", e);
                }
            }));
        } catch (Exception e) {
            failHandshake(gen, "Signaling connection failed", e);
        }
    }

    private boolean isCurrentAttempt(int generation) {
        return generation == attemptGeneration && isOpen();
    }

    private boolean isCurrentHandshake(int generation) {
        return isCurrentAttempt(generation) && !handshakeComplete && connectPromise != null && !connectPromise.isDone();
    }

    private void executeForAttempt(int generation, Runnable callback) {
        if (!isCurrentAttempt(generation)) return;
        try {
            eventLoop().execute(() -> {
                if (isCurrentAttempt(generation)) {
                    callback.run();
                }
            });
        } catch (RejectedExecutionException e) {
            // Shutdown can race an engine or signaling callback after the channel closes.
            if (isCurrentAttempt(generation) && connectPromise != null) {
                connectPromise.tryFailure(e);
            }
        }
    }

    private void failHandshake(int generation, String message, Throwable cause) {
        if (!isCurrentHandshake(generation)) return;
        ConnectException failure = new ConnectException(message);
        if (cause != null) {
            failure.initCause(cause);
        }
        connectPromise.tryFailure(failure);
        cancelHandshakeTimeout();
        close();
    }

    /** Starts the attempt's deadline over: past it, the attempt is retried. */
    private void armHandshakeTimeout(int gen, long millis) {
        cancelHandshakeTimeout();
        handshakeTimeoutTask = eventLoop().schedule(() -> {
            if (isCurrentHandshake(gen)) {
                connectionFailed(NetherConnectionFailure.HANDSHAKE_TIMEOUT);
                resetAndRetryHandshake();
            }
        }, millis, TimeUnit.MILLISECONDS);
    }

    private long handshakeTimeoutMillis() {
        return clientConfig.getHandshakeTimeoutMillis();
    }

    /**
     * How long the signaling may wait for an answer of its own. Until the answer is here, an
     * attempt's deadline makes room for it: HTTP waits 20 s for an answer by default, and a retry
     * would probe and post again while that answer can still arrive.
     */
    private long signalingWaitMillis() {
        return signaling.answerTimeout().toMillis();
    }

    private void cancelHandshakeTimeout() {
        ScheduledFuture<?> timeout = handshakeTimeoutTask;
        handshakeTimeoutTask = null;
        if (timeout != null) {
            timeout.cancel(false);
        }
    }

    private void resetAndRetryHandshake() {
        if (!isOpen()) return;
        if (connectPromise != null && connectPromise.isDone() && !connectPromise.isSuccess()) return;
        if (handshakeComplete) return;

        // Counts retries: the option's value plus the first attempt are made.
        int maxRetries = clientConfig.getMaxHandshakeAttempts();
        if (retryCount >= maxRetries) {
            if (connectPromise != null && !connectPromise.isDone()) {
                connectPromise.tryFailure(new ConnectException("Connection timed out after " + retryCount + " retries"));
            }
            close();
            return;
        }

        retryCount++;
        NetherChannelMetrics metrics = config.getMetrics();
        if (metrics != null) {
            try {
                metrics.handshakeRetry();
            } catch (RuntimeException e) {
                log.debug("NetherNet metrics failed", e);
            }
        }
        synchronized (attemptLock) {
            attemptGeneration++;
            discardPendingInbound();
        }
        try {
            retirePeer();
            signaling.removeSignalHandler(this.connectionId);
            this.cycleConnectionId();
            offerSent = false;
            pendingLocalCandidates = new ArrayList<>();
            startHandshake();
        } catch (Exception e) {
            failHandshake(attemptGeneration, "Failed to retry WebRTC handshake", e);
        }
    }

    private void startPeer(List<IceServer> iceServers, int gen) {
        NetherNetEngine current = engine.get();
        PeerConfig peerConfig = new PeerConfig(PeerConfig.Role.OFFERER,
                iceServers(clientConfig.getIceServers(), iceServers), clientConfig.getCandidatePolicy(), null, null, null);
        Peer created = current.createPeer(peerConfig, new Events(gen));
        attachPeer(created);
        bindReliable(created.openChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL)));
        bindUnreliable(created.openChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL)));

        final long attemptConnectionId = this.connectionId;
        CompletionStage<String> offered = created.offer();
        // Signaling that does not trickle carries every candidate in the offer, so it waits for
        // gathering, as vanilla's HTTP offers do (they carry host candidates).
        CompletionStage<String> description = signaling.trickles() ? offered
                : offered.thenCompose(ignored -> created.gatheredLocalDescription());
        description.whenComplete((offer, failure) -> executeForAttempt(gen, () -> {
            if (failure != null) {
                // The handshake timeout retries.
                log.debug("Creating the offer failed: {}", failure.toString());
                return;
            }
            mid = NetherNetConstants.applicationMid(offer);
            String signed;
            try {
                signed = signed(offer);
            } catch (JoseException | IllegalArgumentException e) {
                failHandshake(gen, "Could not sign the offer with the client identity", e);
                return;
            }
            try {
                signaling.sendSignal(targetNetworkId,
                        NetherNetConstants.buildSignalConnectRequest(attemptConnectionId, signed));
            } catch (Exception e) {
                log.debug("Failed to send connect request", e);
                resetAndRetryHandshake();
                return;
            }
            offerSent = true;
            if (signalingWaitMillis() > 0) {
                armHandshakeTimeout(gen, handshakeTimeoutMillis() + signalingWaitMillis());
            }
            List<String> pending = pendingLocalCandidates;
            pendingLocalCandidates = new ArrayList<>();
            for (String candidate : pending) {
                if (!sendLocalCandidate(attemptConnectionId, candidate)) {
                    return;
                }
            }
        }));
    }

    /** The offer with the configured identity's assertion, or as it is without one. */
    private String signed(String offer) throws JoseException {
        ClientIdentity identity = clientConfig.getIdentity();
        return identity == null ? offer : identity.withAssertion(offer);
    }

    private boolean sendLocalCandidate(long attemptConnectionId, String candidate) {
        try {
            signaling.sendSignal(targetNetworkId,
                    NetherNetConstants.buildSignalCandidateAdd(attemptConnectionId, candidate));
            return true;
        } catch (Exception e) {
            log.debug("Failed to send ICE candidate", e);
            resetAndRetryHandshake();
            return false;
        }
    }

    private void handleSignal(String signal) {
        final int generation = attemptGeneration;
        String[] parts = signal.split(" ", 3);
        if (parts.length < 2) return; // Allow length 2 for ERROR packets without payload
        String type = parts[0];
        String idStr = parts[1].trim();
        String data = parts.length > 2 ? parts[2] : "";

        // Verify this signal belongs to the current attempt
        final long signalId;
        try {
            signalId = Long.parseUnsignedLong(idStr);
            if (signalId != this.connectionId) {
                log.debug("Ignored stale signal for ID {}", idStr);
                return;
            }
        } catch (NumberFormatException e) {
            return;
        }

        executeForAttempt(generation, () -> {
            // Re-validate on the event loop: a retry may have cycled the
            // connection id between the check above (signaling thread) and
            // this task running. Inside the task the id, generation, and
            // peer mutate together, so passing this check means everything
            // read below belongs to the current attempt.
            if (signalId != this.connectionId) {
                log.debug("Ignored stale signal for ID {} (attempt retried)", idStr);
                return;
            }
            if (!isOpen() || handshakeComplete) return;
            if (NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE.equals(type)) {
                try {
                    setMaxOutboundMessageSize(NetherNetConstants.parseMaxMessageSize(data));
                } catch (IllegalArgumentException e) {
                    failHandshake(generation, "Invalid remote max-message-size", e);
                    return;
                }
                if (signalingWaitMillis() > 0) {
                    // The answer is here: what is left is the handshake itself
                    armHandshakeTimeout(generation, handshakeTimeoutMillis());
                }
            }
            Peer current = peer();
            if (current == null) return;

            switch (type) {
                case NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE ->
                        current.applyAnswer(data).whenComplete((ignored, failure) -> {
                            if (failure != null) {
                                // The handshake timeout retries.
                                log.debug("Applying the answer failed: {}", failure.toString());
                            }
                        });
                // The engine holds candidates until the answer applied.
                case NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD -> current.addRemoteCandidate(data, mid);
                case NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR -> {
                    log.error("Received SIGNAL_CONNECT_ERROR for {}.", Long.toUnsignedString(this.connectionId));
                    connectionFailed(NetherConnectionFailure.CONNECT_ERROR);
                    if (connectPromise != null && !connectPromise.isDone()) {
                        connectPromise.tryFailure(new ConnectException("Remote peer sent connect error."));
                    }
                    close();
                }
                default -> log.debug("Received unknown signal type: {}", type);
            }
        });
    }

    /** One attempt's peer events; events of an older attempt are dropped. */
    private final class Events extends PeerEvents {
        private final int generation;

        private Events(int generation) {
            this.generation = generation;
        }

        @Override
        boolean stale() {
            return !isCurrentAttempt(generation);
        }

        @Override
        public void onLocalCandidate(String candidate, String candidateMid) {
            if (!signaling.trickles()) {
                // The offer carries it
                return;
            }
            final long attemptConnectionId = connectionId;
            executeForAttempt(generation, () -> {
                if (offerSent) {
                    sendLocalCandidate(attemptConnectionId, candidate);
                } else {
                    pendingLocalCandidates.add(candidate);
                }
            });
        }

        @Override
        public void onGatheringComplete() {
            // Trickling, every candidate went out on its own; otherwise the offer waits for
            // the gathered description.
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            // The client opens both data channels itself.
            return false;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            executeForAttempt(generation, () -> {
                if (link == reliableLink() && isCurrentHandshake(generation)) {
                    handshakeComplete = true;
                    cancelHandshakeTimeout();
                    markTransportOpen();
                    if (connectPromise != null) {
                        connectPromise.trySuccess();
                    }
                    fireChannelActiveIfReady();
                }
            });
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            // The loop already serializes attempts, and overflow may end the connection inline.
            if (eventLoop().inEventLoop()) {
                if (!stale()) {
                    inbound(link, message);
                }
                return;
            }
            synchronized (attemptLock) {
                if (!stale()) {
                    inbound(link, message);
                }
            }
        }

        @Override
        void endedBeforeActivation(CloseCause cause, boolean reliableChannel) {
            executeForAttempt(generation, () -> {
                if (handshakeComplete) {
                    // The reliable channel opened while this hopped onto the loop.
                    transportEnded(cause, reliableChannel);
                } else {
                    log.debug("Peer ended during handshake ({}); retrying", cause);
                    connectionFailed(failureOf(cause));
                    resetAndRetryHandshake();
                }
            });
        }
    }

    private long cycleConnectionId() {
        this.connectionId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        return this.connectionId;
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertiser;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * NetherNet signaling over LAN discovery datagrams, for clients that find a server on their
 * network and for servers they find.
 *
 * <p>As a {@link JoinSource}, it listens on the address it was created with, usually
 * {@code 0.0.0.0:7551} ({@link io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants#DISCOVERY_PORT}),
 * answers discovery requests with fixed {@linkplain #setServerData server data} or what an
 * {@linkplain #setAdvertiser advertiser} gives per request, and turns each
 * {@code CONNECTREQUEST} into a {@link Join}. Replies go to the address the offer came from, pinned
 * then, and to the client's network ID; the join takes later signals only from both. Anyone on the
 * network can forge a datagram's source, so joins carry it as {@link AddressSource#SIGNALING_LAN}.
 */
public class NetherNetDiscoverySignaling implements NetherNetClientSignaling, JoinSource {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetDiscoverySignaling.class);

    /** The default deadline of one {@link LanAdvertiser} call, as HTTP's status provider has. */
    public static final Duration DEFAULT_ADVERTISER_TIMEOUT = NetherNetHttpSignaling.DEFAULT_STATUS_TIMEOUT;

    private final NetherNetDiscovery discovery;
    private final InetSocketAddress bindAddress;
    private final String localNetworkId;
    private final long discoveryTimeoutMillis;
    private final Object lifecycleLock = new Object();
    private DiscoveryAttempt pendingConnect;
    private volatile boolean closed;
    private final SourceHealth health = new SourceHealth();

    // State captured after connect
    private volatile InetSocketAddress remoteAddress;
    private final AtomicReference<String> discoveredServerId = new AtomicReference<>(null);

    /**
     * Creates a NetherNetDiscoverySignaling with a random local Network ID and binds to an ephemeral port.
     */
    public NetherNetDiscoverySignaling() {
        this(ThreadLocalRandom.current().nextLong(), new InetSocketAddress(0));
    }

    /**
     * Creates a NetherNetDiscoverySignaling with the specified local Network ID.
     *
     * @param localNetworkId The local Network ID to use.
     */
    public NetherNetDiscoverySignaling(long localNetworkId) {
        this(localNetworkId, new InetSocketAddress(0));
    }

    /**
     * Creates a NetherNetDiscoverySignaling with the specified local Network ID and bind address.
     *
     * @param localNetworkId The local Network ID to use.
     * @param bindAddress    The address to bind the discovery socket to.
     */
    public NetherNetDiscoverySignaling(long localNetworkId, InetSocketAddress bindAddress) {
        this(localNetworkId, bindAddress, new NetherNetDiscovery(localNetworkId), 10_000);
    }

    NetherNetDiscoverySignaling(long localNetworkId, InetSocketAddress bindAddress,
                               NetherNetDiscovery discovery, long discoveryTimeoutMillis) {
        if (discoveryTimeoutMillis <= 0) {
            throw new IllegalArgumentException("discoveryTimeoutMillis must be positive");
        }
        this.localNetworkId = Long.toUnsignedString(localNetworkId);
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
        this.discoveryTimeoutMillis = discoveryTimeoutMillis;
    }

    @Override
    public String getLocalNetworkId() {
        return this.localNetworkId;
    }

    /**
     * Discovers the remote server with a deadline of ten seconds. Calls for the
     * same pending target resend the request and share its completion future.
     */
    @Override
    public CompletableFuture<List<IceServer>> connect(SocketAddress remote) {
        if (!(remote instanceof InetSocketAddress)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Discovery requires InetSocketAddress"));
        }

        DiscoveryAttempt attempt;
        boolean newAttempt;
        synchronized (lifecycleLock) {
            if (closed) {
                return CompletableFuture.failedFuture(new ClosedChannelException());
            }
            if (pendingConnect != null) {
                if (!remote.equals(remoteAddress)) {
                    return CompletableFuture.failedFuture(new IllegalStateException("Discovery is already connecting"));
                }
                if (!pendingConnect.requestSent) {
                    return pendingConnect.result;
                }
                attempt = pendingConnect;
                newAttempt = false;
            } else {
                this.remoteAddress = (InetSocketAddress) remote;
                discoveredServerId.set(null);
                attempt = new DiscoveryAttempt();
                pendingConnect = attempt;
                newAttempt = true;
            }
        }
        if (newAttempt) {
            attempt.timeout.orTimeout(discoveryTimeoutMillis, TimeUnit.MILLISECONDS)
                    .whenComplete((unused, error) -> {
                        if (error != null) {
                            completeAttempt(attempt, null, error);
                        }
                    });
            attempt.result.whenComplete((result, error) -> {
                if (attempt.result.isCancelled()) {
                    completeAttempt(attempt, null, error);
                }
            });
        }

        try {
            if (!this.discovery.isActive()) {
                log.info("Binding NetherNet Discovery to {}", bindAddress);
                this.discovery.bind(bindAddress);
            }

            synchronized (lifecycleLock) {
                if (pendingConnect == attempt && !closed) {
                    log.debug("Sending Discovery Request to {}", remote);
                    this.discovery.sendDiscoveryRequestToPeer((InetSocketAddress) remote, attempt.callback);
                    attempt.requestSent = true;
                }
            }
        } catch (Exception e) {
            log.error("Failed to send discovery request", e);
            completeAttempt(attempt, null, e);
        }

        return attempt.result;
    }

    private void completeAttempt(DiscoveryAttempt attempt, Long serverNetworkId, Throwable failure) {
        synchronized (lifecycleLock) {
            if (pendingConnect != attempt) {
                return;
            }
            if (closed) {
                serverNetworkId = null;
                failure = new ClosedChannelException();
            }
            pendingConnect = null;
            if (serverNetworkId != null) {
                discoveredServerId.set(Long.toUnsignedString(serverNetworkId));
            }
        }
        discovery.clearDiscoveryCallback(attempt.callback);
        attempt.timeout.cancel(false);
        if (failure == null) {
            attempt.result.complete(Collections.emptyList());
        } else {
            attempt.result.completeExceptionally(failure);
        }
    }

    private final class DiscoveryAttempt {
        private boolean requestSent;
        private final CompletableFuture<List<IceServer>> result = new CompletableFuture<>();
        private final CompletableFuture<Void> timeout = new CompletableFuture<>();
        private final BiConsumer<Long, ByteBuf> callback = (serverNetworkId, payload) -> {
            try {
                completeAttempt(this, serverNetworkId, null);
            } finally {
                ReferenceCountUtil.release(payload);
            }
        };
    }

    /**
     * Starts taking joins: binds the discovery socket in the background, unless a client
     * connection already bound it. The health turns {@link SignalingState#CONNECTED} once it
     * listens, or {@link SignalingState#CLOSED} with the failure when the bind fails.
     */
    @Override
    public void start(JoinSink sink) {
        Objects.requireNonNull(sink, "sink");
        if (closed) {
            throw new IllegalStateException("Discovery signaling is closed");
        }
        discovery.setOfferHandler((connectionId, senderId, offer, sender) -> offered(sink, connectionId, senderId,
                offer, sender));
        if (discovery.isActive()) {
            health.set(SignalingState.CONNECTED, null);
            return;
        }
        health.set(SignalingState.STARTING, null);
        // Binding waits for the socket's own event loop; start never blocks the endpoint's.
        Thread.ofVirtual().name("nethernet-lan-bind").start(() -> {
            try {
                discovery.bind(bindAddress);
                health.set(SignalingState.CONNECTED, null);
            } catch (RuntimeException e) {
                log.warn("LAN discovery could not listen on {}: {}", bindAddress, e.getMessage());
                health.set(SignalingState.CLOSED, e);
            }
        });
    }

    private void offered(JoinSink sink, String connectionId, long senderId, String offer, InetSocketAddress sender) {
        LanJoin join = new LanJoin(connectionId, senderId, sender);
        try {
            // Routes this client's later signals for the connection ID to the join, and keeps the
            // client in the peer table while the join lasts.
            if (!discovery.registerJoin(senderId, connectionId, sender, join::received)) {
                return;
            }
        } catch (IllegalStateException closing) {
            return;
        }
        sink.join(new Join(connectionId, Long.toUnsignedString(senderId), offer,
                new SourcedAddress(sender, AddressSource.SIGNALING_LAN, null), join));
    }

    /** One join's way back to the client, at the address its offer came from. */
    private final class LanJoin extends MessageReplyPath {
        private final String connectionId;
        private final long senderId;
        private final InetSocketAddress sender;

        private LanJoin(String connectionId, long senderId, InetSocketAddress sender) {
            super(connectionId);
            this.connectionId = connectionId;
            this.senderId = senderId;
            this.sender = sender;
        }

        @Override
        CompletionStage<Void> send(Signal signal) {
            CompletableFuture<Void> sent = new CompletableFuture<>();
            try {
                ChannelFuture write = discovery.sendSignal(sender, senderId, signal);
                write.addListener(done -> {
                    if (done.isSuccess()) {
                        sent.complete(null);
                    } else {
                        sent.completeExceptionally(done.cause());
                    }
                });
            } catch (RuntimeException e) {
                sent.completeExceptionally(e);
            }
            return sent;
        }

        @Override
        void release() {
            discovery.unregisterJoin(senderId, connectionId);
        }
    }

    @Override
    public SignalingHealth health() {
        return health.get();
    }

    @Override
    public void addHealthListener(Consumer<SignalingHealth> listener) {
        health.addListener(listener);
    }

    @Override
    public void removeHealthListener(Consumer<SignalingHealth> listener) {
        health.removeListener(listener);
    }

    /** LAN peers reach each other directly; there are no STUN or TURN servers. */
    @Override
    public List<IceServer> iceServers() {
        return List.of();
    }

    /**
     * Answers every discovery request with this server data. Replaces an advertiser.
     *
     * @param data the advertisement
     * @throws IllegalArgumentException if it does not fit one datagram
     * @see NetherNetDiscovery#setServerData(LanServerData.V7)
     */
    public void setServerData(LanServerData.V7 data) {
        this.discovery.setServerData(data);
    }

    /**
     * Asks the advertiser what to answer each discovery request with, with a deadline of
     * {@link #DEFAULT_ADVERTISER_TIMEOUT} per call. Replaces fixed server data.
     *
     * @param advertiser the advertiser
     * @see NetherNetDiscovery#setAdvertiser(LanAdvertiser)
     */
    public void setAdvertiser(LanAdvertiser advertiser) {
        this.discovery.setAdvertiser(advertiser);
    }

    /**
     * Asks the advertiser what to answer each discovery request with, with this deadline per
     * call: past it the request gets the last server data the advertiser produced in time instead.
     * Replaces fixed server data.
     *
     * @param advertiser the advertiser
     * @param timeout    the deadline of one call, 1 s for {@link #setAdvertiser(LanAdvertiser)}
     * @throws IllegalArgumentException if the timeout is not positive; what was advertised stays
     * @see NetherNetDiscovery#setAdvertiser(LanAdvertiser, Duration)
     */
    public void setAdvertiser(LanAdvertiser advertiser, Duration timeout) {
        this.discovery.setAdvertiser(advertiser, timeout);
    }

    @Override
    public void sendSignal(String targetNetworkId, Signal signal) {
        send(targetNetworkId, signal.wire());
    }

    /**
     * Sends LAN discovery's keep-alive {@code "Ping"}, which refreshes this peer's address at the
     * target. It is a discovery message, not a NetherNet signal, so it has no place in
     * {@link #sendSignal}.
     *
     * @param targetNetworkId the target's network ID, or {@link #DIALED_SERVER} for the server discovery found
     */
    public void sendPing(String targetNetworkId) {
        send(targetNetworkId, NetherNetDiscovery.PING);
    }

    /** Sends one discovery message to a target named as in {@link #sendSignal}. */
    private void send(String targetNetworkId, String message) {
        String actualIdStr = targetNetworkId;

        // Client Mode: the server discovery found
        if (actualIdStr == null || actualIdStr.equals(DIALED_SERVER)) {
            actualIdStr = discoveredServerId.get();
        }

        if (actualIdStr == null) {
            log.warn("Cannot send signal: Unknown Network ID.");
            return;
        }

        long id;
        try {
            id = Long.parseUnsignedLong(actualIdStr);
        } catch (NumberFormatException e) {
            log.error("Cannot send LAN signal to non-numeric Network ID: {}", actualIdStr);
            return;
        }
        // Client Mode has the server's address; Server Mode finds the client's by its ID
        InetSocketAddress remote = remoteAddress;
        discovery.sendMessage(remote != null ? remote : discovery.recipientOf(id), id, message);
    }

    @Override
    public void setSignalHandler(String connectionId, String remoteNetworkId, SignalHandler handler) {
        // DIALED_SERVER (or none) is the server discovery finds, as in sendSignal. The handler is registered
        // before discovery runs, so the sender is checked against the server known when a signal arrives.
        if (remoteNetworkId == null || remoteNetworkId.equals(DIALED_SERVER)) {
            this.discovery.registerSignalHandler(connectionId, sender -> {
                String discovered = discoveredServerId.get();
                return discovered != null && Long.parseUnsignedLong(discovered) == sender;
            }, handler);
            return;
        }
        long remote;
        try {
            remote = Long.parseUnsignedLong(remoteNetworkId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("A LAN Network ID is an unsigned 64-bit decimal number", e);
        }
        this.discovery.registerSignalHandler(connectionId, sender -> sender == remote, handler);
    }

    @Override
    public void removeSignalHandler(String connectionId) {
        this.discovery.unregisterSignalHandler(connectionId);
    }

    @Override
    public void setNotFoundHandler(NetherNetClientSignaling.NotFoundHandler handler) {
        // Not implemented for Discovery signaling
    }

    /** Completes once the closed discovery socket, on its port, and its event loop are gone. */
    @Override
    public CompletionStage<Void> termination() {
        return discovery.termination();
    }

    @Override
    public void close() {
        DiscoveryAttempt attempt;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            attempt = pendingConnect;
        }
        try {
            if (attempt != null) {
                completeAttempt(attempt, null, new ClosedChannelException());
            }
        } finally {
            this.discovery.close();
            health.set(SignalingState.CLOSED, null);
        }
    }
}

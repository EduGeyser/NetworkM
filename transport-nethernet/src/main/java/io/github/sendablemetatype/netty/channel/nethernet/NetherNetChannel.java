package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportChannel;
import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLinkClosedException;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A NetherNet connection over one engine {@link Peer}: a reliable and an
 * unreliable data channel, each message carried whole.
 *
 * <p><b>Writing.</b> A plain {@link ByteBuf} goes on the reliable channel, a
 * {@link NetherNetPacket} on the channel it names. The channel splits a message
 * into segments of the size the remote peer negotiated, each with a countdown
 * header, at most {@value NetherNetConstants#MAX_SEGMENTS} of them; a longer
 * message fails its write before anything is sent, and an empty one sends
 * nothing. A write completes once the engine accepted every segment. While a
 * data channel's engine queue holds more than
 * {@link io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption#NETHER_SEND_PAUSE_BYTES}
 * the channel stops handing segments over and turns unwritable; past
 * {@link io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption#NETHER_MAX_QUEUED_BYTES}
 * waiting in Java as well, the connection ends with
 * {@link TransportDisconnectReason#QUEUE_TOO_LONG}.
 *
 * <p><b>Reading.</b> A reliable message arrives as a {@link ByteBuf}, an
 * unreliable one as a {@link NetherNetPacket} with {@code reliable} false.
 * {@code autoRead} and {@code read()} are honored: messages wait in a bounded
 * queue, and a queue that overflows ends the connection with
 * {@link TransportDisconnectReason#QUEUE_TOO_LONG}. Broken framing on the
 * reliable channel ends it with {@link TransportDisconnectReason#BAD_PACKET}, a
 * message over
 * {@link io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption#NETHER_MAX_REASSEMBLED_BYTES}
 * with {@link TransportDisconnectReason#SPLIT_QUEUE_TOO_LONG}. Nothing that
 * arrives after the transport ended the connection is delivered.
 *
 * <p><b>Ending.</b> The channel stays active until it closes, so every close
 * after activation passes through {@code channelInactive}. When the transport
 * ends the connection, one {@link TransportDisconnectReason} is fired as a user
 * event on the event loop just before the close, and only if
 * {@code channelActive} fired; closing from the application fires nothing.
 *
 * <p><b>Statistics.</b> {@link #transportMetrics()} reads the ICE round trip
 * time and the SCTP association's counters from the engine,
 * {@link #selectedPath()} is the candidate pair ICE selected, and
 * {@link #signaledAddress()}, {@link #mediaAddress()} and
 * {@link #reportedAddress()} say where the peer is and how that is known.
 * Each may be read from any thread. After the channel closed, the metrics are
 * {@link TransportMetrics#NONE}, since the peer is gone, while the path and
 * the addresses keep their last values, as a record of the connection.
 */
public abstract class NetherNetChannel extends AbstractChannel implements TransportChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetChannel.class);
    protected static final ChannelMetadata METADATA = new ChannelMetadata(false);

    private static final int MAX_PENDING_INBOUND_MESSAGES = 512;
    private static final int MAX_MESSAGES_PER_DRAIN = 64;
    /** The user-defined writability bit that the transport's send pause turns off. */
    private static final int SEND_PAUSE_WRITABILITY = 1;

    protected DefaultNetherChannelConfig config;
    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    private final Object peerLock = new Object();
    private Peer peer; // guarded by peerLock
    private CompletableFuture<Void> retiredPeers = CompletableFuture.completedFuture(null); // guarded by peerLock
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private volatile DataLink reliableLink;
    private volatile DataLink unreliableLink;
    private volatile NetherNetMessageAssembler assembler;

    private final AtomicBoolean channelActiveFired = new AtomicBoolean();
    private final AtomicBoolean failureReported = new AtomicBoolean();
    private final Object inboundLock = new Object();
    private final Runnable inboundDrainTask = this::drainInbound;
    private final Runnable clearReadPendingTask = () -> this.readPending = false;
    private volatile boolean inboundReady;
    private volatile boolean inboundClosed;
    private volatile boolean readPending;
    private Queue<NetherNetPacket> pendingInbound;
    private long pendingInboundBytes;
    private boolean inboundDrainScheduled;
    private volatile boolean inboundDrainRejected;

    // Why the transport ended the connection; the first ending wins. Set from
    // any thread, acted on by the event loop through the shared wakeup.
    private final AtomicReference<Ending> ending = new AtomicReference<>();
    // Event loop only: set before the reason fires, since a handler that
    // writes on it reenters closeIfEnded while the channel is still open.
    private boolean closingForEnding;
    private final AtomicBoolean wakeUpScheduled = new AtomicBoolean();
    private final Runnable wakeUpTask = this::onWakeUp;
    private final Runnable shutdownHook = () -> {
        if (isOpen() && eventLoop().inEventLoop()) {
            Ending end = ending.get();
            ((NetherNetUnsafe) unsafe()).closeWithFailure(end != null ? end.cause()
                    : new IOException("Event loop stopped before NetherNet sends completed"));
        }
    };
    // Netty retains engine-pending writes; keep unsent ones reachable without scanning them.
    private PendingWrite unsubmittedHead;
    private PendingWrite unsubmittedTail;
    private PendingWrite flushedWriteTail;
    private boolean removingCompletedWrites;
    // Set on the event loop when a data channel's engine queue is full; the
    // engine thread that reports the queue low clears it and wakes the loop.
    // The writer rechecks the queue after setting the flag so a concurrent
    // drain cannot lose the wakeup.
    private volatile boolean writesPaused;
    // The engine queue level the paused writes wait for. Set on the pausing
    // link together with its engine watermark, from the pause level in force
    // at that moment: comparing against the option itself could ignore the
    // engine's only report once the option changed.
    private volatile long resumeMark;

    private volatile boolean transportOpen;
    // The newest pair the event loop took in, in step with the remote address.
    private volatile TransportPath selectedPath;

    // Latest ICE round trip time in milliseconds, sampled while the channel is
    // active; negative until the first measurement. Failed samples keep the
    // last good value.
    private volatile long rttMillis = -1;
    private ScheduledFuture<?> rttSampler;
    protected volatile boolean open = true;

    /**
     * Largest segment the remote peer accepts, header included: its
     * {@code a=max-message-size}, capped at our own outgoing limit.
     */
    private volatile int maxOutboundMessageSize = NetherNetConstants.DEFAULT_SCTP_MESSAGE_SIZE;

    protected NetherNetChannel(Channel parent, InetSocketAddress remote, InetSocketAddress local) {
        super(parent);
        this.remoteAddress = remote;
        this.localAddress = local;
    }

    /** Why the transport ended a connection, and the failure pending writes get. */
    private record Ending(TransportDisconnectReason reason, IOException cause) {
    }

    // ---- Engine state ----

    /** The current peer, or null before it exists and after close. */
    final Peer peer() {
        synchronized (peerLock) {
            return peer;
        }
    }

    /**
     * Makes {@code created} this channel's peer. A channel that closed already
     * closes it instead.
     */
    final void attachPeer(Peer created) {
        boolean closed;
        synchronized (peerLock) {
            closed = !open;
            if (!closed) {
                peer = created;
            }
        }
        if (closed) {
            closeQuietly(created);
        }
    }

    /**
     * Closes the current peer and unbinds its data channels, for a client that
     * starts another attempt. The channel's termination still waits for the
     * old peer's teardown.
     */
    final void retirePeer() {
        unbindLinks();
        selectedPath = null;
        synchronized (peerLock) {
            if (peer != null) {
                retiredPeers = CompletableFuture.allOf(retiredPeers, closeQuietly(peer));
                peer = null;
            }
        }
    }

    /** Binds the reliable data channel: the one segments are assembled from and written to by default. */
    final void bindReliable(DataLink link) {
        NetherNetMessageAssembler previous = assembler;
        assembler = new NetherNetMessageAssembler();
        if (previous != null) {
            previous.close();
        }
        reliableLink = link;
    }

    final void bindUnreliable(DataLink link) {
        unreliableLink = link;
    }

    final DataLink reliableLink() {
        return reliableLink;
    }

    final DataLink unreliableLink() {
        return unreliableLink;
    }

    private void unbindLinks() {
        NetherNetMessageAssembler previous = assembler;
        assembler = null;
        if (previous != null) {
            previous.close();
        }
        reliableLink = null;
        unreliableLink = null;
    }

    private static CompletableFuture<Void> closeQuietly(Peer peer) {
        try {
            return peer.close().toCompletableFuture().handle((ignored, failure) -> null);
        } catch (RuntimeException e) {
            log.debug("Closing a NetherNet peer failed: {}", e.toString());
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Completes once this channel is closed and the teardown of every peer it
     * had finished. Capacity held for the connection is released here, not
     * when the close future completes.
     *
     * @return the termination stage
     */
    public CompletionStage<Void> termination() {
        return termination.minimalCompletionStage();
    }

    /**
     * The engine's events for one peer, handed to the engine at peer creation.
     * Callbacks run on engine threads; everything they touch is thread safe
     * or hops onto the event loop. Teardown never runs inside a callback: a
     * close requested here runs on the event loop.
     */
    abstract class PeerEvents implements PeerListener {
        /** The newest pair this peer's engine reported, for the event loop to take in. */
        private volatile TransportPath latestPath;
        private final Runnable applyPath = () -> applyPath(this);

        /** Whether this peer's events no longer concern the channel. */
        boolean stale() {
            return !isOpen();
        }

        /** The peer, or its reliable data channel, ended before the channel became active. */
        abstract void endedBeforeActivation(CloseCause cause, boolean reliableChannel);

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            if (!stale()) {
                inbound(link, message);
            }
        }

        /**
         * Reads a text message as game data, like a binary one. Vanilla sends
         * binary messages only, and its receive path closes a session only for
         * an empty segment or a wrong countdown header, not for anything else
         * about a message (transport-nethernet/docs/client-behavior.md).
         */
        @Override
        public void onTextMessage(DataLink link, ByteBuffer message) {
            onMessage(link, message);
        }

        @Override
        public void onQueueLow(DataLink link) {
            if (!stale()) {
                queueLow(link);
            }
        }

        @Override
        public void onPathSelected(TransportPath path) {
            if (!stale()) {
                latestPath = path;
                schedule(applyPath);
            }
        }

        @Override
        public void onStateChange(PeerState state) {
            if (stale()) {
                return;
            }
            NetherChannelMetrics metrics = config.getMetrics();
            if (metrics != null) {
                try {
                    metrics.peerStateChange(state);
                    TransportPath path = latestPath;
                    if (state == PeerState.CONNECTED && path != null) {
                        metrics.pathSelected(path.localType(), path.remoteType());
                    }
                } catch (RuntimeException e) {
                    log.debug("NetherNet metrics failed", e);
                }
            }
        }

        @Override
        public void onIceStateChange(IceState state) {
            NetherChannelMetrics metrics = config.getMetrics();
            if (!stale() && metrics != null) {
                try {
                    metrics.iceStateChange(state);
                } catch (RuntimeException e) {
                    log.debug("NetherNet metrics failed", e);
                }
            }
        }

        // Only the reliable channel ends the connection: vanilla never sends
        // on the unreliable one, and the connection carries on without it.
        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
            if (!stale() && link == reliableLink) {
                ended(cause, true);
            }
        }

        @Override
        public void onClosed(CloseCause cause) {
            if (!stale()) {
                ended(cause, false);
            }
        }

        private void ended(CloseCause cause, boolean reliableChannel) {
            if (isActive()) {
                transportEnded(cause, reliableChannel);
            } else {
                endedBeforeActivation(cause, reliableChannel);
            }
        }
    }

    /** The peer, or its reliable data channel, ended underneath an active channel. Any thread. */
    final void transportEnded(CloseCause cause, boolean reliableChannel) {
        endTransport(disconnectReason(cause, reliableChannel), new IOException("The NetherNet "
                + (reliableChannel ? "reliable data channel" : "peer") + " ended: " + cause));
    }

    /**
     * The engine refused a segment, so the stream cannot go on. A segment refused because the
     * remote peer closed the data channel it went to ends the connection with the reason of that
     * close: when a client quits while a write is in flight, the write can fail before the close
     * event arrives, and the reason must not depend on which comes first. Any other refusal is a
     * transport failure. Any thread.
     */
    private void sendRefused(IOException failure) {
        TransportDisconnectReason reason = failure instanceof DataLinkClosedException closed
                ? disconnectReason(closed.closeCause(), true) : TransportDisconnectReason.TRANSPORT_FAILED;
        endTransport(reason, failure);
    }

    /**
     * Maps how the engine says a connection ended to the reason fired.
     *
     * <p>On libwebrtc a dead peer shows up only as ICE failure, since dcSCTP
     * never gives up on retransmissions; that reads as
     * {@link TransportDisconnectReason#CONNECTION_LOST}.
     */
    static TransportDisconnectReason disconnectReason(CloseCause cause, boolean reliableChannel) {
        return switch (cause) {
            case REMOTE_CLOSED -> TransportDisconnectReason.CLOSED_BY_REMOTE_PEER;
            case REMOTE_ABORTED -> TransportDisconnectReason.ABORTED_BY_REMOTE_PEER;
            case ICE_FAILED -> TransportDisconnectReason.CONNECTION_LOST;
            case TRANSPORT_FAILED, NEGOTIATION_FAILED, REMOTE_DESCRIPTION_REJECTED ->
                    TransportDisconnectReason.TRANSPORT_FAILED;
            // A data channel that closed while its peer did not fail was closed
            // by the remote; a peer that ended without saying why failed.
            case UNKNOWN -> reliableChannel ? TransportDisconnectReason.CLOSED_BY_REMOTE_PEER
                    : TransportDisconnectReason.TRANSPORT_FAILED;
        };
    }

    /** Reports that this attempt ended before it carried traffic, at most once per attempt. */
    final void connectionFailed(NetherConnectionFailure reason) {
        NetherChannelMetrics metrics = config.getMetrics();
        if (metrics != null && failureReported.compareAndSet(false, true)) {
            try {
                metrics.connectionFailed(reason);
            } catch (RuntimeException e) {
                log.debug("NetherNet metrics failed", e);
            }
        }
    }

    /** Starts a new attempt, so its failure is reportable again. */
    final void clearFailureReported() {
        failureReported.set(false);
    }

    static NetherConnectionFailure failureOf(CloseCause cause) {
        return switch (cause) {
            case NEGOTIATION_FAILED, REMOTE_DESCRIPTION_REJECTED -> NetherConnectionFailure.NEGOTIATION_FAILED;
            case REMOTE_CLOSED, REMOTE_ABORTED -> NetherConnectionFailure.PEER_CLOSED;
            case ICE_FAILED, TRANSPORT_FAILED, UNKNOWN -> NetherConnectionFailure.PEER_FAILED;
        };
    }

    /**
     * The ICE servers a peer uses: the configured ones, then the ones the
     * signaling handed out, each once.
     */
    static List<IceServer> iceServers(List<IceServer> configured, List<IceServer> signaled) {
        return Stream.concat(configured.stream(), signaled == null ? Stream.empty() : signaled.stream())
                .distinct()
                .toList();
    }

    // ---- Remote address ----

    /** Engine thread: ICE selected a pair. Applied on the event loop, in step with the pipeline. */
    private void schedule(Runnable applyPath) {
        try {
            eventLoop().execute(applyPath);
        } catch (RejectedExecutionException | IllegalStateException closing) {
            // The loop is gone or the channel never registered; nobody reads the address anymore.
        }
    }

    /**
     * Takes in the newest pair a peer reported, unless its events went stale meanwhile. The engine
     * thread checked already, but a client can retire the peer between that check and the engine
     * thread's next step; only here, on the loop that retires peers, is the check ordered with the
     * retirement.
     */
    private void applyPath(PeerEvents from) {
        if (!eventLoop().inEventLoop()) {
            schedule(from.applyPath);
            return;
        }
        TransportPath path = from.latestPath;
        if (!isOpen() || from.stale() || path == null) {
            return;
        }
        selectedPath = path;
        InetSocketAddress remote = path.remote();
        if (remote != null && !remote.isUnresolved()) {
            followPath(path);
        }
    }

    /**
     * Event loop: the newest selected pair, whose remote side is a resolved
     * address. For a relayed pair that is the TURN relay, the transport peer
     * actually connected to us, never the address the client declared behind
     * it.
     */
    abstract void followPath(TransportPath path);

    // ---- Statistics ----

    /**
     * Reads the connection's transport metrics from the engine: the selected
     * pair's ICE round trip time and the SCTP association's counters. The
     * engine answers on its own thread, so the stage may complete on any
     * thread; do not block an event loop on it.
     *
     * <p>Before the channel has a peer (a client before it connects, or
     * between two attempts) and after it closed, the stage completes at once
     * with {@link TransportMetrics#NONE}. Before activation each part is empty
     * until it exists: the round trip time until ICE measured one, the SCTP
     * metrics until the association is up.
     *
     * @return the metrics; fails only if the engine failed to read them
     */
    public CompletionStage<TransportMetrics> transportMetrics() {
        Peer current = peer();
        if (current == null) {
            return CompletableFuture.completedStage(TransportMetrics.NONE);
        }
        try {
            // A dependent stage, so a caller cannot complete the engine's own.
            return current.metrics().thenApply(Function.identity());
        } catch (RuntimeException e) {
            return CompletableFuture.failedStage(e);
        }
    }

    /**
     * The candidate pair ICE selected last, both ends with their candidate
     * types; either address is null when the engine does not report it.
     * Empty until ICE selected a pair, which can be before activation; a
     * client's next attempt starts empty again. After close, the last pair.
     * Taken in on the event loop, in step with {@link #remoteAddress()}.
     *
     * @return the selected pair, or empty before there is one
     */
    public Optional<TransportPath> selectedPath() {
        return Optional.ofNullable(selectedPath);
    }

    /**
     * Where signaling met the peer, fixed for the channel's life. Its
     * address is a unique placeholder, with source
     * {@link io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource#UNKNOWN},
     * when signaling did not say.
     *
     * @return the signaled address
     */
    public abstract SourcedAddress signaledAddress();

    /**
     * The remote side of the newest selected pair: where the media flows. A
     * relayed pair has the TURN relay here, the transport peer actually
     * connected, never the address the peer claims behind it.
     *
     * @return the media address, or empty before ICE selected a pair with a
     *         known remote address
     */
    public abstract Optional<SourcedAddress> mediaAddress();

    /**
     * The address this channel reports, with where it came from.
     *
     * @return the reported address
     */
    public abstract SourcedAddress reportedAddress();

    // ---- Message size ----

    /**
     * Sets the largest outbound segment, header included: the
     * {@code a=max-message-size} the remote peer advertised. Zero means
     * unlimited; our own outgoing limit still applies.
     *
     * @throws IllegalArgumentException if negative or too small for a header and payload
     */
    void setMaxOutboundMessageSize(int size) {
        this.maxOutboundMessageSize = NetherNetConstants.outboundMessageSize(size);
    }

    /** The largest outbound segment, header included. */
    public int getMaxOutboundMessageSize() {
        return maxOutboundMessageSize;
    }

    @Override
    public int maxMessageSize() {
        return NetherNetConstants.MAX_SEGMENTS * (maxOutboundMessageSize - 1);
    }

    // ---- Activation ----

    /**
     * Signals that the transport can carry traffic. Safe to call from any
     * thread; fires channelActive on the event loop once the channel is also
     * registered.
     */
    protected void markTransportOpen() {
        transportOpen = true;
        if (isRegistered()) {
            eventLoop().execute(this::fireChannelActiveIfReady);
        }
    }

    protected void fireChannelActiveIfReady() {
        if (!isRegistered() || !isActive()) {
            return;
        }

        // Activation state and the sampler are owned by the event loop.
        EventLoop loop = eventLoop();
        if (!loop.inEventLoop()) {
            loop.execute(this::fireChannelActiveIfReady);
            return;
        }

        if (channelActiveFired.compareAndSet(false, true)) {
            // Started before firing active: a handler that closes the channel
            // synchronously from channelActive runs doClose first otherwise,
            // and the sampler created afterwards would never be cancelled.
            startRttSampler();
            pipeline().fireChannelActive();
        }
        requestInboundDrain();
    }

    // ---- Inbound ----

    /**
     * Engine thread: a segment arrived. The callback-scoped buffer is copied
     * once; whole messages wait for the event loop in arrival order.
     */
    final void inbound(DataLink link, ByteBuffer message) {
        // Once the transport ended the connection nothing more is admitted, on
        // either data channel: vanilla reads nothing past broken framing, and
        // for any other ending the close is already on its way. Messages that
        // arrived before still drain if the loop reaches them first.
        if (!open || inboundClosed || ending.get() != null) {
            return;
        }
        NetherChannelMetrics metrics = config.getMetrics();
        NetherNetPacket packet;
        if (link == reliableLink) {
            NetherNetMessageAssembler current = assembler;
            if (current == null) {
                return;
            }
            ByteBuf whole;
            try {
                whole = current.decode(message, config.getAllocator(), config.getMaxReassembledBytes(), metrics);
            } catch (NetherNetFramingException broken) {
                log.debug("Closing {}: {}", this, broken.getMessage());
                endTransport(broken.reason(), new IOException(broken.getMessage()));
                return;
            } catch (RuntimeException | Error e) {
                // The assembler closed on a lost segment; without an ending the connection would stall.
                endTransport(TransportDisconnectReason.TRANSPORT_FAILED,
                        new IOException("Reassembling a reliable message failed", e));
                throw e;
            }
            if (whole == null) {
                return;
            }
            packet = new NetherNetPacket(whole, true);
        } else if (link == unreliableLink) {
            ByteBuf whole = unreliableMessage(message, metrics);
            if (whole == null) {
                return;
            }
            packet = new NetherNetPacket(whole, false);
        } else {
            return;
        }
        if (metrics != null) {
            try {
                metrics.messagesIn(1);
                metrics.bytesIn(packet.content().readableBytes());
            } catch (RuntimeException e) {
                log.debug("NetherNet metrics failed", e);
            } catch (Error e) {
                // Nothing owns the message yet, so it would leak with the error.
                packet.release();
                throw e;
            }
        }
        enqueueInbound(packet);
    }

    /**
     * Decodes an unreliable message. The onboarding guide says the unreliable
     * channel does not reassemble: its header must be 0. Anything else is
     * dropped without touching the reliable stream; vanilla never sends here.
     */
    private ByteBuf unreliableMessage(ByteBuffer message, NetherChannelMetrics metrics) {
        int payload = message.remaining() - 1;
        if (payload < 1 || message.get(message.position()) != 0 || payload > config.getMaxReassembledBytes()) {
            if (metrics != null) {
                try {
                    metrics.decodeFail(1);
                } catch (RuntimeException e) {
                    log.debug("NetherNet metrics failed", e);
                }
            }
            return null;
        }
        ByteBuf copy = config.getAllocator().buffer(payload, payload);
        try {
            copy.writeBytes(message.duplicate().position(message.position() + 1));
            return copy;
        } catch (RuntimeException | Error e) {
            copy.release();
            throw e;
        }
    }

    private void enqueueInbound(NetherNetPacket packet) {
        boolean accepted = false;
        boolean overflow = false;
        synchronized (inboundLock) {
            if (open && !inboundClosed) {
                int bytes = packet.content().readableBytes();
                long maxBytes = 2L * config.getMaxReassembledBytes();
                if (bytes <= maxBytes - pendingInboundBytes
                        && (pendingInbound == null || pendingInbound.size() < MAX_PENDING_INBOUND_MESSAGES)) {
                    if (pendingInbound == null) {
                        pendingInbound = new ArrayDeque<>();
                    }
                    pendingInbound.add(packet);
                    pendingInboundBytes += bytes;
                    accepted = true;
                } else {
                    inboundClosed = true;
                    overflow = true;
                }
            }
        }
        if (!accepted) {
            packet.release();
        }
        if (overflow) {
            discardPendingInbound();
            log.warn("Closing {}: inbound backlog exceeded {} messages or {} bytes",
                    remoteAddress, MAX_PENDING_INBOUND_MESSAGES, 2L * config.getMaxReassembledBytes());
            endTransport(TransportDisconnectReason.QUEUE_TOO_LONG,
                    new IOException("NetherNet inbound backlog exceeded its limit"));
        } else if (accepted) {
            requestInboundDrain();
        }
    }

    /** Drops messages belonging to an abandoned transport attempt. */
    protected final void discardPendingInbound() {
        Queue<NetherNetPacket> pending;
        synchronized (inboundLock) {
            pending = pendingInbound;
            pendingInbound = null;
            pendingInboundBytes = 0;
        }
        if (pending != null) {
            NetherNetPacket packet;
            while ((packet = pending.poll()) != null) {
                packet.release();
            }
        }
    }

    private boolean canReadInbound() {
        return inboundReady && !inboundClosed && isRegistered() && isActive()
                && (config.isAutoRead() || readPending);
    }

    private void requestInboundDrain() {
        synchronized (inboundLock) {
            if (inboundDrainScheduled || pendingInbound == null || pendingInbound.isEmpty() || !canReadInbound()) {
                return;
            }
            inboundDrainScheduled = true;
        }
        scheduleInboundDrain();
    }

    private void scheduleInboundDrain() {
        EventLoop loop = eventLoop();
        try {
            loop.execute(inboundDrainTask);
        } catch (RejectedExecutionException e) {
            synchronized (inboundLock) {
                inboundDrainScheduled = false;
                inboundDrainRejected = true;
            }
            if (loop != eventLoop()) {
                requestInboundDrain();
            } else if (loop.isShuttingDown()) {
                discardPendingInbound();
            } else {
                // Share the recovery wakeup, without making new reads wait for it.
                wakeUp();
            }
        }
    }

    private void drainInbound() {
        // Re-registration can move the channel after this task was queued.
        if (!eventLoop().inEventLoop()) {
            scheduleInboundDrain();
            return;
        }
        inboundDrainRejected = false;
        int messages = 0;
        try {
            fireChannelActiveIfReady();
            while (messages < MAX_MESSAGES_PER_DRAIN && canReadInbound()) {
                NetherNetPacket packet;
                synchronized (inboundLock) {
                    packet = pendingInbound == null ? null : pendingInbound.poll();
                    if (packet == null) {
                        break;
                    }
                    pendingInboundBytes -= packet.content().readableBytes();
                }
                readPending = false;
                pipeline().fireChannelRead(packet.reliable() ? packet.content() : packet);
                messages++;
                if (!config.isAutoRead()) {
                    break;
                }
            }
            if (messages != 0) {
                pipeline().fireChannelReadComplete();
            }
        } finally {
            synchronized (inboundLock) {
                inboundDrainScheduled = false;
            }
            requestInboundDrain();
        }
    }

    // ---- Outbound ----

    @Override
    protected Object filterOutboundMessage(Object msg) {
        ByteBuf content;
        boolean reliable;
        if (msg instanceof NetherNetPacket packet) {
            content = packet.content();
            reliable = packet.reliable();
        } else if (msg instanceof ByteBuf buffer) {
            content = buffer;
            reliable = true;
        } else {
            throw new UnsupportedOperationException("NetherNet writes take a ByteBuf or a NetherNetPacket");
        }
        // Refused before it is queued, so the write fails and nothing is sent.
        int maxPayload = maxOutboundMessageSize - 1;
        int length = content.readableBytes();
        int segments;
        if (reliable) {
            segments = NetherNetConstants.segmentCount(length, maxPayload);
        } else if (length > maxPayload) {
            throw new IllegalArgumentException("An unreliable message of " + length
                    + " bytes is more than the one segment of " + maxPayload + " it may take");
        } else {
            segments = length == 0 ? 0 : 1;
        }
        PendingWrite write = new PendingWrite(this, content, reliable, segments, maxPayload);
        write.previous = unsubmittedTail;
        if (unsubmittedTail == null) {
            unsubmittedHead = write;
        } else {
            unsubmittedTail.next = write;
        }
        unsubmittedTail = write;
        return write;
    }

    private void removeUnsubmitted(PendingWrite write) {
        if (write.previous == null && unsubmittedHead != write) {
            return;
        }
        if (flushedWriteTail == write) {
            flushedWriteTail = write.previous;
        }
        if (write.previous == null) {
            unsubmittedHead = write.next;
        } else {
            write.previous.next = write.next;
        }
        if (write.next == null) {
            unsubmittedTail = write.previous;
        } else {
            write.next.previous = write.previous;
        }
        write.previous = null;
        write.next = null;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        if (removingCompletedWrites) {
            // The outer drain submits any writes added by completion listeners.
            return;
        }
        removeCompletedWrites(in);
        if (!isOpen() || closeIfEnded()) {
            return;
        }
        // Engine acceptance and a queued recovery task must not gate an explicit flush.
        while (flushedWriteTail != null && isOpen()) {
            PendingWrite write = unsubmittedHead;
            DataLink link = write.reliable ? reliableLink : unreliableLink;
            if (link == null) {
                write.accept(new IllegalStateException("The NetherNet "
                        + (write.reliable ? "reliable" : "unreliable") + " data channel is unavailable"));
                closeIfEnded();
                return;
            }
            // Paced per segment: one message can be far larger than the engine queue.
            while (write.hasNextSegment()) {
                if (sendPaused(link, in)) {
                    return;
                }
                write.sendNextSegment(link, alloc());
                if (closeIfEnded()) {
                    return;
                }
            }
            removeUnsubmitted(write);
            countSent(write);
            write.submitted();
            if (closeIfEnded()) {
                return;
            }
        }
    }

    private void countSent(PendingWrite write) {
        NetherChannelMetrics metrics = config.getMetrics();
        if (metrics == null || write.segments == 0) {
            return;
        }
        try {
            metrics.messagesOut(1);
            metrics.bytesOut(write.content.readableBytes());
            if (write.segments > 1) {
                metrics.fragmentsOut(write.segments - 1);
            }
        } catch (RuntimeException e) {
            log.debug("NetherNet metrics failed", e);
        }
    }

    /**
     * True while the data channel's engine queue holds too much to take
     * another segment. The rest stays in Netty's outbound buffer until the
     * engine reports the queue low. A remote peer that drains nothing while
     * the Java backlog also passes its cap ends the connection.
     */
    private boolean sendPaused(DataLink link, ChannelOutboundBuffer in) {
        int pauseBytes = config.getSendPauseBytes();
        if (link.queuedBytes() < pauseBytes) {
            return false;
        }
        // Resume at a quarter of the pause level.
        long mark = pauseBytes / 4;
        link.lowWatermark(mark);
        resumeMark = mark;
        writesPaused = true;
        if (link.queuedBytes() <= mark) {
            writesPaused = false;
            return false;
        }
        in.setUserDefinedWritability(SEND_PAUSE_WRITABILITY, false);
        if (in.totalPendingWriteBytes() > config.getMaxQueuedBytes()) {
            log.warn("Closing {}: peer cannot keep up ({} bytes in the engine queue, {} bytes waiting)",
                    remoteAddress, link.queuedBytes(), in.totalPendingWriteBytes());
            endTransport(TransportDisconnectReason.QUEUE_TOO_LONG, new IOException(
                    "More than " + config.getMaxQueuedBytes() + " bytes waited to be sent"));
            closeIfEnded();
        }
        return true;
    }

    /** Engine thread: a data channel's engine queue fell to its low watermark. */
    final void queueLow(DataLink link) {
        if (writesPaused && link.queuedBytes() <= resumeMark) {
            writesPaused = false;
            wakeUp();
        }
    }

    /**
     * Ends the connection for a reason the transport found. Safe from any
     * thread: the close runs on the event loop through the shared wakeup,
     * which also recovers from a rejected task. The first ending wins.
     */
    final void endTransport(TransportDisconnectReason reason, IOException cause) {
        if (ending.compareAndSet(null, new Ending(reason, cause))) {
            wakeUp();
        }
    }

    /** Event loop only: closes for the recorded ending, if any. */
    private boolean closeIfEnded() {
        Ending end = ending.get();
        if (end == null || !isOpen()) {
            return false;
        }
        ((NetherNetUnsafe) unsafe()).closeForEnding(end);
        return true;
    }

    /**
     * Wakes the event loop to complete accepted writes, resume paused ones,
     * recover a rejected inbound drain and close for an ending.
     */
    private void wakeUp() {
        if (isOpen() && wakeUpScheduled.compareAndSet(false, true)) {
            scheduleWakeUp();
        }
    }

    private void scheduleWakeUp() {
        EventLoop loop = eventLoop();
        try {
            loop.execute(wakeUpTask);
        } catch (RejectedExecutionException cause) {
            if (loop != eventLoop()) {
                scheduleWakeUp();
            } else if (!loop.isShuttingDown()) {
                // Recover even if a quiet connection produces no further events.
                // Explicit flushes can submit writes while this retry is pending.
                GlobalEventExecutor.INSTANCE.schedule(() -> {
                    if (isOpen()) {
                        scheduleWakeUp();
                    }
                }, 10, TimeUnit.MILLISECONDS);
            } else {
                // The shutdown hook closes on the owner thread, never here on
                // an engine thread or after that owner has terminated.
                wakeUpScheduled.set(false);
            }
        }
    }

    private void onWakeUp() {
        if (!eventLoop().inEventLoop()) {
            scheduleWakeUp();
            return;
        }
        wakeUpScheduled.set(false);
        if (!isOpen()) {
            return;
        }
        if (inboundDrainRejected) {
            requestInboundDrain();
        }
        ChannelOutboundBuffer in = unsafe().outboundBuffer();
        if (in == null) {
            return;
        }
        removeCompletedWrites(in);
        // Retrying a refused segment after later ones were submitted could
        // corrupt the reliable stream, so a failure ends the connection.
        if (closeIfEnded()) {
            return;
        }
        if (!writesPaused) {
            in.setUserDefinedWritability(SEND_PAUSE_WRITABILITY, true);
        }
        if (isOpen() && flushedWriteTail != null) {
            ((NetherNetUnsafe) unsafe()).flushPendingWrites();
        }
    }

    private void removeCompletedWrites(ChannelOutboundBuffer in) {
        removingCompletedWrites = true;
        try {
            Object msg;
            while (isOpen() && (msg = in.current()) != null) {
                if (msg instanceof PendingWrite write && write.result != PendingWrite.SUCCESS) {
                    break;
                }
                // Netty replaces cancelled entries with an empty ByteBuf.
                in.remove();
            }
        } finally {
            removingCompletedWrites = false;
        }
    }

    @Override
    protected abstract NetherNetUnsafe newUnsafe();

    protected abstract class NetherNetUnsafe extends AbstractUnsafe {
        @Override
        protected void flush0() {
            // Netty has marked the current writes uncancellable before reaching here.
            flushedWriteTail = unsubmittedTail;
            super.flush0();
        }

        private void flushPendingWrites() {
            // Bypass the explicit-flush boundary update when resuming existing work.
            super.flush0();
        }

        /**
         * Fires the reason first: a write on a void promise reports its
         * failure as an exception, and a handler that closes on it would
         * otherwise never learn why the connection ended.
         */
        private void closeForEnding(Ending end) {
            if (closingForEnding) {
                return;
            }
            closingForEnding = true;
            if (channelActiveFired.get()) {
                pipeline().fireUserEventTriggered(end.reason());
            }
            closeWithFailure(end.cause());
        }

        private void closeWithFailure(IOException failure) {
            if (!isOpen()) {
                return;
            }
            ClosedChannelException closed = new ClosedChannelException();
            closed.initCause(failure);
            close(voidPromise(), failure, closed);
        }
    }

    /**
     * One message in Netty's outbound buffer. Its segments go to the engine
     * one by one, and the write completes once the engine accepted every
     * segment. Results may arrive on any thread.
     */
    private static final class PendingWrite extends AbstractReferenceCounted
            implements ByteBufHolder, java.util.function.Consumer<Throwable> {
        private static final Object SUCCESS = new Object();
        private static final AtomicReferenceFieldUpdater<PendingWrite, Object> RESULT =
                AtomicReferenceFieldUpdater.newUpdater(PendingWrite.class, Object.class, "result");
        private static final AtomicReferenceFieldUpdater<PendingWrite, IOException> FAILURE =
                AtomicReferenceFieldUpdater.newUpdater(PendingWrite.class, IOException.class, "failure");
        private final NetherNetChannel channel;
        private final boolean reliable;
        private final int segments;
        private final int maxPayload;
        private ByteBuf content;
        private int nextSegment;
        // One for each segment handed over and not yet settled, plus one for
        // the submission itself, dropped once every segment was handed over.
        private final AtomicInteger unsettled = new AtomicInteger(1);
        private volatile IOException failure;
        private volatile Object result;
        private PendingWrite previous;
        private PendingWrite next;

        private PendingWrite(NetherNetChannel channel, ByteBuf content, boolean reliable, int segments, int maxPayload) {
            this.channel = channel;
            this.content = content;
            this.reliable = reliable;
            this.segments = segments;
            this.maxPayload = maxPayload;
        }

        private boolean hasNextSegment() {
            return nextSegment < segments;
        }

        /** Hands the next segment, with its countdown header, to the engine. Event loop only. */
        private void sendNextSegment(DataLink link, ByteBufAllocator allocator) {
            int offset = nextSegment * maxPayload;
            int chunk = Math.min(maxPayload, content.readableBytes() - offset);
            int countdown = segments - 1 - nextSegment;
            nextSegment++;
            unsettled.incrementAndGet();
            ByteBuf segment = null;
            try {
                segment = allocator.directBuffer(1 + chunk, 1 + chunk);
                segment.writeByte(countdown);
                segment.writeBytes(content, content.readerIndex() + offset, chunk);
                // The engine copies the segment before send returns.
                link.send(segment.nioBuffer(), this);
            } catch (Throwable cause) {
                accept(cause);
            } finally {
                if (segment != null) {
                    segment.release();
                }
            }
        }

        /** Every segment was handed over; the engine holds its own copies. */
        private void submitted() {
            releaseContent();
            settle(null);
        }

        @Override
        public ByteBuf content() {
            if (refCnt() == 0) {
                throw new IllegalReferenceCountException(0);
            }
            return content;
        }

        private void releaseContent() {
            ByteBuf buffer = content;
            content = Unpooled.EMPTY_BUFFER;
            buffer.release();
        }

        @Override
        protected void deallocate() {
            // Cancellation or a failed size estimate can release a write before submission.
            channel.removeUnsubmitted(this);
            releaseContent();
        }
        @Override public ByteBufHolder copy() { return replace(content().copy()); }
        @Override public ByteBufHolder duplicate() { return replace(content().duplicate()); }
        @Override public ByteBufHolder retainedDuplicate() { return replace(content().retainedDuplicate()); }
        @Override public ByteBufHolder replace(ByteBuf buffer) { return new DefaultByteBufHolder(buffer); }
        @Override public PendingWrite retain() { super.retain(); return this; }
        @Override public PendingWrite retain(int increment) { super.retain(increment); return this; }
        @Override public PendingWrite touch() { content().touch(); return this; }
        @Override public PendingWrite touch(Object hint) { content().touch(hint); return this; }

        /** A segment's result from the engine: null when accepted, else why not. Any thread. */
        @Override
        public void accept(Throwable cause) {
            settle(cause);
        }

        private void settle(Throwable cause) {
            if (!channel.isOpen()) {
                // The close already failed this write.
                return;
            }
            if (cause != null && result == null) {
                IOException io = cause instanceof IOException e ? e
                        : new IOException("Failed to send NetherNet message", cause);
                if (FAILURE.compareAndSet(this, null, io)) {
                    channel.sendRefused(io);
                }
            }
            if (unsettled.decrementAndGet() != 0) {
                return;
            }
            IOException failed = failure;
            if (RESULT.compareAndSet(this, null, failed == null ? SUCCESS : failed)) {
                channel.wakeUp();
            }
        }
    }

    // ---- Registration, close, round trip time ----

    @Override
    protected void doRegister(ChannelPromise promise) {
        try {
            if (eventLoop() instanceof SingleThreadEventExecutor executor) {
                executor.addShutdownHook(shutdownHook);
            }
            if (isActive()) {
                // Netty's register flow fires channelActive itself for already
                // active channels; pre set the flag so it is not fired twice, and
                // start the sampler here since fireChannelActiveIfReady's CAS
                // will never win on this path.
                channelActiveFired.set(true);
                startRttSampler();
            }
            // Handler initialization and Netty's initial channelActive run once the promise succeeds.
            eventLoop().execute(() -> {
                if (!isOpen()) {
                    return;
                }
                inboundReady = true;
                fireChannelActiveIfReady();
            });
        } catch (Throwable cause) {
            promise.setFailure(cause);
            return;
        }
        promise.setSuccess();
    }

    @Override
    protected void doDeregister() throws Exception {
        inboundReady = false;
        if (eventLoop() instanceof SingleThreadEventExecutor executor) {
            executor.removeShutdownHook(shutdownHook);
        }
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        throw new UnsupportedOperationException("NetherNetChannel cannot be bound directly");
    }

    @Override
    protected void doDisconnect() throws Exception {
        doClose();
    }

    /** The ICE round trip time in milliseconds, sampled every 3 s, or 0 while unknown. */
    @Override
    public long getPing() {
        return Math.max(rttMillis, 0);
    }

    // Runs on the event loop, once, when the channel goes active.
    private void startRttSampler() {
        if (rttSampler != null || !isOpen()) {
            return;
        }
        rttSampler = eventLoop().scheduleAtFixedRate(this::sampleRtt, 1, 3, TimeUnit.SECONDS);
    }

    private void sampleRtt() {
        Peer current = peer();
        if (current == null) {
            return;
        }
        current.metrics().whenComplete((metrics, failure) -> {
            if (metrics != null) {
                metrics.iceRoundTripTime().ifPresent(rtt -> rttMillis = Math.round(rtt.toNanos() / 1_000_000.0));
            }
        });
    }

    @Override
    protected void doClose() throws Exception {
        Peer current;
        CompletableFuture<Void> retired;
        synchronized (peerLock) {
            this.open = false;
            current = peer;
            peer = null;
            retired = retiredPeers;
        }
        this.writesPaused = false;
        while (unsubmittedHead != null) {
            removeUnsubmitted(unsubmittedHead);
        }
        inboundReady = false;
        inboundClosed = true;
        readPending = false;
        discardPendingInbound();
        unbindLinks();

        if (rttSampler != null) {
            rttSampler.cancel(false);
            rttSampler = null;
        }
        CompletableFuture<Void> closing = current == null ? CompletableFuture.completedFuture(null)
                : closeQuietly(current);
        CompletableFuture.allOf(closing, retired).whenComplete((ignored, failure) -> termination.complete(null));
    }

    @Override
    protected void doBeginRead() throws Exception {
        readPending = !config.isAutoRead();
        requestInboundDrain();
    }

    /** Clears read demand when the channel configuration disables automatic reads. */
    public final void clearReadPending() {
        if (!isRegistered() || eventLoop().inEventLoop()) {
            readPending = false;
        } else {
            try {
                // Order this after any automatic read already queued by a configuration change.
                eventLoop().execute(clearReadPendingTask);
            } catch (RejectedExecutionException ignored) {
                readPending = false;
            }
        }
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return true;
    }

    @Override
    protected SocketAddress localAddress0() {
        return this.localAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return this.remoteAddress;
    }

    /**
     * Where the peer is. Before ICE selects a pair, the address signaling saw,
     * or a unique placeholder when it saw none. From then on the remote side
     * of the newest selected pair, which for a relayed connection is the TURN
     * relay; a server's child moves to the pair only when it goes active, and
     * fires a {@link io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressChangedEvent}
     * for each later move. A client that dialed a network ID keeps it.
     */
    @Override
    public SocketAddress remoteAddress() {
        // AbstractChannel caches the first read, which an initializer makes before ICE connects.
        return this.remoteAddress;
    }

    @Override
    public ChannelConfig config() {
        return this.config;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public boolean isActive() {
        return isOpen() && transportOpen;
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }
}

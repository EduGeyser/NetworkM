package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.codec.NetherNetServerDataCodec;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertiser;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanDiscoveryRequest;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetSignaling.SignalHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongPredicate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class NetherNetDiscovery extends SimpleChannelInboundHandler<DatagramPacket> {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetDiscovery.class);

    private final long networkId;
    private final Map<String, ClientHandler> signalHandlers = new ConcurrentHashMap<>();
    private final Map<JoinKey, JoinRoute> joins = new ConcurrentHashMap<>();
    private final DiscoveryPeerRegistry peerAddresses;
    private final Object lifecycleLock = new Object();
    private final Supplier<? extends EventLoopGroup> eventLoopFactory;
    private final AtomicReference<DiscoveryCallback> discoveryCallback = new AtomicReference<>();
    private EventLoopGroup eventLoops;
    private volatile Channel channel;
    private final LanAdvertisements advertisements;
    /** Runs the advertiser's calls when the creator gave no executor; closed with this instance. */
    private final ExecutorService ownedAdvertiserExecutor;
    private volatile OfferHandler offerHandler;
    private volatile boolean closed;

    /** Receives offers for connection IDs that have no signal handler yet. */
    @FunctionalInterface
    interface OfferHandler {
        /**
         * @param connectionId the connection ID the client chose
         * @param senderId the client's network ID
         * @param offer the offer SDP
         * @param sender where the datagram came from
         */
        void onOffer(String connectionId, long senderId, String offer, InetSocketAddress sender);
    }

    /**
     * A join is told apart by its client's network ID too, so one client cannot reach another's
     * join by its connection ID.
     */
    private record JoinKey(long senderId, String connectionId) { }

    /** Where a join's offer came from, pinned then, and where its later signals go. */
    private record JoinRoute(InetSocketAddress sender, SignalHandler handler) { }

    /**
     * Creates discovery with its own event loop, bound later with {@link #bind}.
     *
     * @param networkId the network ID this side announces
     */
    public NetherNetDiscovery(long networkId) {
        this(networkId, () -> new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()));
    }

    NetherNetDiscovery(long networkId, Supplier<? extends EventLoopGroup> eventLoopFactory) {
        this(networkId, eventLoopFactory, new DiscoveryPeerRegistry());
    }

    NetherNetDiscovery(long networkId, Supplier<? extends EventLoopGroup> eventLoopFactory, DiscoveryPeerRegistry peerAddresses) {
        this(networkId, eventLoopFactory, peerAddresses, LanAdvertisements.ADVERTISER_TIMEOUT, null);
    }

    /**
     * @param advertiserTimeout how long one {@link LanAdvertiser} call may take
     * @param advertiserExecutor runs the advertiser's calls, or null for a virtual thread each
     */
    NetherNetDiscovery(long networkId, Supplier<? extends EventLoopGroup> eventLoopFactory,
                       DiscoveryPeerRegistry peerAddresses, Duration advertiserTimeout, Executor advertiserExecutor) {
        this.networkId = networkId;
        this.eventLoopFactory = Objects.requireNonNull(eventLoopFactory, "eventLoopFactory");
        this.peerAddresses = Objects.requireNonNull(peerAddresses, "peerAddresses");
        this.ownedAdvertiserExecutor = advertiserExecutor == null
                ? Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("nethernet-lan-advertiser-", 0).factory())
                : null;
        this.advertisements = new LanAdvertisements(advertiserTimeout,
                advertiserExecutor != null ? advertiserExecutor : ownedAdvertiserExecutor);
    }

    public void bind() {
        bind(NetherNetConstants.DISCOVERY_PORT);
    }

    public void bind(int port) {
        bind(new InetSocketAddress(port));
    }

    public void bind(InetSocketAddress address) {
        Objects.requireNonNull(address, "address");
        EventLoopGroup group;
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("Discovery is closed");
            }
            if (eventLoops != null) {
                throw new IllegalStateException("Discovery is already bound or binding");
            }
            group = Objects.requireNonNull(eventLoopFactory.get(), "eventLoopFactory returned null");
            eventLoops = group;
        }

        ChannelFuture bind = null;
        boolean bound = false;
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
             .channel(NioDatagramChannel.class)
             .option(ChannelOption.SO_BROADCAST, true)
             .option(ChannelOption.RECVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(65535))
             .handler(this);

            bind = bootstrap.bind(address);
            synchronized (lifecycleLock) {
                if (closed) {
                    throw new IllegalStateException("Discovery closed during bind");
                }
                channel = bind.channel();
            }
            bind.sync();
            synchronized (lifecycleLock) {
                if (closed) {
                    throw new IllegalStateException("Discovery closed during bind");
                }
                bound = true;
            }
            log.info("NetherNet Discovery listening on {}", channel.localAddress());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while binding discovery", e);
        } finally {
            if (!bound) {
                try {
                    if (bind != null) {
                        bind.channel().close();
                    }
                } finally {
                    close();
                }
            }
        }
    }

    /**
     * Sends a discovery request. The callback receives every response
     * until replaced or closed and must release each payload buffer it receives.
     */
    public void sendDiscoveryRequest(InetSocketAddress target, BiConsumer<Long, ByteBuf> onServerFound) {
        sendDiscoveryRequest(target, onServerFound, null);
    }

    void sendDiscoveryRequestToPeer(InetSocketAddress target, BiConsumer<Long, ByteBuf> onServerFound) {
        sendDiscoveryRequest(target, onServerFound, Objects.requireNonNull(target, "target"));
    }

    private void sendDiscoveryRequest(InetSocketAddress target, BiConsumer<Long, ByteBuf> onServerFound,
                                      InetSocketAddress expectedSender) {
        DiscoveryCallback callback = new DiscoveryCallback(
                Objects.requireNonNull(onServerFound, "onServerFound"), expectedSender);
        this.discoveryCallback.set(callback);

        ByteBuf buf = Unpooled.buffer();
        buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_REQUEST);
        buf.writeLongLE(this.networkId);
        buf.writeZero(8); // Padding

        try {
            sendPacket(buf, target);
        } catch (RuntimeException e) {
            discoveryCallback.compareAndSet(callback, null);
            throw e;
        }
    }

    void clearDiscoveryCallback(BiConsumer<Long, ByteBuf> callback) {
        DiscoveryCallback current = discoveryCallback.get();
        if (current != null && current.consumer() == callback) {
            discoveryCallback.compareAndSet(current, null);
        }
    }

    private record DiscoveryCallback(BiConsumer<Long, ByteBuf> consumer, InetSocketAddress expectedSender) { }

    /**
     * Answers every discovery request with this server data, encoded once here. Replaces an
     * {@linkplain #setAdvertiser advertiser}. Only v7 is advertised: current clients do not send
     * offers to a server that answers in an older version (claimed by upstream for 1.26.51
     * clients; see {@link NetherNetServerDataCodec}).
     *
     * @param data the advertisement
     * @throws IllegalArgumentException if the encoded advertisement does not fit one datagram;
     *         the previous advertisement stays in place
     */
    public void setServerData(LanServerData.V7 data) {
        advertisements.fix(data);
    }

    /**
     * Asks the advertiser what to answer each discovery request with, once the request passed
     * decryption and validation. Replaces fixed {@linkplain #setServerData server data}. See
     * {@link LanAdvertiser} for its deadline and what goes out when it does not deliver.
     *
     * @param advertiser the advertiser
     */
    public void setAdvertiser(LanAdvertiser advertiser) {
        advertisements.ask(advertiser);
    }

    /**
     * @param connectionId the client attempt's connection ID
     * @param sender       which sender Network IDs the attempt's signals may come from; signals with
     *                     this connection ID from any other sender do not reach the handler
     * @param handler      receives the attempt's signals
     */
    public void registerSignalHandler(String connectionId, LongPredicate sender, SignalHandler handler) {
        Objects.requireNonNull(sender, "sender");
        Objects.requireNonNull(handler, "handler");
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("Discovery is closed");
            }
            this.signalHandlers.put(connectionId, new ClientHandler(sender, handler));
            this.peerAddresses.register(connectionId);
        }
    }

    /** A client attempt's handler and the senders its signals may come from. */
    private record ClientHandler(LongPredicate sender, SignalHandler handler) {
    }

    public void unregisterSignalHandler(String connectionId) {
        synchronized (lifecycleLock) {
            this.signalHandlers.remove(connectionId);
            this.peerAddresses.unregister(connectionId);
        }
    }

    /**
     * Routes a client's later signals for one join to the handler: only those from the network ID
     * and the address its offer came from. Keeps the client in the peer table until
     * {@link #unregisterJoin}.
     *
     * @param senderId the client's network ID
     * @param connectionId the connection ID the client chose
     * @param sender where the offer came from
     * @param handler receives the signals on the discovery's event loop
     * @return false if this client already has a join with this connection ID
     * @throws IllegalStateException if discovery is closed
     */
    boolean registerJoin(long senderId, String connectionId, InetSocketAddress sender, SignalHandler handler) {
        JoinRoute route = new JoinRoute(Objects.requireNonNull(sender, "sender"),
                Objects.requireNonNull(handler, "handler"));
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("Discovery is closed");
            }
            if (joins.putIfAbsent(new JoinKey(senderId, connectionId), route) != null) {
                return false;
            }
            peerAddresses.hold(senderId, sender);
            return true;
        }
    }

    /** Stops routing a join's signals and lets its client leave the peer table. */
    void unregisterJoin(long senderId, String connectionId) {
        synchronized (lifecycleLock) {
            if (joins.remove(new JoinKey(senderId, connectionId)) != null) {
                peerAddresses.release(senderId);
            }
        }
    }

    void setOfferHandler(OfferHandler handler) {
        this.offerHandler = handler;
    }

    // Offers carry the client's identity token, so logs name a signal by its type only.
    private static String signalType(String signal) {
        int space = signal.indexOf(' ');
        return space < 0 ? "malformed" : signal.substring(0, Math.min(space, 16));
    }

    /**
     * Sends a signal immediately and schedules it to be resent periodically
     * until the returned ScheduledFuture is cancelled.
     */
    public ScheduledFuture<?> sendSignalRetrying(InetSocketAddress recipient, long targetNetworkId, String data, long delayMs) {
        return channel.eventLoop().scheduleAtFixedRate(() -> {
            log.debug("Resending a {} signal to {}", signalType(data), recipient);
            sendSignal(recipient, targetNetworkId, data);
        }, 0, delayMs, TimeUnit.MILLISECONDS);
    }

    public ChannelFuture sendSignal(InetSocketAddress recipient, long targetNetworkId, String data) {
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Unpooled.buffer();
        buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_MESSAGE);
        buf.writeLongLE(this.networkId); // Sender ID
        buf.writeZero(8); // Padding

        buf.writeLongLE(targetNetworkId); // Recipient ID
        buf.writeIntLE(dataBytes.length);
        buf.writeBytes(dataBytes);

        return sendPacket(buf, recipient);
    }

    // New sendSignal looking up Address from ID
    public void sendSignal(long targetNetworkId, String data) {
        InetSocketAddress recipient = peerAddresses.get(targetNetworkId);
        if (recipient != null) {
            sendSignal(recipient, targetNetworkId, data);
        } else {
            throw new IllegalArgumentException("Attempted to send signal to unknown peer: " + targetNetworkId);
        }
    }

    private ChannelFuture sendPacket(ByteBuf packetData, InetSocketAddress target) {
        try {
            Objects.requireNonNull(target, "target");
            Channel channel = this.channel;
            if (closed || channel == null || !channel.isActive()) {
                throw new IllegalStateException("Discovery is not bound");
            }
            byte[] encrypted = NetherNetConstants.encryptDiscoveryPacket(packetData);
            return channel.writeAndFlush(new DatagramPacket(Unpooled.wrappedBuffer(encrypted), target));
        } catch (Exception e) {
            throw new RuntimeException("Failed to send discovery packet", e);
        } finally {
            packetData.release();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        if (closed) {
            return;
        }
        ByteBuf content = packet.content();
        ByteBuf decrypted = null;
        try {
            decrypted = NetherNetConstants.decryptDiscoveryPacket(content);
        } catch (Exception e) {
            log.debug("Failed to decrypt discovery packet from {}", packet.sender(), e);
            return;
        }

        if (decrypted == null) {
            log.debug("Received invalid discovery packet from {}", packet.sender());
            return;
        }

        try {
            if (!decrypted.isReadable(18)) {
                return;
            }
            int packetId = decrypted.readUnsignedShortLE();
            long senderId = decrypted.readLongLE();

            decrypted.skipBytes(8); // Padding

            if (senderId == this.networkId) {
                log.debug("Ignoring own discovery packet");
                return;
            }

            switch (packetId) {
                case NetherNetConstants.ID_DISCOVERY_REQUEST -> {
                    if (decrypted.isReadable() || !rememberPeer(senderId, packet.sender(), null)) {
                        return;
                    }
                    log.trace("Handled discovery request from {}", packet.sender());
                    handleRequest(ctx, senderId, packet.sender());
                }
                case NetherNetConstants.ID_DISCOVERY_MESSAGE -> {
                    log.trace("Handled discovery message from {}", packet.sender());
                    if (log.isTraceEnabled()) {
                        log.trace("Message Data: {}", decrypted.toString(StandardCharsets.UTF_8));
                    }
                    handleMessage(decrypted, senderId, packet.sender());
                }
                case NetherNetConstants.ID_DISCOVERY_RESPONSE -> {
                    log.trace("Handled discovery response from {}", packet.sender());
                    DiscoveryCallback callback = discoveryCallback.get();
                    if (callback != null && (callback.expectedSender() == null
                            || callback.expectedSender().equals(packet.sender()))
                            && validResponse(decrypted) && rememberPeer(senderId, packet.sender(), null)) {
                        if (log.isTraceEnabled()) {
                            log.trace("Response Data: {}", decrypted.toString(StandardCharsets.UTF_8));
                        }
                        // Pass the payload (decrypted buffer) to the callback
                        // We retain it because we are passing it out of the pipeline handler
                        callback.consumer().accept(senderId, decrypted.retain());
                    }
                }
                default -> {
                    log.debug("Received unknown discovery packet ID {} from {}", packetId, packet.sender());
                }
            }
        } catch (Exception e) {
            log.debug("Error processing discovery packet from {}", packet.sender(), e);
        } finally {
            decrypted.release();
        }
    }

    private void handleRequest(ChannelHandlerContext ctx, long senderId, InetSocketAddress sender) {
        advertisements.answer(new LanDiscoveryRequest(sender, Long.toUnsignedString(senderId)), ctx.executor())
                .thenAccept(payload -> payload.ifPresent(serverData -> respond(serverData, sender)));
    }

    /** Sends a discovery response; any thread. */
    private void respond(byte[] serverData, InetSocketAddress sender) {
        if (closed) {
            return;
        }
        ByteBuf buf = Unpooled.buffer(18 + serverData.length);
        buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_RESPONSE);
        buf.writeLongLE(this.networkId);
        buf.writeZero(8);
        buf.writeBytes(serverData);
        try {
            sendPacket(buf, sender);
        } catch (RuntimeException e) {
            log.debug("Could not answer the discovery request from {}: {}", sender, e.toString());
        }
    }

    private void handleMessage(ByteBuf data, long senderId, InetSocketAddress sender) {
        if (!data.isReadable(12)) {
            return;
        }
        long recipientId = data.readLongLE();

        if (recipientId != this.networkId && recipientId != 0) {
            log.trace("Ignoring message intended for {}, but I am {}", recipientId, this.networkId);
            return;
        }

        int len = data.readIntLE();
        if (len < 0 || data.readableBytes() < len) {
            log.trace("Malformed message: claimed length {} but only has {}", len, data.readableBytes());
            return;
        }

        // Vanilla can understate this inner length; the outer envelope already bounds the full datagram.
        String messageData = data.readCharSequence(data.readableBytes(), StandardCharsets.UTF_8).toString();
        if ("Ping".equals(messageData)) {
            rememberPeer(senderId, sender, null);
            return;
        }

        Signal signal = Signal.parse(messageData).orElse(null);
        if (signal == null) {
            return;
        }
        String type = signal.type();
        if (!NetherNetConstants.RTC_NEGOTIATION_CONNECT_REQUEST.equals(type)
                && !NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE.equals(type)
                && !NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD.equals(type)
                && !NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR.equals(type)) {
            return;
        }
        if (!NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR.equals(type) && signal.payload().isEmpty()) {
            return;
        }
        String connectionId = signal.connectionId();
        // A signal with a client attempt's ID from another sender is not that attempt's; it may still
        // be an offer of its own, so it goes on to join routing.
        ClientHandler client = signalHandlers.get(connectionId);
        if (client != null && client.sender().test(senderId)) {
            if (rememberPeer(senderId, sender, connectionId)) {
                client.handler().onSignal(signal);
            }
            return;
        }

        JoinRoute join = joins.get(new JoinKey(senderId, connectionId));
        if (join != null) {
            if (!join.sender().equals(sender)) {
                log.debug("Ignoring a signal for join {} of {} from {}: its offer came from {}",
                        connectionId, Long.toUnsignedString(senderId), sender, join.sender());
            } else if (rememberPeer(senderId, sender, null)) {
                // A repeated offer reaches the join too, which ignores it.
                join.handler().onSignal(signal);
            }
            return;
        }

        if (NetherNetConstants.RTC_NEGOTIATION_CONNECT_REQUEST.equals(type)) {
            OfferHandler offers = offerHandler;
            if (offers == null) {
                log.debug("Received a CONNECTREQUEST, but nothing takes offers");
            } else if (rememberPeer(senderId, sender, null)) {
                log.trace("Dispatching an offer: ID={} Sender={}", connectionId, Long.toUnsignedString(senderId));
                offers.onOffer(connectionId, senderId, signal.payload(), sender);
            }
        } else {
            log.debug("Unhandled signal type: {}", type);
        }
    }

    private boolean rememberPeer(long peerId, InetSocketAddress address, String connectionId) {
        synchronized (lifecycleLock) {
            if (closed) {
                return false;
            }
            if (connectionId == null) {
                peerAddresses.remember(peerId, address);
            } else {
                peerAddresses.rememberSignal(connectionId, peerId, address);
            }
            return true;
        }
    }

    private static boolean validResponse(ByteBuf data) {
        if (!data.isReadable(4)) {
            return false;
        }
        int index = data.readerIndex();
        int length = data.getIntLE(index);
        if (length < 0 || length != data.readableBytes() - 4 || (length & 1) != 0) {
            return false;
        }
        for (int i = index + 4; i < data.writerIndex(); i++) {
            int value = data.getUnsignedByte(i);
            if (!(value >= '0' && value <= '9') && !(value >= 'a' && value <= 'f')
                    && !(value >= 'A' && value <= 'F')) {
                return false;
            }
        }
        return true;
    }

    /** Closes the socket and its event loop. A closed instance cannot be rebound. */
    public void close() {
        Channel channel;
        EventLoopGroup group;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            channel = this.channel;
            group = eventLoops;
            signalHandlers.clear();
            joins.clear();
            peerAddresses.clear();
        }
        discoveryCallback.set(null);
        offerHandler = null;
        try {
            if (channel != null) {
                channel.close();
            }
        } finally {
            if (ownedAdvertiserExecutor != null) {
                ownedAdvertiserExecutor.shutdownNow();
            }
            if (group != null) {
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
            }
        }
    }

    public boolean isActive() {
        Channel channel = this.channel;
        return !closed && channel != null && channel.isActive();
    }
}

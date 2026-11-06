package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * NetherNet's HTTP signaling front end: the direct connection model that
 * replaces RakNet for updated clients, per Mojang's NetherNet onboarding
 * guide. Listens on TCP under the same port RakNet serves on UDP; the client
 * probes it before falling back to RakNet, so every failure mode here lands
 * on a working RakNet join.
 *
 * Endpoints:
 * - GET /v1/join: any 2xx means NetherNet is supported and the client
 *   proceeds with the SDP exchange. Carries the
 *   {@link NetherNetServerStatus} document when a supplier is set, which
 *   vanilla 1.26.50 and later answer with as their equivalent of the RakNet
 *   unconnected pong; older clients ignore the body.
 * - POST /v1/join/{networkId}: the request body is the client's SDP offer;
 *   the response body is the full ICE SDP answer (application/sdp). The
 *   whole exchange fits one round trip, so the join's reply path does not
 *   trickle and no candidate signals flow in either direction.
 *
 * As a {@link JoinSource}, each offer becomes a {@link Join} whose reply path
 * is the pending HTTP exchange; the endpoint admits it (identity, throttle,
 * capacity) and answers or refuses it. A refusal answers with the refusal's
 * HTTP status. Bind the listener with {@link #bind}; offers are answered once
 * an endpoint started the source.
 *
 * Connections are one shot (Connection: close), offers are capped at 1 MiB,
 * and a negotiation that produces no answer within the timeout responds 504
 * and abandons the join, which tears the half negotiated connection down.
 *
 * TLS: pre 26.40 clients require HTTPS with a certificate that validates
 * against the server's IP address; pass an SslContext for that. 26.40 and
 * later also accept plain HTTP, trusting the server identity through their
 * TOFU flow. The listener serves both on the one port: each connection's
 * protocol is selected from its first bytes, so with a certificate present
 * TLS and plaintext both work, and with none (or behind a TLS terminating
 * reverse proxy) plaintext alone is served and TLS speakers are closed
 * immediately.
 */
public class NetherNetHttpSignaling implements JoinSource {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetHttpSignaling.class);

    private static final String JOIN_PATH = "/v1/join";
    private static final String OFFER_PATH_PREFIX = "/v1/join/";
    /** Mirrors the go-nethernet reference cap for SDP bodies. */
    private static final int MAX_OFFER_BYTES = 1 << 20;
    private static final int READ_TIMEOUT_SECONDS = 10;
    private static final long NEGOTIATION_TIMEOUT_SECONDS = 15;

    private final Supplier<SslContext> sslContextSupplier;
    private final EventLoopGroup workerGroup;
    private final Supplier<? extends EventLoopGroup> acceptGroupFactory;
    private final Object lifecycleLock = new Object();
    private final ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final SourceHealth health = new SourceHealth();

    private final Map<Long, HttpReply> pendingExchanges = new ConcurrentHashMap<>();

    private volatile Supplier<NetherNetServerStatus> statusSupplier;
    // Racing a duplicate first warn is harmless; this only bounds the spam.
    private volatile boolean statusSupplierWarned;
    private volatile JoinSink sink;
    private volatile Channel serverChannel;
    // The TCP accept loop. Owned: bind() is called from the NetherNet server
    // channel's event loop, so registering the listener on a caller supplied
    // group and waiting for the bind would deadlock a single threaded group
    // against itself (netty rejects it as a blocking call on the event loop).
    private EventLoopGroup acceptGroup;
    private volatile boolean closed;

    /**
     * @param sslContext  server TLS context, or null to serve plaintext
     * @param workerGroup connection I/O loops; not owned, never shut down here
     */
    public NetherNetHttpSignaling(SslContext sslContext, EventLoopGroup workerGroup) {
        this(() -> sslContext, workerGroup);
    }

    /**
     * @param sslContextSupplier consulted per accepted connection: non null
     *                           enables TLS with that context, null serves
     *                           plaintext. Lets certificate rotation (or late
     *                           issuance) apply to new connections without a
     *                           rebind; signaling connections are one shot,
     *                           so nothing needs draining.
     * @param workerGroup        connection I/O loops; not owned, never shut
     *                           down here
     */
    public NetherNetHttpSignaling(Supplier<SslContext> sslContextSupplier, EventLoopGroup workerGroup) {
        this(sslContextSupplier, workerGroup, () -> new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()));
    }

    NetherNetHttpSignaling(Supplier<SslContext> sslContextSupplier, EventLoopGroup workerGroup,
                          Supplier<? extends EventLoopGroup> acceptGroupFactory) {
        this.sslContextSupplier = Objects.requireNonNull(sslContextSupplier, "sslContextSupplier");
        this.workerGroup = Objects.requireNonNull(workerGroup, "workerGroup");
        this.acceptGroupFactory = Objects.requireNonNull(acceptGroupFactory, "acceptGroupFactory");
    }

    /**
     * Sets the source of the server status answered on the capability check,
     * or null to answer with an empty body as before. Consulted per request,
     * so live values (the player count above all) need no republishing; the
     * consumer owns any caching, since this endpoint is reachable by anyone
     * who can open a TCP connection.
     *
     * Called from connection I/O threads; implementations must be thread
     * safe and return promptly. A failure is treated as no status being
     * available and never turns the capability check negative.
     */
    public void setStatusSupplier(Supplier<NetherNetServerStatus> statusSupplier) {
        this.statusSupplier = statusSupplier;
    }

    /**
     * Binds the TCP listener. Call it once, before or after an endpoint starts this source.
     *
     * @param localAddress where to listen
     * @throws ConnectException if the listener cannot bind; the source is closed then
     */
    public void bind(SocketAddress localAddress) throws ConnectException {
        Objects.requireNonNull(localAddress, "localAddress");
        EventLoopGroup accept;
        synchronized (lifecycleLock) {
            if (closed) {
                throw new ConnectException("HTTP signaling is closed");
            }
            if (acceptGroup != null) {
                throw new ConnectException("HTTP signaling is already bound or binding");
            }
            accept = Objects.requireNonNull(acceptGroupFactory.get(), "acceptGroupFactory returned null");
            acceptGroup = accept;
        }

        ChannelFuture bind = null;
        boolean bound = false;
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(accept, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            initConnection(ch);
                        }
                    });
            bind = bootstrap.bind(localAddress);
            synchronized (lifecycleLock) {
                if (closed) {
                    throw new ConnectException("HTTP signaling closed during bind");
                }
                serverChannel = bind.channel();
            }
            bind.sync();
            synchronized (lifecycleLock) {
                if (closed) {
                    throw new ConnectException("HTTP signaling closed during bind");
                }
                bound = true;
            }
            log.info("HTTP signaling listening on {}", bind.channel().localAddress());
            if (sink != null) {
                health.set(SignalingState.CONNECTED, null);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            ConnectException ce = new ConnectException("Failed to bind HTTP signaling listener: " + e.getMessage());
            ce.initCause(e);
            throw ce;
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

    void initConnection(Channel channel) {
        connections.add(channel);
        if (closed) {
            channel.close();
            return;
        }
        ReadTimeoutHandler requestTimeout = new ReadTimeoutHandler(READ_TIMEOUT_SECONDS);
        HttpServerCodec httpCodec = new HttpServerCodec();
        channel.pipeline().addLast(requestTimeout);
        channel.pipeline().addLast(new ProtocolSelectingHandler());
        channel.pipeline().addLast(httpCodec);
        channel.pipeline().addLast(new SingleRequestHandler(requestTimeout, httpCodec));
        channel.pipeline().addLast(new HttpObjectAggregator(MAX_OFFER_BYTES + 8192));
        channel.pipeline().addLast(new SignalingRequestHandler());
    }

    private static final class SingleRequestHandler extends ChannelInboundHandlerAdapter {
        private final ReadTimeoutHandler requestTimeout;
        private final HttpServerCodec httpCodec;
        private boolean complete;

        private SingleRequestHandler(ReadTimeoutHandler requestTimeout, HttpServerCodec httpCodec) {
            this.requestTimeout = requestTimeout;
            this.httpCodec = httpCodec;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (complete) {
                ReferenceCountUtil.release(message);
                return;
            }
            if (message instanceof LastHttpContent) {
                complete = true;
                ctx.pipeline().remove(requestTimeout);
                // Keep the response encoder while discarding subsequent pipelined input.
                // The pending offer now has its own negotiation deadline.
                httpCodec.removeInboundHandler();
            }
            ctx.fireChannelRead(message);
        }
    }

    /**
     * The address the HTTP listener actually bound, resolving an ephemeral
     * port request to the assigned port. Null before {@link #bind}.
     */
    public InetSocketAddress boundAddress() {
        Channel channel = this.serverChannel;
        return channel != null ? (InetSocketAddress) channel.localAddress() : null;
    }


    @Override
    public void start(JoinSink sink) {
        this.sink = Objects.requireNonNull(sink, "sink");
        health.set(serverChannel != null && serverChannel.isActive() ? SignalingState.CONNECTED
                : SignalingState.STARTING, null);
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

    /** Retail HTTP clients run no STUN or TURN of their own, and this source hands out none. */
    @Override
    public List<IceServer> iceServers() {
        return List.of();
    }

    /** Whether the offer with this connection ID still waits for its answer. */
    boolean isPending(long connectionId) {
        return pendingExchanges.containsKey(connectionId);
    }

    /** Closes the listener and accepted connections. This instance cannot be rebound. */
    @Override
    public void close() {
        Channel channel;
        EventLoopGroup accept;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            channel = serverChannel;
            accept = acceptGroup;
            this.serverChannel = null;
            this.acceptGroup = null;
        }
        try {
            if (channel != null) {
                channel.close();
            }
            for (HttpReply exchange : List.copyOf(pendingExchanges.values())) {
                exchange.complete(HttpResponseStatus.SERVICE_UNAVAILABLE, "text/plain", "Server shutting down");
            }
        } finally {
            try {
                connections.close();
            } finally {
                health.set(SignalingState.CLOSED, null);
                if (accept != null) {
                    accept.shutdownGracefully(0, 3, TimeUnit.SECONDS);
                }
            }
        }
    }

    /**
     * One in flight offer/answer exchange, and the join's way back: the HTTP
     * context awaiting the answer and the negotiation timeout reaping it. The
     * first response ends it.
     */
    final class HttpReply implements ReplyPath {
        private final ChannelHandlerContext ctx;
        private final long connectionId;
        private final CompletableFuture<Void> abandoned = new CompletableFuture<>();
        private ScheduledFuture<?> timeout;
        private boolean completed;

        HttpReply(ChannelHandlerContext ctx, long connectionId) {
            this.ctx = ctx;
            this.connectionId = connectionId;
        }

        synchronized void setTimeout(ScheduledFuture<?> timeout) {
            // Close can complete an exchange between its map insertion and timer assignment.
            if (completed) {
                timeout.cancel(false);
            } else {
                this.timeout = timeout;
            }
        }

        /** Ends the exchange; false when it already ended. */
        private boolean end() {
            synchronized (this) {
                if (completed) {
                    return false;
                }
                completed = true;
                if (timeout != null) {
                    timeout.cancel(false);
                    timeout = null;
                }
            }
            pendingExchanges.remove(connectionId, this);
            return true;
        }

        /** Answers the exchange if it is still open. */
        CompletionStage<Void> complete(HttpResponseStatus status, String contentType, String body) {
            if (!end()) {
                return CompletableFuture.failedFuture(new IllegalStateException("The exchange already ended"));
            }
            CompletableFuture<Void> written = new CompletableFuture<>();
            respond(ctx, status, contentType, body).addListener(done -> {
                if (done.isSuccess()) {
                    written.complete(null);
                } else {
                    written.completeExceptionally(done.cause());
                }
            });
            return written;
        }

        /** The client can no longer get an answer: the deadline passed or it disconnected. */
        void abandon(HttpResponseStatus status) {
            if (!end()) {
                return;
            }
            if (status != null) {
                respond(ctx, status, "text/plain", "");
            }
            abandoned.complete(null);
        }

        @Override
        public boolean trickles() {
            return false;
        }

        @Override
        public CompletionStage<Void> answer(String sdp) {
            return complete(HttpResponseStatus.OK, "application/sdp", sdp);
        }

        @Override
        public CompletionStage<Void> candidate(String candidate) {
            throw new UnsupportedOperationException("HTTP answers carry every candidate");
        }

        @Override
        public CompletionStage<Void> refuse(JoinRefusal refusal) {
            return complete(HttpResponseStatus.valueOf(refusal.httpStatus()), "text/plain", "");
        }

        @Override
        public void onRemoteSignal(Consumer<RemoteSignal> handler) {
            // The offer is all a client sends over HTTP.
        }

        @Override
        public CompletionStage<Void> abandoned() {
            return abandoned.minimalCompletionStage();
        }
    }

    private final class SignalingRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            if (!request.decoderResult().isSuccess()) {
                respond(ctx, HttpResponseStatus.BAD_REQUEST, "text/plain", "Malformed request");
                return;
            }
            String path = request.uri();
            int query = path.indexOf('?');
            if (query >= 0) {
                path = path.substring(0, query);
            }

            if (HttpMethod.GET.equals(request.method()) && JOIN_PATH.equals(path)) {
                // Any 2xx tells the client NetherNet is supported here.
                if (sink == null || closed) {
                    respond(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "text/plain", "Service unavailable");
                } else {
                    respondStatus(ctx);
                }
                return;
            }
            if (HttpMethod.POST.equals(request.method()) && path.startsWith(OFFER_PATH_PREFIX)) {
                handleOffer(ctx, request, path.substring(OFFER_PATH_PREFIX.length()));
                return;
            }
            respond(ctx, HttpResponseStatus.NOT_FOUND, "text/plain", "Not found");
        }

        private void handleOffer(ChannelHandlerContext ctx, FullHttpRequest request, String encodedNetworkId) {
            String networkId;
            try {
                if (encodedNetworkId.isEmpty() || encodedNetworkId.indexOf('/') >= 0
                        || encodedNetworkId.indexOf('#') >= 0) {
                    throw new IllegalArgumentException("Expected one Network ID path segment");
                }
                // Decode to bytes first so invalid UTF-8 cannot collapse distinct IDs.
                // Path decoding preserves literal '+' and decodes percent escapes once.
                String decodedBytes = new QueryStringDecoder("/" + encodedNetworkId, StandardCharsets.ISO_8859_1)
                        .path().substring(1);
                networkId = StandardCharsets.UTF_8.newDecoder()
                        .decode(ByteBuffer.wrap(decodedBytes.getBytes(StandardCharsets.ISO_8859_1))).toString();
            } catch (IllegalArgumentException | CharacterCodingException e) {
                respond(ctx, HttpResponseStatus.BAD_REQUEST, "text/plain", "Invalid Network ID path segment");
                return;
            }
            JoinSink current = sink;
            if (current == null || closed) {
                respond(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "text/plain", "Service unavailable");
                return;
            }
            String offerSdp = request.content().toString(StandardCharsets.UTF_8);
            if (offerSdp.isEmpty()) {
                respond(ctx, HttpResponseStatus.BAD_REQUEST, "text/plain", "Missing SDP offer in request body");
                return;
            }

            long connectionId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
            HttpReply exchange = new HttpReply(ctx, connectionId);
            pendingExchanges.put(connectionId, exchange);

            // Reap a negotiation that produces no answer in time: 504 to the
            // client, and the join abandoned so the endpoint closes the half
            // negotiated child.
            exchange.setTimeout(ctx.channel().eventLoop().schedule(() -> {
                log.debug("Negotiation for {} timed out waiting for the answer", Long.toUnsignedString(connectionId));
                exchange.abandon(HttpResponseStatus.valueOf(JoinRefusal.NO_ANSWER_IN_TIME.httpStatus()));
            }, NEGOTIATION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // A client that disconnects mid negotiation leaves the answer with
            // nowhere to go. Completed exchanges already ended and this is a
            // no-op.
            ctx.channel().closeFuture().addListener(future -> exchange.abandon(null));

            String host = request.headers().get(HttpHeaderNames.HOST);
            Join.Reached reached = host != null && ctx.channel().localAddress() instanceof InetSocketAddress local
                    ? new Join.Reached(host, local.getPort()) : null;
            SourcedAddress source = ctx.channel().remoteAddress() instanceof InetSocketAddress remote
                    && !remote.isUnresolved() ? new SourcedAddress(remote, AddressSource.SIGNALING, null)
                    : SourcedAddress.unknown();
            log.debug("Offer for {} from network {} via HTTP", Long.toUnsignedString(connectionId), networkId);
            try {
                current.join(new Join(connectionId, networkId, offerSdp, source, exchange, reached));
            } catch (RuntimeException e) {
                if (exchange.end()) {
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Routine noise: TLS handshakes from probing clients without our
            // certificate trusted, port scanners, read timeouts.
            log.debug("HTTP signaling connection error: {}", cause.toString());
            ctx.close();
        }
    }

    /**
     * Selects the connection's protocol from its first bytes, so TLS and
     * plaintext are both served on the one port a client ever derives from
     * the join address. A TLS record installs the SslHandler when a
     * certificate is available, and closes the connection when none is,
     * before any handshake byte can reach the HTTP codec. Everything else
     * proceeds as plaintext HTTP, which keeps signaling reachable for
     * clients whose TLS attempt failed against a broken certificate: 26.40
     * and later retry over plain HTTP and trust the server identity through
     * their TOFU flow, so certificate rot degrades to a confirmation prompt
     * instead of losing NetherNet entirely.
     */
    private final class ProtocolSelectingHandler extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            // A TLS record is recognizable from its first five bytes.
            if (in.readableBytes() < 5) {
                return;
            }
            if (SslHandler.isEncrypted(in, false)) {
                SslContext sslContext = sslContextSupplier.get();
                if (sslContext == null) {
                    // TLS spoken at a certificate-less server: close instead
                    // of letting handshake bytes reach the HTTP codec. The
                    // immediate close is also the fastest signal for the
                    // client's own fallback ladder.
                    ctx.close();
                    return;
                }
                ctx.pipeline().addAfter(ctx.name(), null, sslContext.newHandler(ctx.alloc()));
            }
            // Removing the decoder forwards the buffered bytes to whichever
            // handler now follows: the SslHandler for TLS, the HTTP codec
            // for plaintext.
            ctx.pipeline().remove(this);
        }
    }

    /**
     * Answers the capability check, carrying the server status document when
     * the consumer supplies one. A supplier that is absent or that fails
     * leaves the historical empty body, which every client accepts: the guide
     * defines the body as ignored, and only 1.26.50 and later read it.
     */
    private void respondStatus(ChannelHandlerContext ctx) {
        Supplier<NetherNetServerStatus> supplier = this.statusSupplier;
        if (supplier == null) {
            respond(ctx, HttpResponseStatus.OK, "text/plain", "");
            return;
        }
        String body;
        try {
            NetherNetServerStatus status = supplier.get();
            body = status == null ? null : status.toJson();
        } catch (Throwable e) {
            // The probe decides whether the client attempts NetherNet at all,
            // so it must still answer 2xx when the consumer cannot describe
            // itself. Older clients ignore the body outright; what a reading
            // client makes of an absent one is not established. Throwable
            // rather than Exception: an Error escaping here reaches
            // exceptionCaught, which closes the connection with no response
            // at all, and a client reads that as no NetherNet rather than as
            // a server that cannot currently describe itself.
            //
            // Warn once, then debug: the endpoint is unauthenticated TCP, so
            // per request warns would let anyone drive log volume by
            // hammering the probe against a broken supplier.
            if (statusSupplierWarned) {
                log.debug("Server status supplier failed: {}", e.getMessage());
            } else {
                statusSupplierWarned = true;
                log.warn("Server status supplier failed (further failures log at debug): {}", e.getMessage());
            }
            body = null;
        }
        if (body == null) {
            respond(ctx, HttpResponseStatus.OK, "text/plain", "");
        } else {
            respond(ctx, HttpResponseStatus.OK, "application/json", body);
        }
    }

    /**
     * Writes a one shot response: Connection: close and the channel closed
     * after the write, matching the one request per connection model. Safe
     * from any thread.
     */
    private static ChannelFuture respond(ChannelHandlerContext ctx, HttpResponseStatus status, String contentType,
                                         String body) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, contentType)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        return ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}

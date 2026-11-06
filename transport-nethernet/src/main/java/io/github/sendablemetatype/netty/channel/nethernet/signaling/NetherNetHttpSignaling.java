/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusProvider;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.TlsContextSelector;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * HTTP signaling, the direct connection path of Mojang's NetherNet onboarding guide, as a
 * {@link JoinSource}. It listens on TCP, usually at the port players type; the client asks it
 * before it tries anything else, and a client that still supports RakNet falls back to it when
 * the answer is not a {@code 2xx}.
 * <ul>
 *   <li>{@code GET /v1/join}: a {@code 2xx} says NetherNet is served here. The body is the
 *   {@link io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus status
 *   document} the {@link StatusProvider} gives, or empty without one. A {@code 404} declines
 *   NetherNet: when the provider declines, and whenever an offer would certainly be refused
 *   (not started, closed, or the endpoint's {@link JoinSink#admits} says no).</li>
 *   <li>{@code POST /v1/join/{networkId}}: the body is the client's SDP offer; the response is
 *   the full answer ({@code application/sdp}) or a {@link JoinRefusal}'s status with an empty
 *   body. The offer goes to the endpoint as a {@link Join} whose reply path does not trickle. The
 *   endpoint refuses a second offer for a network ID whose join has neither been answered nor
 *   failed yet with {@link JoinRefusal#DUPLICATE}, whichever of its HTTP sources the offers came on
 *   ({@link #oneJoinPerNetworkId()}).</li>
 * </ul>
 * Everything else is a {@code 404}, as on vanilla.
 *
 * <p>Connections: one request, or a status request and one more request started within 5 s;
 * each request has 10 s to arrive whole. The last response says {@code Connection: close}. An
 * offer has the negotiation deadline (15 s by default) from the end of its request for the
 * endpoint's admission and the answer; past it the client gets a {@code 504}, and the join's
 * {@link ReplyPath#abandoned()} completes, as it does when the client leaves first. Nothing else
 * closes a connection while its answer is pending.
 *
 * <p>TLS and plain HTTP share the port: each connection's first bytes say which it is. A TLS
 * connection asks the {@link TlsContextSelector} with the server name it requested; null closes
 * it, which clients read as TLS refused. With {@code requiresTls}, plain HTTP is always answered
 * {@code 426}, also while no certificate is available.
 *
 * <p>Behind proxies, configure {@link TrustedProxies}: a PROXY protocol header (v1 or v2) from a
 * trusted PROXY proxy wins over {@code X-Forwarded-For} from a trusted forwarder; nobody else can
 * declare a source. Connections count against a global cap and a per-address cap (IPv6 by /64,
 * proxied clients by the source the proxy declares).
 *
 * <p>What this source refuses on its own goes to the {@link Builder#metrics metrics}. Every request
 * logs one debug line with the requester, method, path (without its query), status and time,
 * under the logger {@code io.github.sendablemetatype.netty.channel.nethernet.signaling.HttpAccessLog}.
 *
 * <p>Bind with {@link #bind}, then hand the source to an endpoint, which calls {@link #start}.
 * Closed instances cannot be bound again.
 */
public final class NetherNetHttpSignaling implements JoinSource {
    static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetHttpSignaling.class);

    /** The default negotiation deadline, measured from the end of an offer's request. */
    public static final Duration DEFAULT_NEGOTIATION_TIMEOUT = Duration.ofSeconds(15);
    /** The default status deadline. */
    public static final Duration DEFAULT_STATUS_TIMEOUT = Duration.ofSeconds(1);
    /** The default cap on open signaling connections. */
    public static final int DEFAULT_MAX_CONNECTIONS = 1024;
    /**
     * The default cap on connections per client address. High, since players behind one NAT
     * (a classroom) share an address.
     */
    public static final int DEFAULT_MAX_CONNECTIONS_PER_ADDRESS = 100;
    /**
     * The default cap on a request body. Real offers measured 2.0 to 2.5 KB (2026-11-05); the cap
     * bounds what every open connection may buffer, since bodies are read before admission.
     */
    public static final int DEFAULT_MAX_REQUEST_BODY_BYTES = 32 * 1024;

    private static final long WARNING_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(10);

    private final EventLoopGroup workerGroup;
    private final Supplier<? extends EventLoopGroup> acceptGroupFactory;
    private final TlsContextSelector tlsSelector;
    private final boolean requiresTls;
    private final TrustedProxies trustedProxies;
    private final Duration negotiationTimeout;
    private final int maxRequestBodyBytes;
    private final HttpConnectionLimits limits;
    private final HttpStatusService statusService;
    private final ExecutorService ownedStatusExecutor;
    private final NetherServerMetrics metrics;

    private final Object lifecycleLock = new Object();
    private final ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final List<Consumer<SignalingHealth>> healthListeners = new CopyOnWriteArrayList<>();
    private final AtomicLong nextPlaintextWarning = new AtomicLong(System.nanoTime());
    private final AtomicLong nextTlsWarning = new AtomicLong(System.nanoTime());
    private volatile JoinSink sink;
    private volatile Channel serverChannel;
    // The TCP accept loop. Owned: binding on a group the caller also runs on and waiting for it
    // would deadlock a single threaded group against itself
    private EventLoopGroup acceptGroup;
    private boolean bound;
    private volatile boolean closed;
    private volatile SignalingHealth health = new SignalingHealth(SignalingState.NEW, 0, null, null);

    private NetherNetHttpSignaling(Builder builder) {
        this.workerGroup = builder.workerGroup;
        this.acceptGroupFactory = builder.acceptGroupFactory;
        this.tlsSelector = builder.tlsSelector;
        this.requiresTls = builder.requiresTls;
        this.trustedProxies = builder.trustedProxies;
        this.negotiationTimeout = builder.negotiationTimeout;
        this.maxRequestBodyBytes = builder.maxRequestBodyBytes;
        this.limits = new HttpConnectionLimits(builder.maxConnections, builder.maxConnectionsPerAddress);
        this.metrics = builder.metrics;
        if (builder.statusProvider == null) {
            this.ownedStatusExecutor = null;
            this.statusService = null;
        } else {
            Executor executor = builder.statusExecutor;
            this.ownedStatusExecutor = executor == null
                    ? Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("nethernet-http-status-", 0).factory())
                    : null;
            this.statusService = new HttpStatusService(builder.statusProvider, builder.statusTimeout,
                    executor != null ? executor : ownedStatusExecutor);
        }
    }

    /**
     * @param workerGroup the I/O loops connections are served on; not owned, never shut down here.
     *                    It has to take NIO channels, as the listener is NIO
     * @return a builder with the defaults: plain HTTP only, no trusted proxies, an empty status
     */
    public static Builder builder(EventLoopGroup workerGroup) {
        return new Builder(workerGroup);
    }

    /**
     * Binds the listener and returns once it listens. A failed bind closes this instance. Do not
     * call it from the worker group's loops.
     *
     * @param localAddress where to listen
     * @throws ConnectException if binding fails, or this instance is closed or already bound
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
        updateHealth(null);

        ChannelFuture bind = null;
        Throwable failure = null;
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
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            ConnectException ce = new ConnectException("Failed to bind HTTP signaling listener: " + e.getMessage());
            ce.initCause(e);
            failure = ce;
            throw ce;
        } finally {
            if (failure != null) {
                try {
                    if (bind != null) {
                        bind.channel().close();
                    }
                } finally {
                    close(failure);
                }
            }
        }
        updateHealth(null);
    }

    /** The address the listener bound, an ephemeral port resolved; null before {@link #bind}. */
    public InetSocketAddress boundAddress() {
        Channel channel = this.serverChannel;
        return channel != null ? (InetSocketAddress) channel.localAddress() : null;
    }

    @Override
    public void start(JoinSink sink) {
        Objects.requireNonNull(sink, "sink");
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("HTTP signaling is closed");
            }
            if (this.sink != null) {
                throw new IllegalStateException("HTTP signaling was already started");
            }
            this.sink = sink;
        }
        updateHealth(null);
    }

    /**
     * {@link SignalingState#CONNECTED} once bound and started; {@link SignalingState#STARTING}
     * while one of the two is missing.
     */
    @Override
    public SignalingHealth health() {
        return health;
    }

    /** Listeners run on the thread that changed the state: the one binding, starting or closing. */
    @Override
    public void addHealthListener(Consumer<SignalingHealth> listener) {
        healthListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeHealthListener(Consumer<SignalingHealth> listener) {
        healthListeners.remove(listener);
    }

    /** None: HTTP signaling hands out no STUN or TURN servers. */
    @Override
    public List<IceServer> iceServers() {
        return List.of();
    }

    /** Yes: a client sends one offer per connect, so a second one for a pending join is a duplicate. */
    @Override
    public boolean oneJoinPerNetworkId() {
        return true;
    }

    /**
     * Closes the listener and every connection. An offer waiting for its answer has its
     * {@link ReplyPath#abandoned()} completed. This instance cannot be bound again.
     */
    @Override
    public void close() {
        close(null);
    }

    private void close(Throwable failure) {
        Channel channel;
        EventLoopGroup accept;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            channel = serverChannel;
            accept = acceptGroup;
            serverChannel = null;
            acceptGroup = null;
        }
        try {
            if (channel != null) {
                channel.close();
            }
            connections.close();
        } finally {
            try {
                if (ownedStatusExecutor != null) {
                    ownedStatusExecutor.shutdownNow();
                }
            } finally {
                if (accept != null) {
                    accept.shutdownGracefully(0, 3, TimeUnit.SECONDS);
                }
            }
        }
        updateHealth(failure);
    }

    /** Builds the pipeline of one accepted connection. */
    void initConnection(Channel channel) {
        connections.add(channel);
        if (closed) {
            channel.close();
            return;
        }
        if (!limits.open()) {
            InetSocketAddress refused = (InetSocketAddress) channel.remoteAddress();
            log.debug("Refused a signaling connection from {}: the connection cap is reached", refused);
            metrics(metrics -> metrics.connectionCapRefused(refused));
            channel.close();
            return;
        }
        channel.closeFuture().addListener(future -> limits.closed());
        HttpConnection connection = new HttpConnection(channel, limits);
        InetSocketAddress peer = (InetSocketAddress) channel.remoteAddress();
        // A proxy's clients count by the source it declares: in its PROXY header, or in each
        // request's headers (HttpSourceCounter)
        if (!trustedProxies.isProxy(peer) && !connection.countAddress(peer.getAddress())) {
            log.debug("Refused a signaling connection from {}: its address holds its share", peer);
            addressRefused(peer);
            channel.close();
            return;
        }
        ChannelPipeline pipeline = channel.pipeline();
        if (trustedProxies.trustsProxyProtocol(peer)) {
            pipeline.addLast(new HttpProxyHeaderReader(connection, this));
        }
        pipeline.addLast(new HttpTlsSelector(connection, this));
        HttpServerCodec codec = new HttpServerCodec();
        pipeline.addLast(codec);
        pipeline.addLast(new HttpRequestBudget(this, connection, codec));
        pipeline.addLast(new HttpAccessLog(this, connection));
        if (trustedProxies.isProxy(peer)) {
            pipeline.addLast(new HttpSourceCounter(this, connection));
        }
        pipeline.addLast(new HttpOfferAggregator(this, maxRequestBodyBytes));
        pipeline.addLast(new HttpRequestHandler(this, connection));
    }

    /** Where a request came from. */
    SourcedAddress sourceOf(HttpConnection connection, InetSocketAddress peer, List<String> forwardedFor) {
        return SourcedAddress.signaling(peer, connection.proxyDeclared, forwardedFor, trustedProxies);
    }

    /** Counts a client a PROXY protocol header declared. */
    void countProxiedClient(HttpConnection connection, InetSocketAddress declared) {
        if (!connection.countAddress(declared.getAddress())) {
            log.debug("Refused a signaling connection for {} via {}: its address holds its share", declared,
                    connection.channel.remoteAddress());
            addressRefused(declared);
            connection.channel.close();
        }
    }

    /** A connection for this client was refused: its address holds its share of connections. */
    void addressRefused(InetSocketAddress client) {
        metrics(metrics -> metrics.addressRefused(client));
    }

    /** A request on this connection did not arrive whole within its deadline. */
    void requestTimedOut(HttpConnection connection) {
        InetSocketAddress client = connection.proxyDeclared != null ? connection.proxyDeclared
                : (InetSocketAddress) connection.channel.remoteAddress();
        metrics(metrics -> metrics.requestTimedOut(client));
    }

    void plaintextRefused(InetSocketAddress client) {
        warnSparingly(nextPlaintextWarning, "Refused plain HTTP signaling from {}: requiresTls is set, so only "
                + "TLS is served", client);
        metrics(metrics -> metrics.plaintextRefused(client));
    }

    /**
     * An offer this source refused itself, before or instead of the endpoint. The endpoint
     * reports the refusals it decides.
     */
    void joinRefused(JoinRefusal refusal) {
        joinRefused(refusal.httpStatus());
    }

    /** An offer refused with this status, also one the aggregator turned away before it was read whole. */
    void joinRefused(int httpStatus) {
        metrics(metrics -> metrics.joinRefused(httpStatus));
    }

    /** Reports to the metrics, which must never break signaling. */
    private void metrics(Consumer<NetherServerMetrics> event) {
        if (metrics == null) {
            return;
        }
        try {
            event.accept(metrics);
        } catch (RuntimeException e) {
            log.debug("NetherNet server metrics failed", e);
        }
    }

    void tlsRefused(String serverName, String reason) {
        if (requiresTls) {
            warnSparingly(nextTlsWarning, "Refused TLS signaling for server name {} ({}), and requiresTls refuses "
                    + "plain HTTP: these clients get no NetherNet signaling at all", serverName, reason);
        } else {
            log.debug("Refused TLS signaling for server name {}: {}", serverName, reason);
        }
    }

    void tlsSelectorFailed(String serverName, Throwable cause) {
        warnSparingly(nextTlsWarning, "The TLS selector failed for server name {}: {}", serverName, cause);
    }

    TlsContextSelector tlsSelector() {
        return tlsSelector;
    }

    boolean requiresTls() {
        return requiresTls;
    }

    TrustedProxies trustedProxies() {
        return trustedProxies;
    }

    JoinSink sink() {
        return sink;
    }

    boolean isClosed() {
        return closed;
    }

    HttpStatusService statusService() {
        return statusService;
    }

    Duration negotiationTimeout() {
        return negotiationTimeout;
    }

    /**
     * Warns at most once per 10 minutes for one kind of event and logs the rest at debug: the
     * endpoint is open to anyone, who could otherwise drive the log volume.
     */
    private static void warnSparingly(AtomicLong next, String format, Object... arguments) {
        long now = System.nanoTime();
        long due = next.get();
        if (now - due >= 0 && next.compareAndSet(due, now + WARNING_INTERVAL_NANOS)) {
            log.warn(format + " (repeats log at debug for 10 minutes)", arguments);
        } else {
            log.debug(format, arguments);
        }
    }

    private void updateHealth(Throwable failure) {
        SignalingHealth next;
        synchronized (lifecycleLock) {
            SignalingState state;
            if (closed) {
                state = SignalingState.CLOSED;
            } else if (bound && sink != null) {
                state = SignalingState.CONNECTED;
            } else if (acceptGroup == null && sink == null) {
                state = SignalingState.NEW;
            } else {
                state = SignalingState.STARTING;
            }
            if (state == health.state()) {
                return;
            }
            next = new SignalingHealth(state, failure == null ? 0 : 1, failure, null);
            health = next;
        }
        for (Consumer<SignalingHealth> listener : healthListeners) {
            try {
                listener.accept(next);
            } catch (RuntimeException e) {
                log.warn("A signaling health listener failed", e);
            }
        }
    }

    /** Builds a {@link NetherNetHttpSignaling}. */
    public static final class Builder {
        private final EventLoopGroup workerGroup;
        private Supplier<? extends EventLoopGroup> acceptGroupFactory =
                () -> new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        private TlsContextSelector tlsSelector;
        private boolean requiresTls;
        private TrustedProxies trustedProxies = TrustedProxies.NONE;
        private StatusProvider statusProvider;
        private Executor statusExecutor;
        private Duration statusTimeout = DEFAULT_STATUS_TIMEOUT;
        private Duration negotiationTimeout = DEFAULT_NEGOTIATION_TIMEOUT;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int maxConnectionsPerAddress = DEFAULT_MAX_CONNECTIONS_PER_ADDRESS;
        private int maxRequestBodyBytes = DEFAULT_MAX_REQUEST_BODY_BYTES;
        private NetherServerMetrics metrics;

        private Builder(EventLoopGroup workerGroup) {
            this.workerGroup = Objects.requireNonNull(workerGroup, "workerGroup");
        }

        /**
         * Serves TLS next to plain HTTP, with the context the selector picks per connection. See
         * {@link io.github.sendablemetatype.netty.channel.nethernet.signaling.http.TlsContexts} to
         * load one from files. Without a selector, TLS connections are closed.
         *
         * @param selector the selector, or null for no TLS
         * @return this builder
         */
        public Builder tls(TlsContextSelector selector) {
            this.tlsSelector = selector;
            return this;
        }

        /**
         * Refuses plain HTTP with a {@code 426}, also when no certificate is available, so that
         * nothing is served then. Off by default: a client whose TLS attempt was refused retries
         * plain HTTP and trusts the server's identity on first use, so refusing plain HTTP turns
         * that into no NetherNet at all, or a RakNet join where the client still has RakNet.
         *
         * @param requiresTls whether to refuse plain HTTP
         * @return this builder
         */
        public Builder requiresTls(boolean requiresTls) {
            this.requiresTls = requiresTls;
            return this;
        }

        /**
         * Sets the proxies allowed to declare a client: PROXY protocol headers from one role,
         * {@code X-Forwarded-For} from the other. Parse them once at startup.
         *
         * @param trustedProxies the proxies; {@link TrustedProxies#NONE} by default
         * @return this builder
         */
        public Builder trustedProxies(TrustedProxies trustedProxies) {
            this.trustedProxies = Objects.requireNonNull(trustedProxies, "trustedProxies");
            return this;
        }

        /**
         * Sets what status requests are answered with. Without a provider every status request
         * gets an empty {@code 200}.
         *
         * @param provider the provider, or null
         * @return this builder
         */
        public Builder statusProvider(StatusProvider provider) {
            this.statusProvider = provider;
            return this;
        }

        /**
         * Sets how long a status request waits for the provider before the last document served
         * for its {@code Host} goes out instead.
         *
         * @param timeout the deadline, 1 s by default
         * @return this builder
         */
        public Builder statusTimeout(Duration timeout) {
            this.statusTimeout = positive(timeout, "statusTimeout");
            return this;
        }

        /**
         * Sets how long an offer may wait for the endpoint's admission and its answer, from the
         * end of its request. Past it the client gets a {@code 504}.
         *
         * @param timeout the deadline, 15 s by default
         * @return this builder
         */
        public Builder negotiationTimeout(Duration timeout) {
            this.negotiationTimeout = positive(timeout, "negotiationTimeout");
            return this;
        }

        /**
         * @param maxConnections open signaling connections at most,
         *                       {@value NetherNetHttpSignaling#DEFAULT_MAX_CONNECTIONS} by default
         * @return this builder
         */
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = positive(maxConnections, "maxConnections");
            return this;
        }

        /**
         * @param maxConnectionsPerAddress open connections per client address (IPv6 per /64) at
         *                                 most, {@value NetherNetHttpSignaling#DEFAULT_MAX_CONNECTIONS_PER_ADDRESS}
         *                                 by default
         * @return this builder
         */
        public Builder maxConnectionsPerAddress(int maxConnectionsPerAddress) {
            this.maxConnectionsPerAddress = positive(maxConnectionsPerAddress, "maxConnectionsPerAddress");
            return this;
        }

        /**
         * @param maxRequestBodyBytes the largest request body taken; larger ones get a {@code 413}.
         *                            {@value NetherNetHttpSignaling#DEFAULT_MAX_REQUEST_BODY_BYTES} by default
         * @return this builder
         */
        public Builder maxRequestBodyBytes(int maxRequestBodyBytes) {
            this.maxRequestBodyBytes = positive(maxRequestBodyBytes, "maxRequestBodyBytes");
            return this;
        }

        /**
         * Reports what this source refuses on its own: connections whose address holds its share
         * ({@link NetherServerMetrics#addressRefused}) or that find every connection taken
         * ({@link NetherServerMetrics#connectionCapRefused}), requests that miss their deadline
         * ({@link NetherServerMetrics#requestTimedOut}), plain HTTP under {@link #requiresTls}
         * ({@link NetherServerMetrics#plaintextRefused}), and offers it answers with a
         * {@link JoinRefusal} before or instead of the endpoint, such as a {@code 504} past the
         * negotiation deadline, or with a {@code 413} or {@code 417} before reading it whole
         * ({@link NetherServerMetrics#joinRefused}).
         * Give it the metrics the endpoint has as
         * {@link io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption#NETHER_SERVER_METRICS}:
         * the endpoint reports the refusals it decides, so nothing is counted twice. A hook that
         * throws is logged at debug and ignored.
         *
         * @param metrics the metrics, or null (the default) for none
         * @return this builder
         */
        public Builder metrics(NetherServerMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        /** Creates the accept loop at bind; a test seam. */
        Builder acceptGroupFactory(Supplier<? extends EventLoopGroup> factory) {
            this.acceptGroupFactory = Objects.requireNonNull(factory, "factory");
            return this;
        }

        /** Runs status provider calls, instead of a virtual thread each; a test seam. */
        Builder statusExecutor(Executor executor) {
            this.statusExecutor = Objects.requireNonNull(executor, "executor");
            return this;
        }

        /** @return the signaling, not bound yet */
        public NetherNetHttpSignaling build() {
            return new NetherNetHttpSignaling(this);
        }

        private static Duration positive(Duration value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isNegative() || value.isZero()) {
                throw new IllegalArgumentException(name + " must be positive: " + value);
            }
            return value;
        }

        private static int positive(int value, String name) {
            if (value < 1) {
                throw new IllegalArgumentException(name + " must be positive: " + value);
            }
            return value;
        }
    }
}

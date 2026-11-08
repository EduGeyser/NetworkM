package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Xbox signaling over the JSON-RPC WebSocket service: joins by friends and by connection ID
 * arrive here, and answers and candidates go back as client messages to the joining player.
 *
 * <p>As a {@link JoinSource}, each {@code CONNECTREQUEST} becomes a {@link Join} whose replies go
 * back to the sender's player ID; the client's later {@code CANDIDATEADD} and {@code CONNECTERROR}
 * messages for that connection ID reach the join through its reply path. Xbox signaling does not
 * say where the client is, so joins carry {@link SourcedAddress#unknown()}.
 *
 * <p>The service addresses every player by pmid, the GUID in the pmid claim of the player's
 * MCToken, not by network ID. A client channel over this signaling dials the host's pmid as the
 * network ID of its {@code NetherNetAddress}.
 *
 * <p>The source keeps its own socket up. {@link #start(JoinSink)} and {@link #connect} start it in
 * the background and never wait for Xbox, so an outage at startup leaves the endpoint bound.
 * Before every connection attempt the {@link TokenSupplier} is asked for a token, since a token
 * can expire between attempts. Failed attempts are retried with backoff, and a lost socket is
 * replaced in place: the network ID, the registered handlers and every connected player stay
 * as they are. {@link #health()} and the health listeners report the state; {@link #reconnect()}
 * replaces the socket on request.
 *
 * <p>A socket counts as lost when the service closes it, it fails, no frame arrives for
 * {@value #SILENCE_LIMIT_SECONDS} s (WebSocket pings every {@value #WS_PING_INTERVAL_SECONDS} s
 * guarantee frames on a healthy socket), {@value #RPC_PING_FAILURE_LIMIT} {@code System_Ping}
 * requests in a row go unanswered, or the self-addressed route probe shows that joins can no
 * longer reach us.
 */
@Sharable
public class NetherNetXboxRpcSignaling extends SimpleChannelInboundHandler<TextWebSocketFrame>
        implements NetherNetClientSignaling, JoinSource {

    /**
     * Supplies the authorization for the signaling WebSocket. Asked before every connection
     * attempt; the stage may complete on any thread.
     */
    @FunctionalInterface
    public interface TokenSupplier {
        /**
         * @param refresh true when the service refused the previous token (HTTP 401 or 403 on the
         *                upgrade); the supplier must then not return a cached token
         * @return the {@code Authorization} header value, {@code MCToken ...}
         */
        CompletionStage<String> token(boolean refresh);
    }

    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetXboxRpcSignaling.class);
    private static final Gson gson = new GsonBuilder().serializeNulls().create();
    private static final URI SERVICE_URI = URI.create("wss://signal.franchise.minecraft-services.net/ws/v1.0/messaging/connect");
    /**
     * A pmid, the form the service gives players (36 characters, hyphenated). The service reads
     * {@code toPlayerId} as a GUID and refuses a numeric network ID or an empty one with
     * {@code -32602} (captured 2026-11-08, RpcProbe 2.2 and 4.2).
     */
    private static final Pattern PLAYER_ID = Pattern.compile("[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}");

    /** Deadline for every RPC request. */
    static final long REQUEST_TIMEOUT_SECONDS = 20;
    /** WebSocket pings. The RFC obliges the service to answer with a pong, so a healthy socket
     * always has inbound frames, even on an idle server. */
    static final long WS_PING_INTERVAL_SECONDS = 15;
    /** Three missed pongs. */
    static final long SILENCE_LIMIT_SECONDS = 45;
    static final long RPC_PING_FIRST_SECONDS = 30;
    static final long RPC_PING_INTERVAL_SECONDS = 50;
    /** An answered System_Ping proves the RPC layer behind the WebSocket, which a pong does not. */
    static final int RPC_PING_FAILURE_LIMIT = 2;
    /** The service routes a self-addressed message back only while our registration is alive, so
     * the probe exercises the path a joining client uses. Pings never touch that path: a socket
     * whose registration died keeps answering them. */
    static final long ROUTE_PROBE_INTERVAL_SECONDS = 30;
    /** Three probes without an answer on a socket whose route was proven. */
    static final long ROUTE_SILENCE_LIMIT_SECONDS = 90;
    static final long LIVENESS_CHECK_SECONDS = 5;
    /** TURN refresh when the service states no lifetime, and the longest wait between refreshes. */
    static final long TURN_REFRESH_FALLBACK_SECONDS = 30 * 60;
    static final long TURN_REFRESH_MIN_SECONDS = 1;
    static final long TURN_RETRY_SECONDS = 60;
    /** Signals held while a socket waits for its first TURN credentials. */
    static final int HELD_SIGNAL_LIMIT = 256;

    /** Timers tied to one socket's lifetime, cancelled when it goes inactive, so a replaced
     * socket's loops never keep running on the shared event loop. */
    private static final AttributeKey<CopyOnWriteArrayList<ScheduledFuture<?>>> SOCKET_TASKS =
            AttributeKey.valueOf("nethernet-signaling-socket-tasks");

    /**
     * An in-flight JSON-RPC request, tagged with the socket it was written to so that a dying
     * socket only fails its own requests, never those already sent on its replacement.
     */
    private static final class PendingRequest {
        final CompletableFuture<JsonObject> future;
        final Channel channel;
        private ScheduledFuture<?> timeout;

        PendingRequest(CompletableFuture<JsonObject> future, Channel channel) {
            this.future = future;
            this.channel = channel;
        }

        synchronized void setTimeout(ScheduledFuture<?> timeout) {
            if (future.isDone()) {
                timeout.cancel(false);
            } else {
                this.timeout = timeout;
            }
        }

        synchronized void cancelTimeout() {
            if (timeout != null) {
                timeout.cancel(false);
                timeout = null;
            }
        }
    }

    /** The service answered a request with an error. */
    private static final class RpcResponseException extends RuntimeException {
        /** The error says the target player is not registered with the service. */
        final boolean notFound;

        RpcResponseException(String message, boolean notFound) {
            super(message);
            this.notFound = notFound;
        }
    }

    private record TurnCredentials(List<IceServer> servers, long expirationSeconds) {
    }

    private record HeldSignal(String sender, String message) {
    }

    /** Joins are told apart by sender too, so one client cannot reach another's join by its ID. */
    private record JoinKey(String sender, String connectionId) {
    }

    private final String localNetworkId;
    private final EventLoopGroup eventLoopGroup;
    private final SignalingSupervisor supervisor;
    private final Map<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final Map<String, ClientHandler> handlers = new ConcurrentHashMap<>();
    private final Map<JoinKey, XboxJoin> joins = new ConcurrentHashMap<>();

    private volatile String token;
    private volatile JoinSink sink;
    private volatile NotFoundHandler notFoundHandler;
    private volatile List<IceServer> iceServers = List.of();

    // Guarded by this. The current socket and the credential exchange that makes it usable.
    private Channel channel;
    private CompletableFuture<List<IceServer>> connectFuture;
    private boolean closed;
    // Signals from the current socket that arrived before its first TURN credentials. A join
    // created then would get no TURN servers, and Xbox offers trickle every candidate, so the
    // join's later signals wait with it to keep their order.
    private final ArrayDeque<HeldSignal> heldSignals = new ArrayDeque<>();

    // Liveness of the current socket; reset when a socket connects.
    private volatile long lastFrameAtNanos;
    private volatile int rpcPingFailures;
    private volatile long routeProvenAtNanos;
    private volatile boolean routeProven;
    private volatile boolean routeProbeUnsupported;
    private volatile int routeProbesSent;
    private volatile boolean routeUnansweredWarned;

    /**
     * @param networkId the local network ID, kept across reconnects so registered join codes and
     *                  connection IDs stay valid
     * @param tokens    asked for the authorization before every connection attempt
     */
    public NetherNetXboxRpcSignaling(String networkId, TokenSupplier tokens) {
        this(networkId, tokens, new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()), null);
    }

    /**
     * Creates the source with a random local network ID.
     *
     * @param tokens asked for the authorization before every connection attempt
     */
    public NetherNetXboxRpcSignaling(TokenSupplier tokens) {
        this(Long.toUnsignedString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)), tokens);
    }

    /**
     * @param supervisorExecutor where reconnects are supervised; null for the socket group's one
     *                           thread, which tests replace with an embedded event loop
     */
    NetherNetXboxRpcSignaling(String networkId, TokenSupplier tokens, EventLoopGroup group,
                              EventExecutor supervisorExecutor) {
        this.localNetworkId = Objects.requireNonNull(networkId, "networkId");
        this.eventLoopGroup = Objects.requireNonNull(group, "group");
        this.supervisor = new SignalingSupervisor(supervisorExecutor != null ? supervisorExecutor : group.next(),
                tokens, new SignalingSupervisor.Connector() {
                    @Override
                    public CompletionStage<?> open(String token) {
                        return openSocket(token);
                    }

                    @Override
                    public void abort() {
                        abortSocket();
                    }
                });
    }

    @Override
    public String getLocalNetworkId() {
        return localNetworkId;
    }

    /** @return the current health; never blocks */
    @Override
    public SignalingHealth health() {
        return supervisor.health();
    }

    /**
     * Adds a listener for health changes. It sees changes from now on; {@link #health()} gives the
     * current state. It runs on the signaling's event loop and must not block.
     */
    @Override
    public void addHealthListener(Consumer<SignalingHealth> listener) {
        supervisor.addListener(listener);
    }

    /** Removes a listener added with {@link #addHealthListener}. */
    @Override
    public void removeHealthListener(Consumer<SignalingHealth> listener) {
        supervisor.removeListener(listener);
    }

    /** Starts connecting in the background. {@link #start(JoinSink)} and {@link #connect} call it too. */
    public void start() {
        supervisor.start();
    }

    /**
     * Replaces the socket now, or skips the wait before the next attempt. Players stay connected.
     * An attempt already running is not restarted.
     *
     * @return completes when that attempt succeeds, fails when it fails; the source keeps
     *         retrying in the background either way
     */
    public CompletableFuture<Void> reconnect() {
        return supervisor.reconnect();
    }

    /** Starts the source and returns at once; joins arrive once it is connected. */
    @Override
    public void start(JoinSink sink) {
        this.sink = Objects.requireNonNull(sink, "sink");
        start();
    }

    /**
     * Starts the source if needed.
     *
     * @return the ICE servers once connected; fails if the next connection attempt fails
     */
    @Override
    public CompletableFuture<List<IceServer>> connect(SocketAddress remoteAddress) {
        return supervisor.nextConnection().thenApply(ignored -> iceServers);
    }

    /** The TURN servers of the current credentials, refreshed before they expire. */
    @Override
    public List<IceServer> iceServers() {
        return iceServers;
    }

    @Override
    public void setNotFoundHandler(NotFoundHandler handler) {
        this.notFoundHandler = handler;
    }

    @Override
    public void setSignalHandler(String connectionId, String remoteNetworkId, SignalHandler handler) {
        handlers.put(connectionId, new ClientHandler(Objects.requireNonNull(remoteNetworkId, "remoteNetworkId"),
                Objects.requireNonNull(handler, "handler")));
    }

    /** A client attempt's handler and the peer its signals must come from. */
    private record ClientHandler(String remoteNetworkId, SignalHandler handler) {
    }

    @Override
    public void removeSignalHandler(String connectionId) {
        handlers.remove(connectionId);
    }

    /**
     * Sends a client attempt's signal to the host, addressed by the host's pmid: the service
     * addresses players by pmid and refuses a numeric network ID as the target. An offer that
     * cannot go out, or that the service refuses, ends its attempt at once through the handler's
     * {@link SignalHandler#onFailure}, with the reason. A host the service does not know goes to
     * the not-found handler instead.
     *
     * @param targetNetworkId the host's pmid, a GUID
     */
    @Override
    public void sendSignal(String targetNetworkId, Signal signal) {
        Channel current = currentChannel();
        if (current == null || !current.isActive()) {
            throw new IllegalStateException("Signaling channel is not active");
        }
        if (!PLAYER_ID.matcher(targetNetworkId).matches()) {
            offerFailed(signal, "Xbox RPC signaling addresses the host by its pmid, a GUID, not by its network ID");
            return;
        }
        sendMessage(targetNetworkId, UUID.randomUUID().toString(), signal).whenComplete((ignored, failure) -> {
            Throwable cause = failure instanceof CompletionException wrapped && wrapped.getCause() != null
                    ? wrapped.getCause() : failure;
            // A timeout or a lost socket says nothing about the offer; the attempt's own timeout
            // retries it.
            if (cause instanceof RpcResponseException refused && !refused.notFound) {
                offerFailed(signal, "the signaling service refused it" + SignalingFailure.quote(refused.getMessage()));
            }
        });
    }

    /** Ends the client attempt whose offer cannot reach the host. Its other signals only get a log line. */
    private void offerFailed(Signal signal, String reason) {
        ClientHandler client = handlers.get(signal.connectionId());
        if (signal.type() == SignalType.CONNECT_REQUEST && client != null) {
            client.handler().onFailure(signal.connectionId(), new SignalingFailure(
                    NetherConnectionFailure.NEGOTIATION_FAILED, JoinRefusal.INTERNAL.errorCode(), reason));
        } else {
            log.debug("Could not send a {} signal for connection ID {}: {}", signal.type().wireName(),
                    signal.connectionId(), reason);
        }
    }

    /**
     * Sends a signal to a player. The service accepts a message for a player who is registered
     * but offline, and reports later that it could not deliver it (see {@link #undelivered}).
     *
     * @param messageId the ID the service names the message by, in a receipt or a non-delivery
     *                  notice
     * @return completes once the service accepted the message, or exceptionally when it refused
     *         it, did not answer in time, or no socket is up
     */
    private CompletableFuture<Void> sendMessage(String targetPlayerId, String messageId, Signal signal) {
        JsonObject innerParams = new JsonObject();
        innerParams.addProperty("netherNetId", localNetworkId);
        innerParams.addProperty("message", signal.wire());

        JsonObject innerMsg = new JsonObject();
        innerMsg.add("params", innerParams);
        innerMsg.addProperty("jsonrpc", "2.0");
        innerMsg.addProperty("method", NetherNetConstants.XBOX_RPC_INNER_METHOD_WEBRTC);

        return sendJsonRpcRequest(NetherNetConstants.XBOX_RPC_METHOD_SEND_MESSAGE,
                createSendParams(targetPlayerId, messageId, innerMsg.toString())).thenApply(ignored -> null);
    }

    /** One join's way back to the player who sent its offer. */
    private final class XboxJoin extends MessageReplyPath {
        private final JoinKey key;
        /** The type of each signal sent to the client, by message ID, for non-delivery notices. */
        private final Map<String, SignalType> sent = new ConcurrentHashMap<>();

        private XboxJoin(JoinKey key) {
            super(key.connectionId());
            this.key = key;
        }

        @Override
        CompletionStage<Void> send(Signal signal) {
            String messageId = UUID.randomUUID().toString();
            sent.put(messageId, signal.type());
            return sendMessage(key.sender(), messageId, signal);
        }

        @Override
        void release() {
            joins.remove(key, this);
        }
    }

    @Override
    public void close() {
        Channel current;
        CompletableFuture<List<IceServer>> attempt;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            current = channel;
            channel = null;
            attempt = connectFuture;
            connectFuture = null;
        }
        try {
            supervisor.close();
            if (attempt != null && !attempt.isDone()) {
                attempt.completeExceptionally(new ClosedChannelException());
            }
            if (current != null) {
                current.close();
            }
        } finally {
            joins.clear();
            pendingRequests.forEach((id, request) -> request.future.completeExceptionally(new ClosedChannelException()));
            eventLoopGroup.shutdownGracefully();
        }
    }

    // Socket lifecycle. Called by the supervisor on its executor.

    /**
     * Opens a socket authorized by the token and makes it current.
     *
     * @return completes with the ICE servers once the handshake and the TURN exchange are done
     */
    CompletableFuture<List<IceServer>> openSocket(String token) {
        CompletableFuture<List<IceServer>> future = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                future.completeExceptionally(new ClosedChannelException());
                return future;
            }
            this.token = token;
            connectFuture = future;
            heldSignals.clear();
        }
        ChannelFuture connect;
        try {
            connect = connectSocket(token);
        } catch (Exception e) {
            future.completeExceptionally(e);
            return future;
        }
        synchronized (this) {
            if (connectFuture == future) {
                channel = connect.channel();
            } else {
                connect.channel().close();
            }
        }
        connect.addListener(f -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    /** Connects a new WebSocket to the service with this instance as its last handler. */
    ChannelFuture connectSocket(String token) throws SSLException {
        return connectSocket(token, SERVICE_URI, InetSocketAddress.createUnresolved(SERVICE_URI.getHost(), 443), null);
    }

    /**
     * Connects a new WebSocket to a service at an address, for tests that run their own service.
     *
     * @param service the service URI, whose host the certificate must name
     * @param remote  where to connect; an unresolved address is resolved by the bootstrap
     * @param trust   the trusted roots, null for the JDK's default ones
     */
    ChannelFuture connectSocket(String token, URI service, SocketAddress remote, TrustManagerFactory trust)
            throws SSLException {
        // The upgrade request carries the Xbox token, so the certificate must name the service host.
        // Netty 4.2 already checks the name by default; it is set here so the check does not depend
        // on a library default. XboxSignalingTlsTest fails if it is off.
        SslContext sslCtx = SslContextBuilder.forClient()
                .trustManager(trust)
                .endpointIdentificationAlgorithm("HTTPS")
                .build();
        int port = service.getPort() < 0 ? 443 : service.getPort();
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                service, WebSocketVersion.V13, null, false,
                new DefaultHttpHeaders()
                        .add("Authorization", token)
                        .add("User-Agent", NetherNetConstants.SIGNALING_USER_AGENT)
                        .add("session-id", UUID.randomUUID().toString())
                        .add("request-id", UUID.randomUUID().toString()));

        return new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(sslCtx.newHandler(ch.alloc(), service.getHost(), port));
                        p.addLast(new HttpClientCodec(), new HttpObjectAggregator(8192));
                        // dropPongFrames=false: pongs must reach channelRead, where they refresh
                        // the liveness clock. Close frames are handled there too, not by the
                        // protocol handler, so the reason the service gave gets logged.
                        p.addLast("ws-handshake", new WebSocketClientProtocolHandler(handshaker, false, false));
                        p.addLast("ws-aggregator", new WebSocketFrameAggregator(128 * 1024));
                        p.addLast("handler", NetherNetXboxRpcSignaling.this);
                    }
                })
                .connect(remote);
    }

    /** Drops the current socket without reporting it as lost. */
    void abortSocket() {
        Channel current;
        CompletableFuture<List<IceServer>> attempt;
        synchronized (this) {
            current = channel;
            channel = null;
            attempt = connectFuture;
            connectFuture = null;
            heldSignals.clear();
        }
        if (attempt != null && !attempt.isDone()) {
            attempt.completeExceptionally(new ClosedChannelException());
        }
        if (current != null) {
            current.close();
        }
    }

    /**
     * Drops the socket if it is still current. While it is still being opened its attempt fails;
     * once usable, the supervisor learns it was lost and replaces it.
     */
    private void loseSocket(Channel source, Throwable cause) {
        CompletableFuture<List<IceServer>> attempt;
        synchronized (this) {
            if (!isCurrentChannel(source)) {
                return;
            }
            channel = null;
            attempt = connectFuture;
            connectFuture = null;
            heldSignals.clear();
        }
        if (attempt != null && !attempt.isDone()) {
            attempt.completeExceptionally(cause);
        } else if (attempt != null && !attempt.isCompletedExceptionally()) {
            supervisor.lost(cause);
        }
        source.close();
    }

    synchronized Channel currentChannel() {
        return channel;
    }

    private synchronized boolean isCurrentChannel(Channel source) {
        return !closed && source != null && source == channel;
    }

    // Netty callbacks of the current and replaced sockets.

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt != WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
            super.userEventTriggered(ctx, evt);
            return;
        }
        if (!isCurrentChannel(ctx.channel())) {
            return;
        }
        log.debug("Xbox signaling WebSocket connected");
        lastFrameAtNanos = now(ctx);
        rpcPingFailures = 0;
        routeProven = false;
        routeProbeUnsupported = false;
        routeProbesSent = 0;
        routeUnansweredWarned = false;

        scheduleRecurring(ctx, () -> ctx.writeAndFlush(new PingWebSocketFrame()),
                WS_PING_INTERVAL_SECONDS, WS_PING_INTERVAL_SECONDS);
        scheduleRecurring(ctx, () -> checkLiveness(ctx), LIVENESS_CHECK_SECONDS, LIVENESS_CHECK_SECONDS);
        scheduleRecurring(ctx, () -> sendRpcPing(ctx.channel()), RPC_PING_FIRST_SECONDS, RPC_PING_INTERVAL_SECONDS);
        scheduleRecurring(ctx, () -> sendRouteProbe(ctx), ROUTE_PROBE_INTERVAL_SECONDS, ROUTE_PROBE_INTERVAL_SECONDS);
        refreshTurnCredentials(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!isCurrentChannel(ctx.channel())) {
            ReferenceCountUtil.release(msg);
            return;
        }
        // Every frame, pongs included, proves the socket is alive.
        lastFrameAtNanos = now(ctx);
        if (msg instanceof CloseWebSocketFrame close) {
            String reason;
            try {
                reason = close.statusCode() + " " + close.reasonText();
            } finally {
                close.release();
            }
            // Production logs show 1001 "Signaling server instance is shutting down" as the
            // service's only close reason: it rotates instances, mostly at night.
            log.info("Xbox signaling socket closed by the service: {}", reason);
            loseSocket(ctx.channel(), new IOException("Closed by the signaling service: " + reason));
            return;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        String text = frame.text();
        try {
            JsonObject json = JsonParser.parseString(text).getAsJsonObject();
            if (json.has("result") || (json.has("error") && json.has("id"))) {
                handleResponse(ctx.channel(), json);
            } else if (json.has("method")) {
                handleRequest(ctx.channel(), json);
            }
        } catch (Exception e) {
            log.error("Error processing signaling frame: " + text, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable failure = cause;
        if (cause instanceof WebSocketClientHandshakeException handshake && handshake.response() != null) {
            HttpResponseStatus status = handshake.response().status();
            if (status.equals(HttpResponseStatus.UNAUTHORIZED) || status.equals(HttpResponseStatus.FORBIDDEN)) {
                failure = new SignalingSupervisor.TokenRejectedException(
                        "The signaling service refused the token: " + status, cause);
            }
        }
        if (isCurrentChannel(ctx.channel())) {
            log.warn("Xbox signaling socket failed: {}", failure.getMessage());
            loseSocket(ctx.channel(), failure);
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        CopyOnWriteArrayList<ScheduledFuture<?>> tasks = ctx.channel().attr(SOCKET_TASKS).get();
        if (tasks != null) {
            tasks.forEach(task -> task.cancel(false));
            tasks.clear();
        }
        loseSocket(ctx.channel(), new ClosedChannelException());
        // Fail what waited on a reply over this socket only; its replacement's requests stay.
        pendingRequests.forEach((id, request) -> {
            if (request.channel == ctx.channel()) {
                request.future.completeExceptionally(new ClosedChannelException());
            }
        });
        super.channelInactive(ctx);
    }

    /**
     * Runs a task at a fixed rate while the socket is current and active. It survives its own
     * exceptions, which would otherwise cancel scheduleAtFixedRate silently, and stops with the
     * socket.
     */
    private void scheduleRecurring(ChannelHandlerContext ctx, Runnable task, long initialDelaySeconds, long periodSeconds) {
        trackSocketTask(ctx, ctx.executor().scheduleAtFixedRate(() -> {
            if (!isCurrentChannel(ctx.channel()) || !ctx.channel().isActive()) {
                return;
            }
            try {
                task.run();
            } catch (Throwable t) {
                log.warn("Xbox signaling task failed; it keeps running: {}", t.getMessage());
            }
        }, initialDelaySeconds, periodSeconds, TimeUnit.SECONDS));
    }

    private void scheduleOnce(ChannelHandlerContext ctx, Runnable task, long delaySeconds) {
        ScheduledFuture<?> future = ctx.executor().schedule(() -> {
            if (isCurrentChannel(ctx.channel()) && ctx.channel().isActive()) {
                task.run();
            }
        }, delaySeconds, TimeUnit.SECONDS);
        trackSocketTask(ctx, future);
        // Refresh timers repeat for the socket's whole life; keep only the pending ones.
        future.addListener(done -> {
            CopyOnWriteArrayList<ScheduledFuture<?>> tasks = ctx.channel().attr(SOCKET_TASKS).get();
            if (tasks != null) {
                tasks.remove(future);
            }
        });
    }

    private static void trackSocketTask(ChannelHandlerContext ctx, ScheduledFuture<?> future) {
        ctx.channel().attr(SOCKET_TASKS).setIfAbsent(new CopyOnWriteArrayList<>());
        ctx.channel().attr(SOCKET_TASKS).get().add(future);
    }

    private static long now(ChannelHandlerContext ctx) {
        return ctx.executor().ticker().nanoTime();
    }

    // Liveness.

    private void checkLiveness(ChannelHandlerContext ctx) {
        long now = now(ctx);
        if (now - lastFrameAtNanos > TimeUnit.SECONDS.toNanos(SILENCE_LIMIT_SECONDS)) {
            loseSocket(ctx.channel(), new TimeoutException(
                    "No frame from the signaling service for " + SILENCE_LIMIT_SECONDS + " s"));
        } else if (routeProven && now - routeProvenAtNanos > TimeUnit.SECONDS.toNanos(ROUTE_SILENCE_LIMIT_SECONDS)) {
            loseSocket(ctx.channel(), new TimeoutException(
                    "No route probe came back for " + ROUTE_SILENCE_LIMIT_SECONDS + " s"));
        }
    }

    private void sendRpcPing(Channel source) {
        sendJsonRpcRequest(NetherNetConstants.XBOX_RPC_METHOD_PING, new JsonObject()).whenComplete((result, error) -> {
            if (!isCurrentChannel(source)) {
                return;
            }
            if (error == null || error instanceof RpcResponseException) {
                // An error reply still proves the RPC layer answers.
                rpcPingFailures = 0;
            } else if (error instanceof TimeoutException && ++rpcPingFailures >= RPC_PING_FAILURE_LIMIT) {
                loseSocket(source, new TimeoutException(
                        RPC_PING_FAILURE_LIMIT + " System_Ping requests in a row went unanswered"));
            }
        });
    }

    /**
     * Sends a signal to our own player ID. A live registration routes it back through
     * processIncomingMessage. A dead one answers with an error or, as observed with real joins,
     * makes the service close the socket.
     */
    private void sendRouteProbe(ChannelHandlerContext ctx) {
        Channel source = ctx.channel();
        if (routeProbeUnsupported) {
            return;
        }
        String playerId = localPlayerId();
        if (playerId == null) {
            routeProbeUnsupported = true;
            log.warn("Signaling route probe disabled, the MCToken carries no pmid to address ourselves by");
            return;
        }
        if (!routeProven && routeProbesSent >= 3 && !routeUnansweredWarned) {
            routeUnansweredWarned = true;
            log.warn("Signaling route probe unanswered after {} probes, route liveness cannot be verified on this socket", routeProbesSent);
        }
        routeProbesSent++;

        JsonObject innerMsg = new JsonObject();
        innerMsg.add("params", new JsonObject());
        innerMsg.addProperty("jsonrpc", "2.0");
        innerMsg.addProperty("method", NetherNetConstants.XBOX_RPC_INNER_METHOD_ROUTE_PROBE);
        sendJsonRpcRequest(NetherNetConstants.XBOX_RPC_METHOD_SEND_MESSAGE,
                createSendParams(playerId, UUID.randomUUID().toString(), innerMsg.toString()))
                .whenComplete((result, error) -> {
                    // A lost reply or a local write failure does not prove that the service rejected this route.
                    if (!(error instanceof RpcResponseException) || !isCurrentChannel(source)) {
                        return;
                    }
                    if (!routeProven) {
                        // Without a successful self-probe, a refusal does not prove that peer routing is broken.
                        routeProbeUnsupported = true;
                        log.warn("Signaling route probe rejected on a fresh socket, route liveness disabled: {}", error.getMessage());
                    } else {
                        loseSocket(source, new IOException("Signaling route probe rejected: " + error.getMessage()));
                    }
                });
    }

    // TURN credentials.

    /**
     * Fetches TURN credentials. The first fetch on a socket completes its connect attempt; later
     * ones run before the credentials expire, so peers created on a long-lived socket never get
     * expired ones. A failed or empty refresh keeps the previous credentials.
     */
    private void refreshTurnCredentials(ChannelHandlerContext ctx) {
        Channel source = ctx.channel();
        sendJsonRpcRequest(NetherNetConstants.XBOX_RPC_METHOD_TURN_AUTH, new JsonObject()).whenComplete((response, error) -> {
            CompletableFuture<List<IceServer>> attempt;
            synchronized (this) {
                if (!isCurrentChannel(source)) {
                    return;
                }
                attempt = connectFuture;
            }
            boolean connecting = attempt != null && !attempt.isDone();
            if (error != null) {
                if (connecting) {
                    attempt.completeExceptionally(error);
                } else {
                    log.warn("Failed to refresh TURN credentials, retrying in {} s: {}", TURN_RETRY_SECONDS, error.getMessage());
                    scheduleOnce(ctx, () -> refreshTurnCredentials(ctx), TURN_RETRY_SECONDS);
                }
                return;
            }
            TurnCredentials credentials = parseTurnCredentials(response);
            if (!connecting && credentials.servers().isEmpty()) {
                // The kept credentials expire on their own schedule, not the empty answer's.
                log.warn("TURN refresh returned no servers, keeping the previous ones and retrying in {} s", TURN_RETRY_SECONDS);
                scheduleOnce(ctx, () -> refreshTurnCredentials(ctx), TURN_RETRY_SECONDS);
                return;
            }
            iceServers = credentials.servers();
            scheduleOnce(ctx, () -> refreshTurnCredentials(ctx), turnRefreshDelaySeconds(credentials.expirationSeconds()));
            if (connecting) {
                attempt.complete(iceServers);
                replayHeldSignals(source);
            }
        });
    }

    /** Half the stated lifetime, so a refresh that fails once can be retried before expiry. */
    static long turnRefreshDelaySeconds(long expirationSeconds) {
        if (expirationSeconds <= 0) {
            return TURN_REFRESH_FALLBACK_SECONDS;
        }
        return Math.min(TURN_REFRESH_FALLBACK_SECONDS, Math.max(TURN_REFRESH_MIN_SECONDS, expirationSeconds / 2));
    }

    /** Parses each server on its own, so one malformed entry does not drop the others. */
    private TurnCredentials parseTurnCredentials(JsonObject json) {
        List<IceServer> result = new ArrayList<>();
        JsonElement servers = member(json, "TurnAuthServers", "turnAuthServers");
        if (servers != null && servers.isJsonArray()) {
            for (JsonElement element : servers.getAsJsonArray()) {
                try {
                    JsonObject server = element.getAsJsonObject();
                    JsonElement urls = member(server, "Urls", "urls");
                    if (urls == null || !urls.isJsonArray()) {
                        continue;
                    }
                    List<String> urlList = new ArrayList<>();
                    for (JsonElement url : urls.getAsJsonArray()) {
                        urlList.add(url.getAsString());
                    }
                    if (urlList.isEmpty()) {
                        continue;
                    }
                    result.add(new IceServer(urlList, nonEmpty(member(server, "Username", "username")),
                            nonEmpty(member(server, "Password", "password", "Credential", "credential"))));
                } catch (RuntimeException e) {
                    log.warn("Skipping a malformed TURN server entry: {}", e.getMessage());
                }
            }
        }
        long expiration = 0;
        JsonElement expirationElement = member(json, "ExpirationInSeconds", "expirationInSeconds");
        if (expirationElement != null) {
            try {
                expiration = expirationElement.getAsLong();
            } catch (RuntimeException e) {
                log.debug("Ignoring a malformed TURN credential lifetime: {}", expirationElement);
            }
        }
        log.debug("Parsed {} ICE servers, lifetime {} s", result.size(), expiration);
        return new TurnCredentials(List.copyOf(result), expiration);
    }

    private static String nonEmpty(JsonElement value) {
        return value == null || value.getAsString().isEmpty() ? null : value.getAsString();
    }

    private static JsonElement member(JsonObject json, String... names) {
        for (String name : names) {
            JsonElement value = json.get(name);
            if (value != null && !value.isJsonNull()) {
                return value;
            }
        }
        return null;
    }

    // RPC messages.

    private void handleResponse(Channel source, JsonObject json) {
        if (!json.has("id") || json.get("id").isJsonNull()) return;
        String id = json.get("id").getAsString();
        PendingRequest pending = pendingRequests.get(id);
        if (pending == null || pending.channel != source) {
            return;
        }
        CompletableFuture<JsonObject> future = pending.future;

        try {
            if (json.has("error") && !json.get("error").isJsonNull()) {
                JsonObject error = json.getAsJsonObject("error");
                String msg = error.has("message") ? error.get("message").getAsString() : error.toString();

                boolean isNotFound = msg.contains("Player not registered");
                if (!isNotFound && error.has("data") && error.get("data").isJsonObject()) {
                    JsonObject data = error.getAsJsonObject("data");
                    if (data.has("Code") && "MissingOrExpiredIdentity".equals(data.get("Code").getAsString())) {
                        isNotFound = true;
                    }
                }

                boolean completed = future.completeExceptionally(new RpcResponseException(msg, isNotFound));
                NotFoundHandler handler = notFoundHandler;
                if (completed && isNotFound && isCurrentChannel(source) && handler != null) {
                    handler.onNotFound(msg);
                }
            } else {
                future.complete(json.has("result") && !json.get("result").isJsonNull() ? json.getAsJsonObject("result") : new JsonObject());
            }
        } catch (RuntimeException e) {
            future.completeExceptionally(new IllegalArgumentException("Invalid signaling RPC response", e));
        }
    }

    private void handleRequest(Channel source, JsonObject json) {
        String method = json.get("method").getAsString();
        JsonElement id = json.get("id");

        switch (method) {
            case NetherNetConstants.XBOX_RPC_METHOD_RECEIVE_MESSAGE -> {
                if (id != null) sendJsonRpcResult(id, null);

                JsonElement params = json.get("params");
                if (params != null && params.isJsonArray()) {
                    for (JsonElement el : params.getAsJsonArray()) processIncomingMessage(source, el.getAsJsonObject());
                } else if (params != null && params.isJsonObject()) {
                    processIncomingMessage(source, params.getAsJsonObject());
                }
            }
            case NetherNetConstants.XBOX_RPC_METHOD_PONG, NetherNetConstants.XBOX_RPC_METHOD_PING -> {
                if (id != null) sendJsonRpcResult(id, null);
            }
            default -> {
            }
        }
    }

    /**
     * Handles one message delivered to us. Only WebRTC signals get a delivery receipt, as in
     * gophertunnel and netherconnect. A receipt for anything else risks a loop: a receipt for a
     * receipt makes two hosts acknowledge each other forever, and a receipt for a non-delivery
     * notice goes to a player who is offline, so the service answers it with the next notice
     * (captured 2026-11-08, RpcProbe 3.6 and 3.7).
     */
    private void processIncomingMessage(Channel source, JsonObject msgObj) {
        String from = msgObj.get("From").getAsString();
        String rawInner = msgObj.get("Message").getAsString();
        String msgId = msgObj.has("Id") ? msgObj.get("Id").getAsString() : null;

        JsonObject inner;
        try {
            inner = JsonParser.parseString(rawInner).getAsJsonObject();
        } catch (RuntimeException e) {
            // The service passes on any text (RpcProbe 4.3), so this is the sender's doing.
            log.debug("Ignoring a message from {} that is not a JSON object", from);
            return;
        }
        JsonElement method = inner.get("method");
        if (method == null || !method.isJsonPrimitive()) {
            JsonElement code = inner.get("Code");
            if (msgId != null && code != null && code.isJsonPrimitive() && code.getAsJsonPrimitive().isNumber()) {
                undelivered(from, msgId, code.getAsInt());
            } else {
                log.debug("Ignoring a message from {} without a method", from);
            }
            return;
        }

        String innerMethod = method.getAsString();
        if (NetherNetConstants.XBOX_RPC_INNER_METHOD_WEBRTC.equals(innerMethod)) {
            if (msgId != null) {
                acknowledge(from, msgId);
            }
            try {
                String payload = inner.getAsJsonObject("params").get("message").getAsString();
                if (!holdUntilCredentials(source, from, payload)) {
                    dispatchSignal(from, payload);
                }
            } catch (RuntimeException e) {
                log.debug("Ignoring a WebRTC message from {} without a signal in it", from);
            }
        } else if (NetherNetConstants.XBOX_RPC_INNER_METHOD_ROUTE_PROBE.equals(innerMethod) && isSelf(from)) {
            // Our own route probe came back, so the registration is routable.
            if (!routeProven) {
                log.debug("Signaling route probe confirmed, the registration is routable");
            }
            routeProvenAtNanos = source.eventLoop().ticker().nanoTime();
            routeProven = true;
        }
    }

    /** Tells the sender of a WebRTC message that it arrived. */
    private void acknowledge(String sender, String messageId) {
        JsonObject innerParams = new JsonObject();
        innerParams.addProperty("messageId", messageId);
        JsonObject innerMsg = new JsonObject();
        innerMsg.add("params", innerParams);
        innerMsg.addProperty("jsonrpc", "2.0");
        innerMsg.addProperty("method", NetherNetConstants.XBOX_RPC_INNER_METHOD_DELIVERY);
        sendJsonRpcRequest(NetherNetConstants.XBOX_RPC_METHOD_SEND_MESSAGE,
                createSendParams(sender, UUID.randomUUID().toString(), innerMsg.toString()));
    }

    /**
     * The service could not deliver a message of ours. When a player who is registered but
     * offline is sent a message, the send succeeds, and seconds later the service sends a message
     * "From" that player with {@code {"Code":2,"Message":"Message to player could not be
     * delivered."}} and the original message ID as its {@code Id} (captured 2026-11-08, RpcProbe
     * 3.4 and 3.5). The sender must match the join, so another player cannot end it.
     *
     * <p>A join whose answer did not arrive is abandoned: its client cannot connect without it, and
     * the handshake timeout would only end it later, with a warning. A candidate that did not
     * arrive gets a log line only, since the answer and other candidates may have done their part.
     */
    private void undelivered(String recipient, String messageId, int code) {
        for (XboxJoin join : joins.values()) {
            SignalType type = join.key.sender().equals(recipient) ? join.sent.get(messageId) : null;
            if (type != null) {
                log.debug("Join {}: the signaling service could not deliver its {} to {} (code {})",
                        join.key.connectionId(), type.wireName(), recipient, code);
                if (type == SignalType.CONNECT_RESPONSE) {
                    join.abandon();
                }
                return;
            }
        }
        // A receipt, a refusal sent as the join ended, or a message of a join that is over.
        log.debug("The signaling service could not deliver a message to {} (code {})", recipient, code);
    }

    /** Holds the signal if the socket still waits for its first TURN credentials. */
    private boolean holdUntilCredentials(Channel source, String sender, String message) {
        synchronized (this) {
            if (!isCurrentChannel(source) || connectFuture == null || connectFuture.isDone()) {
                return false;
            }
            if (heldSignals.size() >= HELD_SIGNAL_LIMIT) {
                log.warn("Dropping a signal from {}: {} signals already wait for TURN credentials", sender, HELD_SIGNAL_LIMIT);
                return true;
            }
            heldSignals.add(new HeldSignal(sender, message));
            return true;
        }
    }

    private void replayHeldSignals(Channel source) {
        List<HeldSignal> held;
        synchronized (this) {
            if (!isCurrentChannel(source)) {
                return;
            }
            held = List.copyOf(heldSignals);
            heldSignals.clear();
        }
        held.forEach(signal -> dispatchSignal(signal.sender(), signal.message()));
    }

    private void dispatchSignal(String sender, String rawMsg) {
        Signal signal = Signal.parse(rawMsg).orElse(null);
        if (signal == null) {
            return;
        }
        try {
            String connectionId = signal.connectionId();

            // A signal with a client attempt's ID from another sender is not that attempt's; it may
            // still be an offer of its own, so it goes on to join routing. Senders are pmids, GUIDs
            // whose case does not matter, and the host's may have been dialed in upper case.
            ClientHandler client = handlers.get(connectionId);
            if (client != null && client.remoteNetworkId().equalsIgnoreCase(sender)) {
                client.handler().onSignal(signal);
                return;
            }

            JoinKey key = new JoinKey(sender, connectionId);
            boolean request = signal.type() == SignalType.CONNECT_REQUEST;
            XboxJoin join = joins.get(key);
            if (join != null) {
                if (request) {
                    log.debug("Ignoring a repeated offer for connection ID {}", connectionId);
                } else {
                    join.received(signal);
                }
                return;
            }
            JoinSink current = sink;
            if (request && current != null) {
                XboxJoin created = new XboxJoin(key);
                if (joins.putIfAbsent(key, created) == null) {
                    current.join(new Join(connectionId, sender, signal.payload(),
                            SourcedAddress.unknown(), created));
                }
            } else {
                log.debug("No handler found for connection ID: {} (Type: {})", connectionId, signal.type().wireName());
            }
        } catch (Exception e) {
            // The signal can be an offer with the client's identity token in it, so it is not logged.
            log.error("Failed to dispatch a {} signal ({} chars)", signal.type().wireName(), rawMsg.length(), e);
        }
    }

    /**
     * The RPC transport addresses players by their PlayFab ID, the pmid claim of the MCToken, not
     * by the numeric network ID. Read from the current token, because every attempt can bring a
     * new one.
     *
     * @return our own player ID, or null if the token carries none
     */
    private String localPlayerId() {
        String token = this.token;
        if (token == null) return null;
        String[] parts = token.split(" ", 2);
        if (parts.length < 2) return null;
        String[] jwt = parts[1].split("\\.");
        if (jwt.length < 2) return null;
        try {
            String payload = new String(Base64.getUrlDecoder().decode(jwt[1]), StandardCharsets.UTF_8);
            JsonObject claims = JsonParser.parseString(payload).getAsJsonObject();
            return claims.has("pmid") ? claims.get("pmid").getAsString() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isSelf(String from) {
        if (from.equals(localNetworkId)) return true;
        String playerId = localPlayerId();
        return playerId != null && playerId.equalsIgnoreCase(from);
    }

    private static JsonObject createSendParams(String toPlayerId, String messageId, String message) {
        JsonObject params = new JsonObject();
        params.addProperty("toPlayerId", toPlayerId);
        params.addProperty("messageId", messageId);
        params.addProperty("message", message);
        return params;
    }

    CompletableFuture<JsonObject> sendJsonRpcRequest(String method, JsonObject params) {
        String id = UUID.randomUUID().toString();
        JsonObject rpc = new JsonObject();
        rpc.add("params", params);
        rpc.addProperty("jsonrpc", "2.0");
        rpc.addProperty("method", method);
        rpc.addProperty("id", id);

        CompletableFuture<JsonObject> future = new CompletableFuture<>();
        Channel source = currentChannel();
        if (!isCurrentChannel(source) || !source.isActive()) {
            future.completeExceptionally(new ClosedChannelException());
            return future;
        }
        PendingRequest pending = new PendingRequest(future, source);
        pendingRequests.put(id, pending);
        future.whenComplete((result, error) -> {
            pendingRequests.remove(id, pending);
            pending.cancelTimeout();
        });
        TextWebSocketFrame frame = null;
        try {
            pending.setTimeout(source.eventLoop().schedule(() -> {
                future.completeExceptionally(new TimeoutException("Signaling RPC timed out: " + method));
            }, REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            frame = new TextWebSocketFrame(gson.toJson(rpc));
            source.writeAndFlush(frame).addListener(write -> {
                if (write.isCancelled()) {
                    future.cancel(false);
                } else if (!write.isSuccess()) {
                    future.completeExceptionally(write.cause());
                }
            });
        } catch (RuntimeException e) {
            if (frame != null && frame.refCnt() > 0) {
                frame.release();
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private void sendJsonRpcResult(JsonElement id, JsonElement result) {
        JsonObject response = new JsonObject();
        response.add("id", id);
        response.add("result", result);
        response.addProperty("jsonrpc", "2.0");
        Channel current = currentChannel();
        if (current != null && current.isActive()) current.writeAndFlush(new TextWebSocketFrame(gson.toJson(response)));
    }
}

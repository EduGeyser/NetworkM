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

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.ClientInfo;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.HttpClientSettings;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.HttpClientSettings.Scheme;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The client half of HTTP signaling: reaches a server's {@code /v1/join} endpoint, as a proxy
 * does toward a server behind it. One instance serves one client channel.
 * <p>
 * {@link #connect} asks {@code GET /v1/join} the way the settings say. With
 * {@link Scheme#AUTO}, the default, that is HTTPS first and plain HTTP only when the server
 * refused TLS (closed or reset before answering, or answered with something other than TLS);
 * a certificate the client rejects ends the attempt, as on the retail client. An answer that is
 * not a NetherNet status (a web page, say) fails it too. The offer then goes out as
 * {@code POST /v1/join/{localNetworkId}} on the scheme that answered, and its answer comes back as
 * a {@code CONNECTRESPONSE} signal; a refusal, a {@code 2xx} without an SDP (vanilla BDS answers a
 * refused offer with {@code 200} and body {@code 37}) or a transport failure as a
 * {@code CONNECTERROR}. HTTP carries nothing after the offer, so the offer has to hold every
 * candidate; candidate signals are dropped.
 * <p>
 * Requests are HTTP/1.1 with {@code Connection: close}, follow no redirects, and quote at most
 * {@value #REASON_LIMIT} printable ASCII characters of a server's error body in messages.
 * Everything runs on the given event loop group.
 */
public final class NetherNetHttpClientSignaling implements NetherNetClientSignaling {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetHttpClientSignaling.class);

    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    private static final Duration STATUS_TIMEOUT = Duration.ofSeconds(10);
    /** Far above a status document or an answer, which are a few KB. */
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;
    /** How much of a server's error body is worth carrying into a message. */
    private static final int REASON_LIMIT = 200;

    private final HttpClientSettings settings;
    private final EventLoopGroup group;
    private final String localNetworkId;
    private final ChannelGroup exchanges = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final Map<Long, SignalHandler> handlers = new ConcurrentHashMap<>();
    private volatile Target target;
    private volatile boolean closed;

    /** Where offers go once {@link #connect} found the server. */
    private record Target(InetSocketAddress address, boolean secure, SslContext ssl) {
    }

    /**
     * What a status request found.
     *
     * @param scheme {@link Scheme#HTTPS} or {@link Scheme#HTTP}, the one that answered
     * @param status the status document, or null when the server sent none
     */
    public record Probe(Scheme scheme, NetherNetServerStatus status) {
    }

    /**
     * @param settings how to reach the server
     * @param group    the loops requests run on; not owned. It has to take NIO channels
     */
    public NetherNetHttpClientSignaling(HttpClientSettings settings, EventLoopGroup group) {
        this(settings, group, Long.toUnsignedString(ThreadLocalRandom.current().nextLong()));
    }

    /**
     * @param settings       how to reach the server
     * @param group          the loops requests run on; not owned. It has to take NIO channels
     * @param localNetworkId the network ID to join as, sent in the offer's path
     */
    public NetherNetHttpClientSignaling(HttpClientSettings settings, EventLoopGroup group, String localNetworkId) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.group = Objects.requireNonNull(group, "group");
        this.localNetworkId = Objects.requireNonNull(localNetworkId, "localNetworkId");
    }

    /**
     * Asks a server whether it serves NetherNet, the way the settings say: for
     * {@link Scheme#AUTO}, HTTPS first and plain HTTP when TLS was refused.
     *
     * @param address   the server's signaling endpoint; a name is looked up off the I/O threads
     *                  and is what HTTPS checks the certificate against
     * @param settings  how to reach it
     * @param group     the loops the requests run on; not owned
     * @param networkId the network ID to tell the server in the query, or null for none
     * @return the scheme that answered and the server's status, or a {@link ConnectException}
     *         saying why not
     */
    public static CompletableFuture<Probe> probe(InetSocketAddress address, HttpClientSettings settings,
                                                 EventLoopGroup group, String networkId) {
        SslContext ssl;
        try {
            ssl = settings.scheme() == Scheme.HTTP ? null : clientContext(settings);
        } catch (SSLException e) {
            return CompletableFuture.failedFuture(e);
        }
        return probe(address, settings, group, networkId, ssl, null);
    }

    private static CompletableFuture<Probe> probe(InetSocketAddress address, HttpClientSettings settings,
                                                  EventLoopGroup group, String networkId, SslContext ssl,
                                                  ChannelGroup exchanges) {
        String path = "/v1/join" + query(settings.clientInfo(), networkId);
        return switch (settings.scheme()) {
            case HTTPS -> get(address, ssl, path, group, exchanges)
                    .handle((response, error) -> probed(address, response, error, Scheme.HTTPS, null));
            case HTTP -> get(address, null, path, group, exchanges)
                    .handle((response, error) -> probed(address, response, error, Scheme.HTTP, null));
            case AUTO -> get(address, ssl, path, group, exchanges).handle((response, error) -> {
                if (error == null || !(unwrap(error) instanceof TlsRefusedException refused)) {
                    // An answer over HTTPS is final, whatever it says. So is a rejected certificate,
                    // as on the retail client; and where nothing answered, plain HTTP would only
                    // wait through the same failure again
                    return CompletableFuture.completedFuture(probed(address, response, error, Scheme.HTTPS, null));
                }
                return get(address, null, path, group, exchanges)
                        .handle((fallback, fallbackError) -> probed(address, fallback, fallbackError, Scheme.HTTP,
                                refused));
            }).thenCompose(Function.identity());
        };
    }

    @Override
    public CompletableFuture<List<IceServer>> connect(SocketAddress remoteAddress) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("HTTP client signaling is closed"));
        }
        if (!(remoteAddress instanceof InetSocketAddress address)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("HTTP signaling needs an InetSocketAddress, not " + remoteAddress));
        }
        SslContext ssl;
        try {
            ssl = settings.scheme() == Scheme.HTTP ? null : clientContext(settings);
        } catch (SSLException e) {
            return CompletableFuture.failedFuture(e);
        }
        if (settings.scheme() != Scheme.AUTO) {
            target = new Target(address, settings.scheme() == Scheme.HTTPS, ssl);
            return CompletableFuture.completedFuture(settings.iceServers());
        }
        return probe(address, settings, group, localNetworkId, ssl, exchanges).thenApply(probe -> {
            target = new Target(address, probe.scheme() == Scheme.HTTPS, ssl);
            return settings.iceServers();
        });
    }

    /** False: HTTP carries one offer with every candidate in it, and one answer. */
    @Override
    public boolean trickles() {
        return false;
    }

    /** The longer of the status request's wait and the settings' answer wait. */
    @Override
    public Duration answerTimeout() {
        Duration answer = settings.answerTimeout();
        return answer.compareTo(STATUS_TIMEOUT) >= 0 ? answer : STATUS_TIMEOUT;
    }

    /**
     * Posts the offer of a {@code CONNECTREQUEST}. Candidate and error signals are dropped: HTTP
     * carries nothing after the offer.
     */
    @Override
    public void sendSignal(String targetNetworkId, String data) {
        String[] parts = data.split(" ", 3);
        if (parts.length < 3 || !NetherNetConstants.RTC_NEGOTIATION_CONNECT_REQUEST.equals(parts[0])) {
            log.debug("HTTP signaling carries only the offer; dropped a {} signal", parts[0]);
            return;
        }
        long connectionId;
        try {
            connectionId = Long.parseUnsignedLong(parts[1]);
        } catch (NumberFormatException e) {
            log.debug("Dropped an offer with a malformed connection ID {}", parts[1]);
            return;
        }
        Target current = target;
        if (closed || current == null) {
            fail(connectionId, current, "HTTP client signaling is " + (closed ? "closed" : "not connected"),
                    JoinRefusal.INTERNAL.errorCode());
            return;
        }
        String path = "/v1/join/" + URLEncoder.encode(localNetworkId, StandardCharsets.UTF_8).replace("+", "%20");
        FullHttpRequest request = request(HttpMethod.POST, path, current.address(),
                Unpooled.copiedBuffer(parts[2], StandardCharsets.UTF_8));
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/sdp");
        request.headers().set(HttpHeaderNames.ACCEPT, "application/sdp");
        exchange(current.address(), current.secure() ? current.ssl() : null, request, settings.answerTimeout(), group,
                exchanges).whenComplete((response, error) -> answered(connectionId, current, response, error));
    }

    private void answered(long connectionId, Target current, Response response, Throwable error) {
        if (closed) {
            return;
        }
        if (error != null) {
            Throwable cause = unwrap(error);
            fail(connectionId, current, "could not reach the signaling endpoint: " + describe(cause),
                    cause instanceof TimeoutException ? JoinRefusal.NO_ANSWER_IN_TIME.errorCode()
                            : JoinRefusal.INTERNAL.errorCode());
            return;
        }
        if (response.status() / 100 != 2) {
            fail(connectionId, current, "HTTP " + response.status() + reason(response.body()),
                    errorCodeFor(response.status()));
            return;
        }
        // A refusal can arrive as a 2xx with a short body instead of an SDP, as vanilla BDS does
        if (!response.body().startsWith("v=")) {
            fail(connectionId, current,
                    "HTTP " + response.status() + " without an SDP answer" + reason(response.body()),
                    JoinRefusal.INTERNAL.errorCode());
            return;
        }
        SignalHandler handler = handlers.get(connectionId);
        if (handler != null) {
            handler.onSignal(NetherNetConstants.buildSignalConnectResponse(connectionId, response.body()));
        }
    }

    private void fail(long connectionId, Target current, String reason, int errorCode) {
        log.debug("Signaling to {} failed: {}", current == null ? null : current.address(), reason);
        SignalHandler handler = handlers.get(connectionId);
        if (handler != null) {
            handler.onSignal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR + " "
                    + Long.toUnsignedString(connectionId) + " " + errorCode);
        }
    }

    /** The code of the refusal the server's status stands for, as vanilla reads them from signals. */
    private static int errorCodeFor(int status) {
        for (JoinRefusal refusal : JoinRefusal.values()) {
            if (refusal.httpStatus() == status) {
                return refusal.errorCode();
            }
        }
        return JoinRefusal.INTERNAL.errorCode();
    }

    @Override
    public void setSignalHandler(long connectionId, SignalHandler handler) {
        handlers.put(connectionId, Objects.requireNonNull(handler, "handler"));
    }

    @Override
    public void removeSignalHandler(long connectionId) {
        handlers.remove(connectionId);
    }

    /** Never called: a server reached over HTTP is either there or not, which {@link #connect} reports. */
    @Override
    public void setNotFoundHandler(NotFoundHandler handler) {
    }

    @Override
    public String getLocalNetworkId() {
        return localNetworkId;
    }

    /** Ends requests in flight; their answers are dropped. */
    @Override
    public void close() {
        closed = true;
        handlers.clear();
        exchanges.close();
    }

    /** One HTTP response, its body read as UTF-8. */
    private record Response(int status, String body) {
    }

    /** The server closed, reset or answered with something other than TLS before the handshake finished. */
    private static final class TlsRefusedException extends SSLException {
        TlsRefusedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static SslContext clientContext(HttpClientSettings settings) throws SSLException {
        // HTTP/1.1 only: no ALPN, so the server cannot pick anything else
        return SslContextBuilder.forClient()
                .trustManager(settings.trust())
                .endpointIdentificationAlgorithm("HTTPS")
                .build();
    }

    private static CompletableFuture<Response> get(InetSocketAddress address, SslContext ssl, String path,
                                                   EventLoopGroup group, ChannelGroup exchanges) {
        FullHttpRequest request = request(HttpMethod.GET, path, address, Unpooled.EMPTY_BUFFER);
        request.headers().set(HttpHeaderNames.ACCEPT, "application/json");
        return exchange(address, ssl, request, STATUS_TIMEOUT, group, exchanges);
    }

    private static FullHttpRequest request(HttpMethod method, String path, InetSocketAddress address, ByteBuf body) {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, body);
        String host = address.getHostString();
        // What the player reached goes into Host, as the retail client does; status providers read it
        request.headers()
                .set(HttpHeaderNames.HOST, (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + address.getPort())
                .set(HttpHeaderNames.USER_AGENT, NetherNetConstants.SIGNALING_USER_AGENT)
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes());
        return request;
    }

    /**
     * Runs one request on a connection of its own. The timeout covers all of it: the name lookup,
     * the connection, the TLS handshake and the response. The result always completes, also when
     * the loop group is shutting down, and the request is released whether or not it was written.
     *
     * @param ssl the client context for HTTPS, or null for plain HTTP
     */
    private static CompletableFuture<Response> exchange(InetSocketAddress address, SslContext ssl,
                                                        FullHttpRequest request, Duration timeout,
                                                        EventLoopGroup group, ChannelGroup exchanges) {
        CompletableFuture<Response> result = new CompletableFuture<>();
        // Taken by the write once connected; whatever ends the exchange first releases it otherwise
        AtomicReference<FullHttpRequest> unsent = new AtomicReference<>(request);
        result.whenComplete((response, failure) -> {
            FullHttpRequest left = unsent.getAndSet(null);
            if (left != null) {
                left.release();
            }
        });
        try {
            ScheduledFuture<?> deadline = group.next().schedule(() -> {
                result.completeExceptionally(new TimeoutException("no response within " + timeout.toMillis() + " ms"));
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
            result.whenComplete((response, failure) -> deadline.cancel(false));
        } catch (RejectedExecutionException e) {
            // The loop group is shutting down
            result.completeExceptionally(e);
            return result;
        }
        resolve(address).whenComplete((resolved, failure) -> {
            if (failure != null) {
                result.completeExceptionally(unwrap(failure));
                return;
            }
            try {
                connect(address, resolved, ssl, unsent, group, exchanges, result);
            } catch (RuntimeException e) {
                // Thrown into a stage nobody reads, it would leave the result pending forever
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    /** Looks a name up on a virtual thread, so no I/O thread waits on DNS. */
    private static CompletableFuture<InetSocketAddress> resolve(InetSocketAddress address) {
        if (!address.isUnresolved()) {
            return CompletableFuture.completedFuture(address);
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new InetSocketAddress(InetAddress.getByName(address.getHostString()), address.getPort());
            } catch (UnknownHostException e) {
                throw new CompletionException(e);
            }
        }, task -> Thread.ofVirtual().name("nethernet-http-resolve").start(task));
    }

    private static void connect(InetSocketAddress address, InetSocketAddress resolved, SslContext ssl,
                                AtomicReference<FullHttpRequest> unsent, EventLoopGroup group,
                                ChannelGroup exchanges, CompletableFuture<Response> result) {
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        ChannelPipeline pipeline = channel.pipeline();
                        SslHandler tls = null;
                        if (ssl != null) {
                            ServerSide server = new ServerSide();
                            tls = ssl.newHandler(channel.alloc(), address.getHostString(), address.getPort());
                            // The exchange's deadline bounds the handshake too; Netty's own 10 s
                            // timer would end a stalled handshake with a failure of its own
                            tls.setHandshakeTimeoutMillis(0);
                            pipeline.addLast(server, tls);
                            tls.handshakeFuture().addListener(handshake -> {
                                if (!handshake.isSuccess()) {
                                    result.completeExceptionally(tlsFailure(handshake.cause(), server));
                                }
                            });
                        }
                        pipeline.addLast(new HttpClientCodec(), new HttpObjectAggregator(MAX_RESPONSE_BYTES),
                                new ResponseReader(result, tls));
                    }
                });
        ChannelFuture connected = bootstrap.connect(resolved);
        if (connected.isDone() && !connected.isSuccess()) {
            // A loop that no longer takes tasks refuses the registration at once, and would not run
            // a listener either
            result.completeExceptionally(connected.cause());
            return;
        }
        Channel channel = connected.channel();
        if (exchanges != null) {
            exchanges.add(channel);
        }
        result.whenComplete((response, failure) -> channel.close());
        connected.addListener(future -> {
            if (!future.isSuccess()) {
                result.completeExceptionally(future.cause());
                return;
            }
            FullHttpRequest request = unsent.getAndSet(null);
            if (request != null) {
                channel.writeAndFlush(request).addListener(written -> {
                    if (!written.isSuccess()) {
                        result.completeExceptionally(written.cause());
                    }
                });
            }
        });
    }

    /**
     * Whether a failed handshake is the server refusing TLS, the one case the retail client falls
     * back to plain HTTP for: it closed or reset the connection before sending anything, or what it
     * sent was not TLS. Anything else is final: a certificate the client rejects, an alert, and a
     * handshake that ended without the server ending it (a timeout, or this side closing).
     */
    private static SSLException tlsFailure(Throwable cause, ServerSide server) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof NotSslRecordException) {
                return new TlsRefusedException("the server did not answer with TLS", cause);
            }
        }
        if (!server.spoke && server.ended) {
            return new TlsRefusedException("the server closed the connection before answering", cause);
        }
        return cause instanceof SSLException ssl ? ssl : new SSLException(cause.toString(), cause);
    }

    /**
     * What the server did on a TLS connection, which tells a refused handshake from a failed one.
     * First in the pipeline, so it sees the server's bytes, a reset and the end of the connection
     * before the TLS handler fails the handshake.
     */
    private static final class ServerSide extends ChannelDuplexHandler {
        /** Whether the server sent any byte. */
        boolean spoke;
        /** Whether the server ended the connection: closed it, or reset it. */
        boolean ended;
        private boolean closing;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            spoke = true;
            ctx.fireChannelRead(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof IOException) {
                ended = true;
            }
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // A close this side asked for passed here on its way out; the server's close did not
            if (!closing) {
                ended = true;
            }
            super.channelInactive(ctx);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            closing = true;
            super.close(ctx, promise);
        }
    }

    /** Completes the exchange with the response, or with why there is none. */
    private static final class ResponseReader extends SimpleChannelInboundHandler<FullHttpResponse> {
        private final CompletableFuture<Response> result;
        /** The TLS handler, whose handshake listener reports failures before the handshake ends. */
        private final SslHandler tls;

        ResponseReader(CompletableFuture<Response> result, SslHandler tls) {
            this.result = result;
            this.tls = tls;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) {
            if (!response.decoderResult().isSuccess()) {
                result.completeExceptionally(new ConnectException("malformed response: " + response.decoderResult()));
                return;
            }
            String body = response.content().toString(StandardCharsets.UTF_8);
            result.complete(new Response(response.status().code(), body));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // During the handshake a reset is TLS refused, which only the handshake listener can tell
            if (tls == null || tls.handshakeFuture().isDone()) {
                result.completeExceptionally(cause);
            }
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            result.completeExceptionally(new ClosedChannelException());
            super.channelInactive(ctx);
        }
    }

    /**
     * Reads a status response into a result, or fails with the reason as a
     * {@link ConnectException}, also when no response came.
     *
     * @param httpsRefused why the HTTPS attempt before this plain one failed, when there was one
     */
    private static Probe probed(InetSocketAddress address, Response response, Throwable error, Scheme scheme,
                                Throwable httpsRefused) {
        if (error != null) {
            Throwable cause = unwrap(error);
            throw failure(address + " could not be reached over " + scheme + ": " + describe(cause), cause);
        }
        int code = response.status();
        if (code / 100 == 2) {
            // Vanilla BDS before 1.26.50 answers with an empty body, and serves NetherNet all the same
            if (response.body().isBlank()) {
                return new Probe(scheme, null);
            }
            try {
                return new Probe(scheme, NetherNetServerStatus.fromJson(response.body()));
            } catch (IllegalArgumentException e) {
                throw failure(address + " answered the status request with something other than a NetherNet status",
                        null);
            }
        }
        if (code == 404) {
            throw failure(address + " does not serve NetherNet (HTTP 404)", null);
        }
        if (code == 426) {
            throw failure(address + " requires TLS"
                    + (httpsRefused == null ? "" : ", but refused it: " + describe(httpsRefused)), null);
        }
        throw failure(address + " answered HTTP " + code + " to the status request" + reason(response.body()), null);
    }

    private static CompletionException failure(String message, Throwable cause) {
        ConnectException failure = new ConnectException(message);
        if (cause != null) {
            failure.initCause(cause);
        }
        return new CompletionException(failure);
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /** Netty's exceptions often carry no message, so the type has to say what happened. */
    private static String describe(Throwable cause) {
        if (cause instanceof TimeoutException || cause instanceof ConnectTimeoutException) {
            return "timed out";
        }
        if (cause instanceof ConnectException) {
            return "connection refused";
        }
        if (cause instanceof ClosedChannelException) {
            return "the server closed the connection without answering";
        }
        if (cause instanceof SSLException) {
            return "TLS failed, " + cause.getMessage();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }

    /**
     * The server's stated reason, fit for a message: the body is remote controlled, so it is cut
     * short and reduced to printable ASCII before it can reach a log.
     */
    static String reason(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        StringBuilder out = new StringBuilder(": ");
        for (int i = 0; i < body.length() && out.length() < REASON_LIMIT + 2; i++) {
            char c = body.charAt(i);
            out.append(c >= ' ' && c <= '~' ? c : ' ');
        }
        return out.toString().stripTrailing();
    }

    /** The status request's query, or empty when there is nothing to send. */
    private static String query(ClientInfo info, String networkId) {
        StringJoiner query = new StringJoiner("&", "?", "").setEmptyValue("");
        if (info != null) {
            query.add("version=" + URLEncoder.encode(info.version(), StandardCharsets.UTF_8));
            query.add("protocol=" + info.protocol());
            query.add("platform=" + info.platform());
        }
        if (networkId != null) {
            query.add("id=" + URLEncoder.encode(networkId, StandardCharsets.UTF_8));
        }
        return query.toString();
    }
}

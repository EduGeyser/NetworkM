package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.ClientInfo;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Serves the requests of one HTTP signaling connection: {@code GET /v1/join} for the status, and
 * {@code POST /v1/join/{networkId}} with an offer, handed to the endpoint as a {@link Join} whose
 * reply path writes the HTTP response. Everything else is a {@code 404}, as on vanilla. Bodies of
 * refusals are empty (ruling E-7).
 * <p>
 * Requests are answered in order: one that arrives while another waits for its response is held
 * until that response is written. {@link HttpRequestBudget} lets through at most two.
 */
final class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final String JOIN_PATH = "/v1/join";
    private static final String OFFER_PATH_PREFIX = "/v1/join/";
    static final String FORWARDED_FOR = "X-Forwarded-For";
    /** Long enough for any network ID a client sends; the query is the client's own say. */
    private static final int MAX_QUERY_NETWORK_ID_LENGTH = 64;

    private final NetherNetHttpSignaling signaling;
    private final HttpConnection connection;
    private ChannelHandlerContext ctx;
    private int handled;
    private boolean busy;
    private FullHttpRequest held;
    /** When the held request was read whole, on the loop's ticker. */
    private long heldSince;

    HttpRequestHandler(NetherNetHttpSignaling signaling, HttpConnection connection) {
        this.signaling = signaling;
        this.connection = connection;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        releaseHeld();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseHeld();
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (busy) {
            // Pipelined behind a request still waiting for its response
            if (held == null) {
                held = request.retain();
                heldSince = ctx.executor().ticker().nanoTime();
            }
            return;
        }
        handle(request, ctx.executor().ticker().nanoTime());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Routine on an open port: TLS from clients that do not trust the certificate, scanners,
        // malformed requests, resets
        NetherNetHttpSignaling.log.debug("Closing the signaling connection from {}: {}",
                ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }

    /** @param receivedNanos when the request was read whole, on the loop's ticker */
    private void handle(FullHttpRequest request, long receivedNanos) {
        busy = true;
        boolean first = handled++ == 0;
        if (!request.decoderResult().isSuccess()) {
            respond(HttpResponseStatus.BAD_REQUEST, false);
            return;
        }
        if (signaling.requiresTls() && !connection.tls) {
            signaling.plaintextRefused(signaling.sourceOf(connection, peer(),
                    request.headers().getAll(FORWARDED_FOR)).address());
            FullHttpResponse response = response(HttpResponseStatus.UPGRADE_REQUIRED, null, "");
            response.headers().set(HttpHeaderNames.UPGRADE, "TLS/1.2, HTTP/1.1");
            write(response, false);
            return;
        }
        String uri = request.uri();
        int query = uri.indexOf('?');
        String path = query < 0 ? uri : uri.substring(0, query);
        if (HttpMethod.GET.equals(request.method()) && JOIN_PATH.equals(path)) {
            status(request, first && HttpUtil.isKeepAlive(request));
        } else if (HttpMethod.POST.equals(request.method()) && path.startsWith(OFFER_PATH_PREFIX)) {
            offer(request, path.substring(OFFER_PATH_PREFIX.length()), receivedNanos);
        } else {
            respond(HttpResponseStatus.NOT_FOUND, false);
        }
    }

    /**
     * Answers a status request. A 2xx tells the client NetherNet is served here; a 404 declines
     * it, also whenever the offer would certainly be refused, since this is the last point where
     * a client can still fall back to RakNet.
     */
    private void status(FullHttpRequest request, boolean keep) {
        JoinSink sink = signaling.sink();
        if (sink == null || signaling.isClosed()) {
            respond(HttpResponseStatus.NOT_FOUND, false);
            return;
        }
        SourcedAddress source = signaling.sourceOf(connection, peer(), request.headers().getAll(FORWARDED_FOR));
        if (!admits(sink, source)) {
            NetherNetHttpSignaling.log.debug("Declined NetherNet for {}: a join would be refused now", source);
            respond(HttpResponseStatus.NOT_FOUND, false);
            return;
        }
        HttpStatusService service = signaling.statusService();
        if (service == null) {
            respond(HttpResponseStatus.OK, keep);
            return;
        }
        Map<String, List<String>> parameters = parameters(request.uri());
        StatusRequest statusRequest = new StatusRequest(request.headers().get(HttpHeaderNames.HOST), source,
                ClientInfo.fromQuery(parameters).orElse(null), queryNetworkId(parameters));
        service.query(statusRequest, ctx.executor()).whenComplete((reply, failure) -> onLoop(() -> {
            if (!ctx.channel().isActive()) {
                return;
            }
            switch (reply) {
                case HttpStatusService.Reply.Document document ->
                        write(response(HttpResponseStatus.OK, "application/json", document.json()), keep)
                                .addListener(written -> {
                                    if (written.isSuccess()) {
                                        service.served(statusRequest, document);
                                    }
                                });
                case HttpStatusService.Reply.Declined _ -> respond(HttpResponseStatus.NOT_FOUND, false);
                case HttpStatusService.Reply.Empty _ -> respond(HttpResponseStatus.OK, keep);
                case null -> respond(HttpResponseStatus.OK, keep);
            }
        }));
    }

    private void offer(FullHttpRequest request, String encodedNetworkId, long receivedNanos) {
        String networkId = networkId(encodedNetworkId);
        if (networkId == null) {
            refuseOffer(JoinRefusal.BAD_OFFER);
            return;
        }
        JoinSink sink = signaling.sink();
        if (sink == null || signaling.isClosed()) {
            refuseOffer(JoinRefusal.CAPACITY);
            return;
        }
        String offer = request.content().toString(StandardCharsets.UTF_8);
        if (offer.isEmpty()) {
            refuseOffer(JoinRefusal.BAD_OFFER);
            return;
        }
        InetSocketAddress peer = peer();
        SourcedAddress source = signaling.sourceOf(connection, peer, request.headers().getAll(FORWARDED_FOR));
        int localPort = ctx.channel().localAddress() instanceof InetSocketAddress local ? local.getPort() : 0;
        Join.Addressed addressed = new Join.Addressed(request.headers().get(HttpHeaderNames.HOST), localPort,
                signaling.trustedProxies().isProxy(peer), ClientInfo.fromQuery(parameters(request.uri())).orElse(null));
        // From the end of the request, also for one that waited behind a status request (E-4)
        long remainingNanos = receivedNanos + signaling.negotiationTimeout().toNanos()
                - ctx.executor().ticker().nanoTime();
        if (remainingNanos <= 0) {
            // Too late to start: an endpoint answering at once would beat the expiry task
            NetherNetHttpSignaling.log.debug("Offer from network {} at {} waited past its negotiation deadline",
                    networkId, source);
            refuseOffer(JoinRefusal.NO_ANSWER_IN_TIME);
            return;
        }
        Exchange exchange = new Exchange(networkId);
        if (!signaling.claimPending(networkId, exchange)) {
            NetherNetHttpSignaling.log.debug("Refused an offer from network {} at {}: another offer for it is pending",
                    networkId, source);
            refuseOffer(JoinRefusal.DUPLICATE);
            return;
        }
        exchange.start(remainingNanos);
        Join join = new Join(exchange.connectionId, networkId, offer, source, exchange, addressed);
        NetherNetHttpSignaling.log.debug("Offer {} from network {} at {} via HTTP",
                exchange.connectionId, networkId, source);
        try {
            sink.join(join);
        } catch (RuntimeException e) {
            NetherNetHttpSignaling.log.warn("The endpoint failed to take a join", e);
            signaling.joinRefused(JoinRefusal.INTERNAL);
            exchange.refuse(JoinRefusal.INTERNAL);
        }
    }

    private static boolean admits(JoinSink sink, SourcedAddress source) {
        try {
            return sink.admits(source);
        } catch (RuntimeException e) {
            // The join itself is checked again, so a failing check does not decline anybody
            NetherNetHttpSignaling.log.debug("The endpoint's admission check failed", e);
            return true;
        }
    }

    /**
     * Decodes the network ID path segment once, to bytes first and then as strict UTF-8, so that
     * invalid UTF-8 cannot make two IDs equal. Path decoding keeps a literal {@code +}.
     *
     * @return the ID, or null when the segment is empty, has more segments or does not decode
     */
    static String networkId(String encoded) {
        if (encoded.isEmpty() || encoded.indexOf('/') >= 0 || encoded.indexOf('#') >= 0) {
            return null;
        }
        try {
            String bytes = new QueryStringDecoder("/" + encoded, StandardCharsets.ISO_8859_1).path().substring(1);
            return StandardCharsets.UTF_8.newDecoder()
                    .decode(ByteBuffer.wrap(bytes.getBytes(StandardCharsets.ISO_8859_1))).toString();
        } catch (IllegalArgumentException | CharacterCodingException e) {
            return null;
        }
    }

    private static Map<String, List<String>> parameters(String uri) {
        try {
            return new QueryStringDecoder(uri).parameters();
        } catch (IllegalArgumentException e) {
            // A bad escape costs the client its info and nothing more
            return Map.of();
        }
    }

    private static String queryNetworkId(Map<String, List<String>> parameters) {
        List<String> values = parameters.get("id");
        if (values == null || values.isEmpty()) {
            return null;
        }
        String id = values.getFirst();
        if (id.isEmpty() || id.length() > MAX_QUERY_NETWORK_ID_LENGTH || id.chars().anyMatch(Character::isISOControl)) {
            return null;
        }
        return id;
    }

    private InetSocketAddress peer() {
        return (InetSocketAddress) ctx.channel().remoteAddress();
    }

    /** Refuses an offer this source turned away itself, before or instead of the endpoint. */
    private void refuseOffer(JoinRefusal refusal) {
        signaling.joinRefused(refusal);
        respond(HttpResponseStatus.valueOf(refusal.httpStatus()), false);
    }

    private ChannelFuture respond(HttpResponseStatus status, boolean keep) {
        return write(response(status, null, ""), keep);
    }

    private static FullHttpResponse response(HttpResponseStatus status, String contentType, String body) {
        ByteBuf content = body.isEmpty() ? Unpooled.EMPTY_BUFFER : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        if (contentType != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
        return response;
    }

    /**
     * Writes a response. Only a kept one leaves the connection open, see {@link HttpRequestBudget};
     * once it is written, a request held behind it is served.
     */
    private ChannelFuture write(FullHttpResponse response, boolean keep) {
        if (keep) {
            connection.keepOpen = response;
        }
        ChannelFuture written = ctx.writeAndFlush(response);
        if (keep) {
            written.addListener((ChannelFutureListener) future -> {
                busy = false;
                FullHttpRequest next = held;
                held = null;
                if (next != null) {
                    try {
                        if (future.isSuccess()) {
                            handle(next, heldSince);
                        }
                    } finally {
                        next.release();
                    }
                }
            });
        }
        return written;
    }

    private void releaseHeld() {
        if (held != null) {
            held.release();
            held = null;
        }
    }

    private void onLoop(Runnable task) {
        if (ctx.executor().inEventLoop()) {
            task.run();
        } else {
            try {
                ctx.executor().execute(task);
            } catch (RejectedExecutionException e) {
                // The loop is shutting down, and the connection with it
                NetherNetHttpSignaling.log.debug("Dropped a signaling response: the I/O loop is shutting down");
            }
        }
    }

    /** Where a pending offer stands. Changed on the connection's loop only. */
    private enum State {
        PENDING, ANSWERED, REFUSED, TIMED_OUT, ABANDONED
    }

    /**
     * The reply path of one offer: the HTTP response it waits for. The answer or a refusal is the
     * last response on the connection. A negotiation deadline runs from the end of the request; a
     * client that leaves, or a deadline that passes, completes {@link #abandoned()}.
     * <p>
     * The offer holds its network ID as pending, so a second offer with it is a
     * {@link JoinRefusal#DUPLICATE}, until the join fails (refused, past its deadline, its client
     * gone before the answer, or its connection closed before going active) or goes active. A
     * player whose join failed can retry at once.
     */
    private final class Exchange implements ReplyPath {
        private final String connectionId = Signal.newConnectionId();
        private final String networkId;
        private final CompletableFuture<Void> abandoned = new CompletableFuture<>();
        private final CompletionStage<Void> abandonedView = abandoned.minimalCompletionStage();
        private State state = State.PENDING;
        private ScheduledFuture<?> deadline;

        Exchange(String networkId) {
            this.networkId = networkId;
        }

        /** @param remainingNanos how long the negotiation deadline is still ahead, more than zero */
        void start(long remainingNanos) {
            deadline = ctx.executor().schedule(this::expire, remainingNanos, TimeUnit.NANOSECONDS);
            ctx.channel().closeFuture().addListener(future -> onLoop(this::left));
        }

        @Override
        public boolean trickles() {
            return false;
        }

        @Override
        public CompletionStage<Void> answer(String sdp) {
            Objects.requireNonNull(sdp, "sdp");
            return settle(State.ANSWERED, () -> response(HttpResponseStatus.OK, "application/sdp", sdp));
        }

        @Override
        public CompletionStage<Void> candidate(String candidate) {
            throw new UnsupportedOperationException("HTTP signaling answers once, with every candidate in it");
        }

        @Override
        public CompletionStage<Void> refuse(JoinRefusal refusal) {
            Objects.requireNonNull(refusal, "refusal");
            return settle(State.REFUSED, () -> response(HttpResponseStatus.valueOf(refusal.httpStatus()), null, ""));
        }

        @Override
        public void onRemoteSignal(Consumer<RemoteSignal> handler) {
            Objects.requireNonNull(handler, "handler");
            // A client sends nothing after its offer over HTTP
        }

        @Override
        public CompletionStage<Void> abandoned() {
            return abandonedView;
        }

        @Override
        public void connected() {
            signaling.releasePending(networkId, this);
        }

        @Override
        public void finished() {
            signaling.releasePending(networkId, this);
        }

        private CompletableFuture<Void> settle(State outcome, Supplier<FullHttpResponse> response) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            Runnable task = () -> {
                if (state != State.PENDING) {
                    result.completeExceptionally(new IllegalStateException(
                            "The HTTP request is no longer waiting for a reply: " + state));
                    return;
                }
                state = outcome;
                deadline.cancel(false);
                if (outcome == State.REFUSED) {
                    signaling.releasePending(networkId, this);
                }
                write(response.get(), false).addListener(future -> {
                    if (future.isSuccess()) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(future.cause());
                        if (outcome == State.ANSWERED) {
                            // The client never got the answer
                            signaling.releasePending(networkId, this);
                            abandoned.complete(null);
                        }
                    }
                });
            };
            if (ctx.executor().inEventLoop()) {
                task.run();
            } else {
                try {
                    ctx.executor().execute(task);
                } catch (RejectedExecutionException e) {
                    result.completeExceptionally(e);
                }
            }
            return result;
        }

        private void expire() {
            if (state != State.PENDING) {
                return;
            }
            state = State.TIMED_OUT;
            NetherNetHttpSignaling.log.debug("Offer {} got no answer in time", connectionId);
            signaling.releasePending(networkId, this);
            refuseOffer(JoinRefusal.NO_ANSWER_IN_TIME);
            abandoned.complete(null);
        }

        private void left() {
            if (state != State.PENDING) {
                return;
            }
            state = State.ABANDONED;
            deadline.cancel(false);
            signaling.releasePending(networkId, this);
            abandoned.complete(null);
        }
    }
}

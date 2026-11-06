package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

/**
 * The connection model of ruling E-4. A connection carries one request, or a
 * {@code GET /v1/join} status request and then one more request that starts within
 * {@value #GAP_SECONDS} s of the status response. Each request has to arrive whole within
 * {@value #REQUEST_SECONDS} s, counted from the connection's start for the first one (TLS
 * handshake included) and from the status response for the second; these are absolute deadlines,
 * so a client sending a byte now and then gains nothing. Missing either closes the connection
 * unanswered.
 * <p>
 * Every final response ends the connection, says {@code Connection: close}, and closes once
 * written, except the one status response {@link HttpConnection#keepOpen} names. That includes the
 * responses the aggregator writes itself ({@code 413}, {@code 417}), which would otherwise leave a
 * connection open with no timer. Input past the last request is discarded unread.
 * <p>
 * Sits between the HTTP codec and the aggregator, where request starts and ends are visible and
 * every response passes. No timer runs while a request waits for its response: the status and
 * negotiation deadlines bound that.
 */
final class HttpRequestBudget extends ChannelDuplexHandler {
    static final int REQUEST_SECONDS = 10;
    static final int GAP_SECONDS = 5;
    private static final String JOIN_PATH = "/v1/join";

    private final NetherNetHttpSignaling signaling;
    private final HttpConnection connection;
    private final HttpServerCodec codec;
    private int started;
    private int completed;
    private int responded;
    /** Whether the first request is a status request, which one more request may follow. */
    private boolean secondAllowed;
    private boolean discarding;
    private ScheduledFuture<?> requestDeadline;
    private ScheduledFuture<?> gap;

    HttpRequestBudget(NetherNetHttpSignaling signaling, HttpConnection connection, HttpServerCodec codec) {
        this.signaling = signaling;
        this.connection = connection;
        this.codec = codec;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        requestDeadline = ctx.executor().schedule(() -> expire(ctx), REQUEST_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelTimers();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelTimers();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (discarding) {
            ReferenceCountUtil.release(msg);
            return;
        }
        if (msg instanceof HttpRequest request) {
            started++;
            if (started == 1) {
                secondAllowed = isStatusRequest(request);
            } else {
                cancel(gap);
                gap = null;
                if (requestDeadline == null) {
                    // Pipelined behind the status request, before its response started the clock
                    requestDeadline = ctx.executor().schedule(() -> expire(ctx), REQUEST_SECONDS, TimeUnit.SECONDS);
                }
            }
        }
        if (msg instanceof LastHttpContent) {
            completed++;
            cancel(requestDeadline);
            requestDeadline = null;
            if (completed >= 2 || !secondAllowed) {
                stopReading();
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!(msg instanceof HttpResponse response) || response.status().code() < 200) {
            // Interim responses such as 100 Continue leave the request open
            ctx.write(msg, promise);
            return;
        }
        ChannelPromise written = promise.unvoid();
        boolean keep = response == connection.keepOpen && secondAllowed && responded == 0;
        responded++;
        connection.keepOpen = null;
        if (keep) {
            ctx.write(msg, written);
            written.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    future.channel().close();
                    return;
                }
                if (started < 2) {
                    gap = ctx.executor().schedule(() -> gapPassed(ctx), GAP_SECONDS, TimeUnit.SECONDS);
                }
                if (completed < 2 && requestDeadline == null) {
                    requestDeadline = ctx.executor().schedule(() -> expire(ctx), REQUEST_SECONDS, TimeUnit.SECONDS);
                }
            });
            return;
        }
        // The last response: whatever comes after it is not read
        HttpUtil.setKeepAlive(response, false);
        stopReading();
        cancelTimers();
        ctx.write(msg, written);
        written.addListener(ChannelFutureListener.CLOSE);
    }

    /** A request did not arrive whole within its deadline. */
    private void expire(ChannelHandlerContext ctx) {
        // Routine on an open port, so no exception and no log above debug
        NetherNetHttpSignaling.log.debug("Closing the signaling connection from {}: no complete request in time",
                ctx.channel().remoteAddress());
        signaling.requestTimedOut(connection);
        ctx.close();
    }

    /** No second request followed the status response: a client that needed only the status. */
    private void gapPassed(ChannelHandlerContext ctx) {
        NetherNetHttpSignaling.log.debug("Closing the signaling connection from {}: no second request",
                ctx.channel().remoteAddress());
        ctx.close();
    }

    private void stopReading() {
        if (discarding) {
            return;
        }
        discarding = true;
        // The response encoder stays; the decoder would only parse input nobody reads
        codec.removeInboundHandler();
    }

    private void cancelTimers() {
        cancel(requestDeadline);
        cancel(gap);
        requestDeadline = null;
        gap = null;
    }

    private static void cancel(ScheduledFuture<?> timer) {
        if (timer != null) {
            timer.cancel(false);
        }
    }

    private static boolean isStatusRequest(HttpRequest request) {
        String uri = request.uri();
        int query = uri.indexOf('?');
        String path = query < 0 ? uri : uri.substring(0, query);
        return HttpMethod.GET.equals(request.method()) && JOIN_PATH.equals(path);
    }
}

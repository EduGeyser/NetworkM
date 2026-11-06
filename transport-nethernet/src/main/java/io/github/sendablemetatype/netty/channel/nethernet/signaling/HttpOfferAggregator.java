package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Netty's aggregator, reporting the offers it answers itself: a body over the cap ({@code 413})
 * and an expectation it does not meet ({@code 417}, or {@code 413} for a {@code 100-continue}
 * with too long a body). Such a request never reaches {@link HttpRequestHandler}, which reports
 * every other refused offer. A request that is no offer is no join, so its {@code 413} or
 * {@code 417} is not reported.
 */
final class HttpOfferAggregator extends HttpObjectAggregator {
    private final NetherNetHttpSignaling signaling;

    /** @param maxContentLength the largest body taken */
    HttpOfferAggregator(NetherNetHttpSignaling signaling, int maxContentLength) {
        super(maxContentLength);
        this.signaling = signaling;
    }

    @Override
    protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
        Object response = super.newContinueResponse(start, maxContentLength, pipeline);
        if (response instanceof HttpResponse refusal && refusal.status().code() >= 400) {
            refused(start, refusal.status().code());
        }
        return response;
    }

    @Override
    protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        refused(oversized, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE.code());
        super.handleOversizedMessage(ctx, oversized);
    }

    private void refused(HttpMessage message, int httpStatus) {
        if (message instanceof HttpRequest request && HttpRequestHandler.isOffer(request)) {
            signaling.joinRefused(httpStatus);
        }
    }
}

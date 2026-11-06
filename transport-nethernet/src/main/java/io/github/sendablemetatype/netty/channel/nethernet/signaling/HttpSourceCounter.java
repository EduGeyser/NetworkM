package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;

/**
 * Counts the client a trusted proxy declares against its address as soon as a request's headers
 * arrive, before the body is read. Connections from trusted proxies are not counted when they
 * open, since every client behind a proxy shares its address; counting only after the whole
 * request would let one client behind a proxy that streams bodies hold connections up to the
 * global cap with headers sent and bodies unfinished. Each request is counted: a proxy that sends
 * one client's status request and another client's offer on one connection moves the count to the
 * second client.
 * <p>
 * Added only for connections from trusted proxies, between the HTTP codec and the aggregator.
 */
final class HttpSourceCounter extends ChannelInboundHandlerAdapter {
    private final NetherNetHttpSignaling signaling;
    private final HttpConnection connection;
    private boolean refused;

    HttpSourceCounter(NetherNetHttpSignaling signaling, HttpConnection connection) {
        this.signaling = signaling;
        this.connection = connection;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (refused) {
            ReferenceCountUtil.release(msg);
            return;
        }
        if (msg instanceof HttpRequest request) {
            SourcedAddress source = signaling.sourceOf(connection, (InetSocketAddress) ctx.channel().remoteAddress(),
                    request.headers().getAll(HttpRequestHandler.FORWARDED_FOR));
            if (!connection.countAddress(source.address().getAddress())) {
                NetherNetHttpSignaling.log.debug("Refused a signaling connection for {}: its address holds its share",
                        source);
                signaling.addressRefused(source.address());
                refused = true;
                ReferenceCountUtil.release(msg);
                ctx.close();
                return;
            }
        }
        ctx.fireChannelRead(msg);
    }
}

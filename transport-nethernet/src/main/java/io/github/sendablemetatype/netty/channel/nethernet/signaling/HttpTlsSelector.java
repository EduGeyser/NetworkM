package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.TlsContextSelector;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.AbstractSniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;

import java.util.List;

/**
 * Tells TLS from plain HTTP by a connection's first bytes, so both are served on the one port a
 * client derives from the join address. A TLS record goes to {@link SniLookup}, which reads the
 * ClientHello and asks the {@link TlsContextSelector}; anything else is plain HTTP.
 */
final class HttpTlsSelector extends ByteToMessageDecoder {
    private final HttpConnection connection;
    private final NetherNetHttpSignaling signaling;

    HttpTlsSelector(HttpConnection connection, NetherNetHttpSignaling signaling) {
        this.connection = connection;
        this.signaling = signaling;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // A TLS record is recognizable from its first five bytes
        if (in.readableBytes() < 5) {
            return;
        }
        if (SslHandler.isEncrypted(in, false)) {
            TlsContextSelector selector = signaling.tlsSelector();
            if (selector == null) {
                // No TLS here at all: closing before any handshake byte reaches the HTTP codec is
                // also the quickest refusal for the client's fallback to plain HTTP
                signaling.tlsRefused(null, "no TLS selector is configured");
                ctx.close();
                return;
            }
            ctx.pipeline().addAfter(ctx.name(), null, new SniLookup(selector));
        }
        // Removing the decoder hands the buffered bytes to whatever follows: the SNI lookup for
        // TLS, the HTTP codec for plain HTTP
        ctx.pipeline().remove(this);
    }

    /** Reads the server name from the ClientHello, then installs the context the selector picks. */
    private final class SniLookup extends AbstractSniHandler<SslContext> {
        private final TlsContextSelector selector;

        SniLookup(TlsContextSelector selector) {
            // No handshake timeout of its own: the request deadline covers the handshake
            super(0L);
            this.selector = selector;
        }

        @Override
        protected Future<SslContext> lookup(ChannelHandlerContext ctx, String hostname) {
            try {
                return ctx.executor().newSucceededFuture(selector.select(hostname));
            } catch (RuntimeException e) {
                return ctx.executor().newFailedFuture(e);
            }
        }

        @Override
        protected void onLookupComplete(ChannelHandlerContext ctx, String hostname, Future<SslContext> future) {
            if (!future.isSuccess()) {
                signaling.tlsSelectorFailed(hostname, future.cause());
                ctx.close();
                return;
            }
            SslContext context = future.getNow();
            if (context == null) {
                signaling.tlsRefused(hostname, "the TLS selector withdrew TLS");
                ctx.close();
                return;
            }
            SslHandler handler = context.newHandler(ctx.alloc());
            try {
                ctx.pipeline().replace(this, null, handler);
            } catch (Throwable e) {
                // The engine is reference counted with some providers and nothing else owns it yet
                ReferenceCountUtil.safeRelease(handler.engine());
                throw e;
            }
            connection.tls = true;
            handler.handshakeFuture().addListener(handshake -> {
                if (!handshake.isSuccess()) {
                    signaling.tlsHandshakeFailed(connection, hostname, handshake.cause());
                }
            });
        }
    }
}

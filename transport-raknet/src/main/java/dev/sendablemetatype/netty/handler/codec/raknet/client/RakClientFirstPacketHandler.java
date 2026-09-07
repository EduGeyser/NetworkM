package dev.sendablemetatype.netty.handler.codec.raknet.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;

public class RakClientFirstPacketHandler extends ChannelOutboundHandlerAdapter {
    public static final String NAME = "rak-client-first-packet-handler";

    private final Promise<Object> packetPromise;

    public RakClientFirstPacketHandler(Promise<Object> packetPromise) {
        this.packetPromise = packetPromise;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Replay the first application write with its original promise after the handshake batch.
        if (!this.packetPromise.trySuccess(new PendingWrite(msg, promise))) {
            ReferenceCountUtil.release(msg);
            promise.tryFailure(new ClosedChannelException());
        }
        ctx.pipeline().remove(this);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (!this.packetPromise.isDone()) {
            this.packetPromise.tryFailure(new ClosedChannelException());
        }
    }

    static final class PendingWrite {
        final Object message;
        final ChannelPromise promise;

        PendingWrite(Object message, ChannelPromise promise) {
            this.message = message;
            this.promise = promise;
        }
    }
}

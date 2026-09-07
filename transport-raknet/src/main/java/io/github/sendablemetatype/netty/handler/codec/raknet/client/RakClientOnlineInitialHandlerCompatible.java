package io.github.sendablemetatype.netty.handler.codec.raknet.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import io.netty.util.ReferenceCountUtil;
import io.netty.handler.codec.CorruptedFrameException;
import io.github.sendablemetatype.netty.channel.raknet.RakChannel;
import io.github.sendablemetatype.netty.channel.raknet.RakPriority;
import io.github.sendablemetatype.netty.channel.raknet.RakReliability;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelOption;
import io.github.sendablemetatype.netty.channel.raknet.packet.RakMessage;
import io.github.sendablemetatype.netty.util.RakUtils;

import java.net.InetSocketAddress;

import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.ID_CONNECTED_PING;
import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.ID_NEW_INCOMING_CONNECTION;
import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.IPV4_MESSAGE_SIZE;
import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.LOCAL_ADDRESS;

public class RakClientOnlineInitialHandlerCompatible extends RakClientOnlineInitialHandler {
    public static final String NAME = "rak-client-online-initial-handler";

    private final Promise<Object> packetPromise;
    private long pingTime = 0;

    public RakClientOnlineInitialHandlerCompatible(RakChannel rakChannel, ChannelPromise successPromise, Promise<Object> packetPromise) {
        super(rakChannel, successPromise);
        this.packetPromise = packetPromise;
    }

    @Override
    void onSuccess(ChannelHandlerContext ctx) {
        super.onSuccess(ctx);

        // Wait for the first game packet (request network settings) before sending the final batch
        this.packetPromise.addListener(future -> {
            if (!future.isSuccess()) {
                return;
            }
            RakClientFirstPacketHandler.PendingWrite pending = (RakClientFirstPacketHandler.PendingWrite) future.getNow();
            if (!ctx.channel().isActive()) {
                ReferenceCountUtil.release(pending.message);
                pending.promise.tryFailure(new java.nio.channels.ClosedChannelException());
                return;
            }
            ByteBuf incomingBuffer = ctx.alloc().ioBuffer();
            this.writeIncomingConnection(ctx, incomingBuffer, pingTime);
            ctx.write(new RakMessage(incomingBuffer, RakReliability.RELIABLE_ORDERED, RakPriority.NORMAL));

            ByteBuf pingBuffer = ctx.alloc().ioBuffer();
            pingBuffer.writeByte(ID_CONNECTED_PING);
            pingBuffer.writeLong(System.nanoTime() / 1_000_000L);
            ctx.write(new RakMessage(pingBuffer, RakReliability.UNRELIABLE, RakPriority.NORMAL));

            ctx.write(pending.message, pending.promise);

            ctx.flush();
        });
    }

    @Override
    void onConnectionRequestAccepted(ChannelHandlerContext ctx, ByteBuf buf) {
        buf.skipBytes(1);
        if (!RakUtils.skipAddress(buf)) {
            throw new CorruptedFrameException("Truncated client address");
        }
        buf.skipBytes(2); // System index

        while (buf.isReadable(IPV4_MESSAGE_SIZE + 16)) {
            if (!RakUtils.skipAddress(buf)) {
                throw new CorruptedFrameException("Truncated internal address");
            }
        }
        this.pingTime = buf.readLong();
    }

    @Override
    void writeIncomingConnection(ChannelHandlerContext ctx, ByteBuf buf, long pingTime) {
        buf.writeByte(ID_NEW_INCOMING_CONNECTION);
        RakUtils.writeAddress(buf, (InetSocketAddress) ctx.channel().remoteAddress());

        RakUtils.writeAddress(buf, (InetSocketAddress) ctx.channel().localAddress());
        for (int i = 0; i < this.rakChannel().config().getOption(RakChannelOption.RAK_CLIENT_INTERNAL_ADDRESSES) - 1; i++) {
            RakUtils.writeAddress(buf, LOCAL_ADDRESS);
        }

        buf.writeLong(pingTime);
        buf.writeLong(System.nanoTime() / 1_000_000L);
    }
}

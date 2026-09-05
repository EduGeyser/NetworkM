package org.cloudburstmc.netty.handler.codec.raknet.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RakFirstPacketTests {
    @Test
    void replayCompletesTheOriginalWritePromise() {
        EmbeddedChannel channel = new EmbeddedChannel();
        Promise<Object> captured = channel.eventLoop().newPromise();
        channel.pipeline().addLast(new RakClientFirstPacketHandler(captured));
        ByteBuf payload = Unpooled.buffer(1).writeByte(0xfe);
        try {
            ChannelFuture write = channel.writeOneOutbound(payload);
            assertTrue(captured.isSuccess());
            assertFalse(write.isDone());
            RakClientFirstPacketHandler.PendingWrite pending = (RakClientFirstPacketHandler.PendingWrite) captured.getNow();
            channel.writeAndFlush(pending.message, pending.promise);
            assertTrue(write.isSuccess());
            ByteBuf sent = channel.readOutbound();
            assertEquals(0xfe, sent.readUnsignedByte());
            sent.release();
            assertEquals(0, payload.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void canceledCaptureFailsAndReleasesTheWrite() {
        EmbeddedChannel channel = new EmbeddedChannel();
        Promise<Object> captured = channel.eventLoop().newPromise();
        captured.cancel(false);
        channel.pipeline().addLast(new RakClientFirstPacketHandler(captured));
        ByteBuf payload = Unpooled.buffer(1).writeByte(0xfe);
        try {
            assertNotNull(channel.writeOneOutbound(payload).cause());
            assertEquals(0, payload.refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void closingBeforeTheFirstWriteCompletesTheCaptureExceptionally() {
        EmbeddedChannel channel = new EmbeddedChannel();
        Promise<Object> captured = channel.eventLoop().newPromise();
        channel.pipeline().addLast(new RakClientFirstPacketHandler(captured));
        channel.finishAndReleaseAll();
        assertTrue(captured.isDone());
        assertFalse(captured.isSuccess());
    }
}

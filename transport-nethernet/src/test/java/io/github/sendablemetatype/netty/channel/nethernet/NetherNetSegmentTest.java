package io.github.sendablemetatype.netty.channel.nethernet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.Test;

import static io.github.sendablemetatype.netty.channel.nethernet.ActiveChild.payload;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How the channel splits outgoing messages into segments. */
class NetherNetSegmentTest {

    @Test
    void messagesAreSplitAtTheNegotiatedSizeWithACountdown() {
        try (ActiveChild h = new ActiveChild(child -> child.setMaxOutboundMessageSize(4))) {
            ChannelFuture write = h.channel.writeAndFlush(payload(1, 2, 3, 4, 5, 6, 7));
            assertEquals(3, h.reliable().sendCount());
            assertArrayEquals(new byte[]{2, 1, 2, 3}, h.reliable().sentBytes().get(0));
            assertArrayEquals(new byte[]{1, 4, 5, 6}, h.reliable().sentBytes().get(1));
            assertArrayEquals(new byte[]{0, 7}, h.reliable().sentBytes().get(2));
            h.reliable().accept(0);
            h.reliable().accept(1);
            h.run();
            assertFalse(write.isDone(), "one promise per message, completed by the last segment");
            h.reliable().accept(2);
            h.run();
            assertTrue(write.isSuccess());
        }
    }

    @Test
    void theLargestMessageFollowsTheNegotiatedSize() {
        try (ActiveChild h = new ActiveChild()) {
            assertEquals(256 * 65535, h.channel.maxMessageSize());
            h.channel.setMaxOutboundMessageSize(0);
            assertEquals(256 * 262143, h.channel.maxMessageSize());
        }
    }

    @Test
    void aMessageTheCountdownCannotNumberFailsBeforeAnythingIsSent() {
        try (ActiveChild h = new ActiveChild(child -> child.setMaxOutboundMessageSize(4))) {
            ByteBuf largest = Unpooled.buffer().writeZero(256 * 3);
            ByteBuf tooLarge = Unpooled.buffer().writeZero(256 * 3 + 1);
            ChannelFuture refused = h.channel.writeAndFlush(tooLarge);
            assertInstanceOf(IllegalArgumentException.class, refused.cause());
            assertEquals(0, tooLarge.refCnt());
            assertEquals(0, h.reliable().sendCount());
            assertTrue(h.channel.isOpen());

            h.channel.writeAndFlush(largest);
            assertEquals(256, h.reliable().sendCount());
            assertEquals(255, h.reliable().sentBytes().get(0)[0] & 0xFF);
        }
    }

    @Test
    void anEmptyWriteSendsNothingAndSucceeds() {
        try (ActiveChild h = new ActiveChild()) {
            ChannelFuture write = h.channel.writeAndFlush(Unpooled.EMPTY_BUFFER);
            h.run();
            assertTrue(write.isSuccess());
            assertEquals(0, h.reliable().sendCount());
            assertEquals(0, h.unreliable().sendCount());
        }
    }

    @Test
    void anUnreliablePacketGoesOnTheUnreliableChannelAsOneSegment() {
        try (ActiveChild h = new ActiveChild()) {
            ChannelFuture write = h.channel.writeAndFlush(new NetherNetPacket(payload(7, 8), false));
            ChannelFuture reliable = h.channel.writeAndFlush(new NetherNetPacket(payload(9), true));
            assertArrayEquals(new byte[]{0, 7, 8}, h.unreliable().sentBytes().getFirst());
            assertArrayEquals(new byte[]{0, 9}, h.reliable().sentBytes().getFirst());
            h.unreliable().accept(0);
            h.reliable().accept(0);
            h.run();
            assertTrue(write.isSuccess());
            assertTrue(reliable.isSuccess());
        }
    }

    @Test
    void anUnreliablePacketLargerThanOneSegmentIsRefused() {
        try (ActiveChild h = new ActiveChild(child -> child.setMaxOutboundMessageSize(4))) {
            NetherNetPacket packet = new NetherNetPacket(payload(1, 2, 3, 4), false);
            ChannelFuture write = h.channel.writeAndFlush(packet);
            assertInstanceOf(IllegalArgumentException.class, write.cause());
            assertEquals(0, packet.refCnt());
            assertEquals(0, h.unreliable().sendCount());
        }
    }
}

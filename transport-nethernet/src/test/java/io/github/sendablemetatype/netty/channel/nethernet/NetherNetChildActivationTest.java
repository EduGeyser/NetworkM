package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A child goes active once the reliable data channel is open and the unreliable one opened or
 * closed. The application here writes on the unreliable channel as soon as the child is active,
 * which the fake engine refuses while that channel still connects, as libwebrtc does.
 */
class NetherNetChildActivationTest {
    private final EmbeddedChannel clock = new EmbeddedChannel();
    private final ChildFixture fixture = new ChildFixture(null);
    private final List<String> events = new ArrayList<>();

    @BeforeEach
    void register() {
        fixture.child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                events.add("active");
                ctx.writeAndFlush(new NetherNetPacket(Unpooled.wrappedBuffer(new byte[]{7}), false));
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                events.add("inactive");
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                events.add(String.valueOf(event));
            }
        });
        clock.eventLoop().register(fixture.child).syncUninterruptibly();
        clock.runPendingTasks();
        fixture.reliable = fixture.peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        fixture.peer.open(fixture.reliable);
    }

    @AfterEach
    void close() {
        fixture.child.close().syncUninterruptibly();
        clock.runPendingTasks();
        clock.finishAndReleaseAll();
    }

    /**
     * libwebrtc announces a remote channel before it opens it, so a client whose unreliable channel
     * is announced after the reliable one opened gives the child a channel that still connects.
     */
    @Test
    void anUnreliableChannelAnnouncedLastHoldsActivationUntilItOpens() {
        fixture.unreliable = fixture.peer.remoteChannel(
                ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        clock.runPendingTasks();
        assertEquals(List.of(), events);

        fixture.peer.open(fixture.unreliable);
        clock.runPendingTasks();
        assertEquals(List.of("active"), events);
        assertEquals(1, fixture.unreliable.sendCount());
    }

    /** The connection carries on without the unreliable channel, also when it closes before it opened. */
    @Test
    void anUnreliableChannelClosedBeforeItOpenedDoesNotHoldUpActivation() {
        fixture.unreliable = fixture.peer.remoteChannel(
                ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        fixture.peer.linkClosed(fixture.unreliable, CloseCause.REMOTE_CLOSED);
        clock.runPendingTasks();
        // The write on the closed channel ends the connection with the close's reason.
        assertEquals(List.of("active", "CLOSED_BY_REMOTE_PEER", "inactive"), events);
    }
}

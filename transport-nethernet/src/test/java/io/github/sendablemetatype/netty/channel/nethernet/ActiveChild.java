package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * An active child on a fake peer, registered on an embedded event loop whose
 * clock stands still: tasks run only when the test calls {@link #run()}.
 */
final class ActiveChild implements AutoCloseable {
    final EmbeddedChannel clock = new EmbeddedChannel();
    final ChildFixture fixture = new ChildFixture(null);
    final NetherNetChildChannel channel = fixture.child;
    final FakePeer peer = fixture.peer;
    /** Pipeline events in order: "active", "inactive", "read:..." and user events by name. */
    final List<String> events = new ArrayList<>();

    ActiveChild() {
        this(child -> { });
    }

    /** @param configure runs before the child registers and activates */
    ActiveChild(Consumer<NetherNetChildChannel> configure) {
        clock.freezeTime();
        configure.accept(channel);
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                events.add("active");
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                events.add("inactive");
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                events.add(String.valueOf(event));
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                try {
                    if (message instanceof NetherNetPacket packet) {
                        events.add("unreliable:" + hex(packet.content()));
                    } else {
                        events.add("read:" + hex((ByteBuf) message));
                    }
                } finally {
                    ReferenceCountUtil.release(message);
                }
            }
        });
        fixture.openLinks();
        clock.eventLoop().register(channel).syncUninterruptibly();
        run();
    }

    FakeDataLink reliable() {
        return fixture.reliable;
    }

    FakeDataLink unreliable() {
        return fixture.unreliable;
    }

    void run() {
        clock.runPendingTasks();
    }

    @Override
    public void close() {
        if (channel.isOpen()) {
            channel.close().syncUninterruptibly();
        }
        run();
        clock.finishAndReleaseAll();
    }

    static ByteBuf payload(int... bytes) {
        ByteBuf buffer = Unpooled.buffer(bytes.length);
        for (int b : bytes) {
            buffer.writeByte(b);
        }
        return buffer;
    }

    private static String hex(ByteBuf buffer) {
        StringBuilder text = new StringBuilder();
        for (int i = buffer.readerIndex(); i < buffer.writerIndex(); i++) {
            text.append(buffer.getUnsignedByte(i)).append(i + 1 < buffer.writerIndex() ? "," : "");
        }
        return text.toString();
    }
}

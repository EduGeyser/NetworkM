package org.cloudburstmc.netty.handler.codec.raknet.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.config.DefaultRakClientConfig;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.util.RakUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.cloudburstmc.netty.channel.raknet.RakConstants.*;
import static org.junit.jupiter.api.Assertions.*;

class RakCompatibilityHandlerTests {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void replyTwoSecurityIsIgnoredOnlyInCompatibilityMode(boolean compatible) {
        try (Handshake h = new Handshake(compatible)) {
            ByteBuf reply = prefix(ID_OPEN_CONNECTION_REPLY_2);
            reply.writeLong(123);
            RakUtils.writeAddress(reply, new InetSocketAddress("127.0.0.1", 19132));
            reply.writeShort(1400).writeBoolean(true);
            h.transport.writeInbound(reply);
            assertEquals(compatible ? 1 : 0, h.handler.installed);
            if (!compatible) {
                assertInstanceOf(SecurityException.class, h.promise.cause());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void repeatedReplyOneCanClearTheCookieSecurityState(boolean compatible) {
        try (Handshake h = new Handshake(compatible)) {
            h.transport.writeInbound(prefix(ID_OPEN_CONNECTION_REPLY_1).writeLong(123)
                    .writeBoolean(true).writeInt(456).writeShort(1400));
            ByteBuf secure = h.requestTwo();
            try {
                assertEquals(39, secure.readableBytes());
                assertEquals(456, secure.getInt(17));
            } finally {
                secure.release();
            }
            h.transport.writeInbound(prefix(ID_OPEN_CONNECTION_REPLY_1).writeLong(123)
                    .writeBoolean(false).writeShort(1400));
            ByteBuf plain = h.requestTwo();
            try {
                assertEquals(34, plain.readableBytes());
                assertEquals(4, plain.getUnsignedByte(17));
            } finally {
                plain.release();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void transientDenialsRetainTheRetryLimitInBothModes(boolean compatible) {
        try (Handshake h = new Handshake(compatible)) {
            h.config.setOption(RakChannelOption.RAK_MAX_CONNECTION_ATTEMPTS, 2);
            h.transport.writeInbound(prefix(ID_NO_FREE_INCOMING_CONNECTIONS));
            assertFalse(h.promise.isDone());
            h.transport.writeInbound(prefix(ID_NO_FREE_INCOMING_CONNECTIONS));
            assertTrue(h.promise.isDone());
            assertEquals("No free incoming connections", h.promise.cause().getMessage());
        }
    }

    @Test
    void unknownAddressTypeUsesTheVanillaIpv6Width() {
        ByteBuf address = Unpooled.buffer().writeByte(99).writeZero(28).writeByte(42);
        try {
            assertTrue(RakUtils.skipAddress(address));
            assertEquals(42, address.readUnsignedByte());
        } finally {
            address.release();
        }
        ByteBuf truncated = Unpooled.buffer().writeByte(99).writeZero(27);
        try {
            assertFalse(RakUtils.skipAddress(truncated));
        } finally {
            truncated.release();
        }
    }

    private static ByteBuf prefix(int id) {
        return Unpooled.buffer().writeByte(id).writeBytes(DEFAULT_UNCONNECTED_MAGIC);
    }

    private static class RecordingHandler extends RakClientOfflineHandler {
        int installed;

        RecordingHandler(RakChannel channel, ChannelPromise promise) {
            super(channel, promise);
        }

        @Override
        void onSuccess(ChannelHandlerContext ctx) {
            installed++;
        }
    }

    private static class Handshake implements AutoCloseable {
        final EmbeddedChannel transport = new EmbeddedChannel() {
            @Override protected SocketAddress localAddress0() { return new InetSocketAddress("127.0.0.1", 19133); }
            @Override protected SocketAddress remoteAddress0() { return new InetSocketAddress("127.0.0.1", 19132); }
        };
        final ChannelPromise promise = transport.newPromise();
        DefaultRakClientConfig config;
        final RecordingHandler handler;

        Handshake(boolean compatible) {
            transport.freezeTime();
            RakChannel channel = (RakChannel) Proxy.newProxyInstance(RakChannel.class.getClassLoader(), new Class<?>[]{RakChannel.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "config": return config;
                            case "rakPipeline": case "pipeline": return transport.pipeline();
                            case "parent": return transport;
                            default: return method.invoke(transport, args);
                        }
                    });
            config = new DefaultRakClientConfig(channel);
            config.setCompatibilityMode(compatible);
            handler = new RecordingHandler(channel, promise);
            transport.pipeline().addLast(RakClientOfflineHandler.NAME, handler);
            transport.runPendingTasks();
            transport.releaseOutbound();
        }

        ByteBuf requestTwo() {
            Object message;
            while ((message = transport.readOutbound()) != null) {
                if (message instanceof ByteBuf && ((ByteBuf) message).getUnsignedByte(0) == ID_OPEN_CONNECTION_REQUEST_2) {
                    return (ByteBuf) message;
                }
                ReferenceCountUtil.release(message);
            }
            fail("No second connection request was emitted");
            return null;
        }

        @Override public void close() { transport.finishAndReleaseAll(); }
    }
}

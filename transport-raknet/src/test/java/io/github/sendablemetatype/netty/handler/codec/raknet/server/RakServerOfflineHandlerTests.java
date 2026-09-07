package io.github.sendablemetatype.netty.handler.codec.raknet.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.github.sendablemetatype.netty.channel.raknet.RakChannelFactory;
import io.github.sendablemetatype.netty.channel.raknet.RakServerChannel;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelOption;
import io.github.sendablemetatype.netty.channel.raknet.config.RakServerCookieMode;
import io.github.sendablemetatype.netty.util.RakUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class RakServerOfflineHandlerTests {
    private static final InetSocketAddress CLIENT = new InetSocketAddress("127.0.0.1", 19134);

    @ParameterizedTest
    @EnumSource(RakServerCookieMode.class)
    void truncatedSecondRequestsNeverCreateAChildOrRaiseAPipelineException(RakServerCookieMode mode) {
        for (boolean ipv6 : new boolean[]{false, true}) {
            try (Server server = new Server(mode)) {
                ByteBuf request = requestTwo(server, ipv6);
                try {
                    server.receiveEveryTruncation(request);
                    assertNull(server.channel.getChildChannel(CLIENT));

                    server.receive(request.copy());
                    server.assertResponse(ID_OPEN_CONNECTION_REPLY_2);
                    assertNotNull(server.channel.getChildChannel(CLIENT));
                } finally {
                    request.release();
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(RakServerCookieMode.class)
    void unknownAddressFamiliesAreDropped(RakServerCookieMode mode) {
        try (Server server = new Server(mode)) {
            ByteBuf request = requestTwo(server, true);
            int addressIndex = 1 + DEFAULT_UNCONNECTED_MAGIC.length + (mode == RakServerCookieMode.INVALID ? 0 : 5);
            request.setByte(addressIndex, 5);

            server.receive(request);

            assertNull(server.responses.poll());
            assertNull(server.channel.getChildChannel(CLIENT));
        }
    }

    @Test
    void truncatedFirstRequestsAreDroppedAndTheCompleteRequestStillWorks() {
        try (Server server = new Server(RakServerCookieMode.ACTIVE)) {
            ByteBuf request = requestOne(11);
            try {
                server.receiveEveryTruncation(request);
                server.receive(request.copy());
                server.assertResponse(ID_OPEN_CONNECTION_REPLY_1);
            } finally {
                request.release();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = RakServerCookieMode.class, names = {"OFFLOADED", "OFFLOADED_PSK"})
    void offloadedModesLeaveFirstRequestsToTheOffloader(RakServerCookieMode mode) {
        try (Server server = new Server(mode)) {
            server.channel.config().setSupportedProtocols(new int[]{11});

            server.receive(requestOne(11));
            server.receive(requestOne(255));

            assertNull(server.responses.poll());
            assertNull(server.channel.getChildChannel(CLIENT));
        }
    }

    @ParameterizedTest
    @EnumSource(value = RakServerCookieMode.class, names = {"ACTIVE", "OFF", "INVALID"})
    void otherModesStillReplyToFirstRequests(RakServerCookieMode mode) {
        try (Server server = new Server(mode)) {
            server.receive(requestOne(11));
            server.assertResponse(ID_OPEN_CONNECTION_REPLY_1);
        }
    }

    @Test
    void truncatedPingsAreDroppedAndACompletePingStillWorks() {
        try (Server server = new Server(RakServerCookieMode.ACTIVE)) {
            ByteBuf request = Unpooled.buffer().writeByte(ID_UNCONNECTED_PING).writeLong(12345)
                    .writeBytes(DEFAULT_UNCONNECTED_MAGIC);
            try {
                server.receiveEveryTruncation(request);
                server.receive(request.copy());
                server.assertResponse(ID_UNCONNECTED_PONG);
            } finally {
                request.release();
            }
        }
    }

    private static ByteBuf requestOne(int protocol) {
        return Unpooled.buffer().writeByte(ID_OPEN_CONNECTION_REQUEST_1)
                .writeBytes(DEFAULT_UNCONNECTED_MAGIC).writeByte(protocol);
    }

    private static ByteBuf requestTwo(Server server, boolean ipv6) {
        ByteBuf request = Unpooled.buffer().writeByte(ID_OPEN_CONNECTION_REQUEST_2)
                .writeBytes(DEFAULT_UNCONNECTED_MAGIC);
        if (server.channel.config().getCookieMode() != RakServerCookieMode.INVALID) {
            request.writeInt(server.channel.config().getSipHash().generateStatelessCookie(CLIENT, 11));
            request.writeBoolean(false);
        }
        RakUtils.writeAddress(request, new InetSocketAddress(ipv6 ? "::1" : "127.0.0.1", 19132));
        return request.writeShort(1400).writeLong(12345);
    }

    private static final class Server implements AutoCloseable {
        private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        private final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        private final Queue<DatagramPacket> responses = new ConcurrentLinkedQueue<>();
        private final RakServerChannel channel;

        private Server(RakServerCookieMode mode) {
            channel = (RakServerChannel) new ServerBootstrap()
                    .group(group)
                    .channelFactory(RakChannelFactory.server(NioDatagramChannel.class))
                    .option(RakChannelOption.RAK_SERVER_COOKIE_MODE, mode)
                    .option(RakChannelOption.RAK_PACKET_LIMIT, 0)
                    .handler(new ChannelInitializer<RakServerChannel>() {
                        @Override
                        protected void initChannel(RakServerChannel channel) {
                            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    exceptions.add(cause);
                                }
                            });
                        }
                    })
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel channel) {
                        }
                    })
                    .bind(new InetSocketAddress("127.0.0.1", 0)).syncUninterruptibly().channel();
            channel.eventLoop().submit(() -> channel.pipeline().addBefore(RakServerOfflineHandler.NAME, "capture-replies", new ChannelOutboundHandlerAdapter() {
                @Override
                public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                    if (message instanceof DatagramPacket datagram) {
                        responses.add(datagram);
                        promise.setSuccess();
                    } else {
                        ctx.write(message, promise);
                    }
                }
            })).syncUninterruptibly();
        }

        private void receive(ByteBuf buffer) {
            channel.eventLoop().submit(() -> channel.pipeline().fireChannelRead(new DatagramPacket(buffer, channel.localAddress(), CLIENT)))
                    .syncUninterruptibly();
            assertEquals(0, buffer.refCnt(), "Inbound datagrams must be released");
            assertTrue(exceptions.isEmpty(), () -> "Unexpected pipeline exception: " + exceptions.peek());
        }

        private void receiveEveryTruncation(ByteBuf request) {
            for (int length = 0; length < request.readableBytes(); length++) {
                ByteBuf truncated = Unpooled.buffer().writeZero(3).writeBytes(request, request.readerIndex(), length);
                truncated.readerIndex(3);
                receive(truncated);
                assertTrue(responses.isEmpty(), "A request truncated to " + length + " bytes must not receive a reply");
            }
        }

        private void assertResponse(int id) {
            DatagramPacket response = responses.poll();
            assertNotNull(response);
            try {
                assertEquals(id, response.content().getUnsignedByte(response.content().readerIndex()));
            } finally {
                response.release();
            }
        }

        @Override
        public void close() {
            channel.close().awaitUninterruptibly();
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
            DatagramPacket response;
            while ((response = responses.poll()) != null) {
                response.release();
            }
        }
    }
}

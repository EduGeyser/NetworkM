/*
 * Copyright 2026 CloudburstMC
 * Licensed under the Apache License, Version 2.0.
 */

package org.cloudburstmc.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.RakChannelFactory;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.cloudburstmc.netty.channel.raknet.packet.RakDatagramPacket;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import org.cloudburstmc.netty.handler.codec.raknet.common.RakSessionCodec;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RakSessionIntegrationTests {
    static Stream<Arguments> transports() {
        return Stream.of(Arguments.of(false, 100), Arguments.of(true, 100),
                Arguments.of(false, 32768), Arguments.of(true, 32768));
    }

    @ParameterizedTest
    @MethodSource("transports")
    @Timeout(15)
    void splitDataRoundTripsAcrossEventLoopsAfterChangingAutoFlush(boolean compatible, int size) throws Exception {
        MultiThreadIoEventLoopGroup transport = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        MultiThreadIoEventLoopGroup application = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel server = null;
        Channel client = null;
        CompletableFuture<byte[]> response = new CompletableFuture<>();
        CompletableFuture<Void> captureReady = new CompletableFuture<>();
        AtomicBoolean handshakeBatchSeen = new AtomicBoolean();
        byte[] expected = new byte[size];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
        }
        expected[0] = (byte) 0xfe;
        try {
            server = new ServerBootstrap()
                    .channelFactory(RakChannelFactory.server(NioDatagramChannel.class))
                    .group(transport, application)
                    .childOption(RakChannelOption.RAK_AUTO_FLUSH, false)
                    .childOption(RakChannelOption.RAK_SESSION_TIMEOUT, 5000L)
                    .childHandler(new ChannelInitializer<RakChannel>() {
                        @Override
                        protected void initChannel(RakChannel channel) {
                            channel.rakPipeline().channel().eventLoop().execute(() -> {
                                channel.rakPipeline().addBefore(RakSessionCodec.NAME, "handshake-capture", new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object message) {
                                        if (message instanceof RakDatagramPacket) {
                                            Set<Integer> ids = new HashSet<>();
                                            for (EncapsulatedPacket packet : ((RakDatagramPacket) message).getPackets()) {
                                                ids.add((int) packet.getBuffer().getUnsignedByte(packet.getBuffer().readerIndex()));
                                            }
                                            if (ids.contains(0x13) && ids.contains(0) && ids.contains(0xfe)) {
                                                handshakeBatchSeen.set(true);
                                            }
                                        }
                                        ctx.fireChannelRead(message);
                                    }
                                });
                                captureReady.complete(null);
                            });
                            channel.pipeline().addLast(new SimpleChannelInboundHandler<RakMessage>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, RakMessage message) {
                                    // This runs on the child application loop, distinct from its transport loop.
                                    channel.config().setFlushInterval(1);
                                    channel.config().setAutoFlush(true);
                                    ctx.writeAndFlush(new RakMessage(message.content().retain(), RakReliability.RELIABLE_ORDERED));
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    response.completeExceptionally(cause);
                                }
                            });
                        }
                    }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();

            client = new Bootstrap()
                    .channelFactory(RakChannelFactory.client(NioDatagramChannel.class))
                    .group(application)
                    .option(RakChannelOption.RAK_PROTOCOL_VERSION, 11)
                    .option(RakChannelOption.RAK_COMPATIBILITY_MODE, compatible)
                    .option(RakChannelOption.RAK_AUTO_FLUSH, false)
                    .handler(new ChannelInitializer<RakChannel>() {
                        @Override
                        protected void initChannel(RakChannel channel) {
                            channel.pipeline().addLast(new SimpleChannelInboundHandler<RakMessage>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, RakMessage message) {
                                    response.complete(ByteBufUtil.getBytes(message.content()));
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    response.completeExceptionally(cause);
                                }
                            });
                        }
                    }).connect(server.localAddress()).sync().channel();
            captureReady.get(5, TimeUnit.SECONDS);
            client.writeAndFlush(new RakMessage(Unpooled.wrappedBuffer(expected), RakReliability.RELIABLE_ORDERED)).sync();
            assertArrayEquals(expected, response.get(5, TimeUnit.SECONDS));
            assertTrue(client.isActive());
            if (compatible && size == 100) {
                assertTrue(handshakeBatchSeen.get(), "Final handshake, ping, and first game packet must share a datagram");
            }
        } finally {
            if (client != null) {
                client.close().syncUninterruptibly();
            }
            if (server != null) {
                server.close().syncUninterruptibly();
            }
            application.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
            transport.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}

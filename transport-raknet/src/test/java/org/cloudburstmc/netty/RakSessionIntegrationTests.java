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
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.cloudburstmc.netty.channel.raknet.RakChannel;
import org.cloudburstmc.netty.channel.raknet.RakChannelFactory;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RakSessionIntegrationTests {
    @Test
    @Timeout(15)
    void splitDataRoundTripsAcrossEventLoopsAfterChangingAutoFlush() throws Exception {
        NioEventLoopGroup transport = new NioEventLoopGroup(1);
        NioEventLoopGroup application = new NioEventLoopGroup(1);
        Channel server = null;
        Channel client = null;
        CompletableFuture<byte[]> response = new CompletableFuture<>();
        byte[] expected = new byte[32768];
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
            client.writeAndFlush(new RakMessage(Unpooled.wrappedBuffer(expected), RakReliability.RELIABLE_ORDERED)).sync();
            assertArrayEquals(expected, response.get(5, TimeUnit.SECONDS));
            assertTrue(client.isActive());
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

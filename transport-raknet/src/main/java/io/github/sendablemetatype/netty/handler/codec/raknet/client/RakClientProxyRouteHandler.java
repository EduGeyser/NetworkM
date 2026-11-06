/*
 * Copyright 2022 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.handler.codec.raknet.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.github.sendablemetatype.netty.channel.raknet.RakClientChannel;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelMetrics;
import io.github.sendablemetatype.netty.util.MetricsFailureLog;

public class RakClientProxyRouteHandler extends ChannelDuplexHandler {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakClientProxyRouteHandler.class);

    public static final String NAME = "rak-client-proxy-route-handler";

    private final RakClientChannel channel;

    public RakClientProxyRouteHandler(RakClientChannel channel) {
        this.channel = channel;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof DatagramPacket)) {
            ctx.fireChannelRead(msg);
            return;
        }

        DatagramPacket packet = (DatagramPacket) msg;
        RakChannelMetrics metrics = this.channel.config().getMetrics();
        if (metrics != null) {
            try {
                metrics.bytesIn(packet.content().readableBytes());
            } catch (RuntimeException cause) {
                MetricsFailureLog.failed(log, "RakChannelMetrics.bytesIn", this.channel.remoteAddress(), cause);
            }
        }

        DatagramPacket datagram = packet.retain();
        try {
            if (packet.sender() == null || packet.sender().equals(this.channel.remoteAddress())) {
                ctx.fireChannelRead(datagram.content());
            } else {
                ctx.fireChannelRead(datagram);
            }
        } finally {
            datagram.release();
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        boolean isDatagram = msg instanceof DatagramPacket;
        if (!isDatagram && !(msg instanceof ByteBuf)) {
            ctx.write(msg, promise);
            return;
        }

        DatagramPacket datagram = isDatagram ? (DatagramPacket) msg : new DatagramPacket((ByteBuf) msg, this.channel.remoteAddress());
        RakChannelMetrics metrics = this.channel.config().getMetrics();
        if (metrics != null) {
            try {
                metrics.bytesOut(datagram.content().readableBytes());
            } catch (RuntimeException cause) {
                MetricsFailureLog.failed(log, "RakChannelMetrics.bytesOut", this.channel.remoteAddress(), cause);
            }
        }

        ctx.write(datagram, promise);
    }
}

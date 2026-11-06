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

package io.github.sendablemetatype.netty.handler.codec.raknet.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.github.sendablemetatype.netty.channel.raknet.RakChildChannel;
import io.github.sendablemetatype.netty.channel.raknet.RakServerChannel;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelMetrics;
import io.github.sendablemetatype.netty.util.MetricsFailureLog;

public class RakServerRouteHandler extends ChannelDuplexHandler {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakServerRouteHandler.class);


    public static final String NAME = "rak-server-route-handler";
    private final RakServerChannel parent;

    public RakServerRouteHandler(RakServerChannel parent) {
        this.parent = parent;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof DatagramPacket)) {
            ctx.fireChannelRead(msg);
            return;
        }
        DatagramPacket packet = (DatagramPacket) msg;

        try {
            RakChildChannel channel = this.parent.getChildChannel(packet.sender());
            if (channel == null) {
                // Pass DatagramPacket which holds remote address and payload.
                ctx.fireChannelRead(packet.retain());
                return;
            }

            RakChannelMetrics metrics = channel.config().getMetrics();
            if (metrics != null) {
                try {
                    metrics.bytesIn(packet.content().readableBytes());
                } catch (RuntimeException cause) {
                    MetricsFailureLog.failed(log, "RakChannelMetrics.bytesIn", packet.sender(), cause);
                }
            }

            // In this case remote address is already known from ChannelHandlerContext
            // so we can pass only payload.
            ByteBuf buffer = packet.content().retain();
            channel.rakPipeline().fireChannelRead(buffer).fireChannelReadComplete();
        } finally {
            packet.release();
        }
    }
}

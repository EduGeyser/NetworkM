/*
 * Copyright 2026 CloudburstMC
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

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ProtocolDetectionResult;
import io.netty.handler.codec.ProtocolDetectionState;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.github.sendablemetatype.netty.util.address.EndpointAddress;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Reads a PROXY protocol header, v1 or v2, from a peer trusted to send one, and steps aside for
 * anything else: a trusted proxy may also speak plain HTTP or TLS with no header. Added only for
 * trusted peers, so nobody else can declare a source. Sits before TLS detection, since a proxy
 * writes the header before the handshake.
 */
final class HttpProxyHeaderReader extends ByteToMessageDecoder {
    private final HttpConnection connection;
    private final NetherNetHttpSignaling signaling;

    HttpProxyHeaderReader(HttpConnection connection, NetherNetHttpSignaling signaling) {
        this.connection = connection;
        this.signaling = signaling;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ProtocolDetectionResult<HAProxyProtocolVersion> detected = HAProxyMessageDecoder.detectProtocol(in);
        if (detected.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
            return;
        }
        if (detected.state() == ProtocolDetectionState.INVALID) {
            ctx.pipeline().remove(this);
            return;
        }
        ctx.pipeline().addAfter(ctx.name(), null, new SimpleChannelInboundHandler<HAProxyMessage>() {
            @Override
            protected void channelRead0(ChannelHandlerContext inner, HAProxyMessage message) {
                inner.pipeline().remove(this);
                // A LOCAL command (a proxy's own health check), an UNKNOWN or a UNIX family declares nobody
                HAProxyProxiedProtocol.AddressFamily family = message.proxiedProtocol().addressFamily();
                if (message.sourceAddress() == null || (family != HAProxyProxiedProtocol.AddressFamily.AF_IPv4
                        && family != HAProxyProxiedProtocol.AddressFamily.AF_IPv6)) {
                    return;
                }
                InetSocketAddress declared;
                try {
                    // The decoder checked the literal; parsing it here still never looks a name up
                    declared = new InetSocketAddress(EndpointAddress.parse(message.sourceAddress()),
                            message.sourcePort());
                } catch (UnknownHostException malformed) {
                    inner.close();
                    return;
                }
                connection.proxyDeclared = declared;
                signaling.countProxiedClient(connection, declared);
            }
        });
        ctx.pipeline().replace(this, null, new HAProxyMessageDecoder());
    }
}

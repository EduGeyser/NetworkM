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

package io.github.sendablemetatype.netty.handler.codec.raknet.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageCodec;
import io.github.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;
import io.github.sendablemetatype.netty.channel.raknet.packet.RakDatagramPacket;

import java.util.List;

import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.*;

public class RakDatagramCodec extends MessageToMessageCodec<ByteBuf, RakDatagramPacket> {
    public static final String NAME = "rak-datagram-codec";

    public RakDatagramCodec() {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RakDatagramPacket packet, List<Object> out) throws Exception {
        if (packet.getPackets().isEmpty()) {
            throw new IllegalArgumentException("A RakNet datagram must contain an encapsulated packet");
        }

        // Use a composite buffer so we don't have to do any memory copying.
        CompositeByteBuf buf = ctx.alloc().compositeBuffer((packet.getPackets().size() * 2) + 1);
        boolean transferred = false;
        try {
            ByteBuf header = ctx.alloc().ioBuffer(4);
            header.writeByte(packet.getFlags());
            header.writeMediumLE(packet.getSequenceIndex());
            buf.addComponent(true, header);

            for (EncapsulatedPacket encapsulated : packet.getPackets()) {
                encapsulated.encode(buf);
            }
            out.add(buf);
            transferred = true;
        } finally {
            if (!transferred) {
                buf.release();
            }
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list) throws Exception {
        if (!buffer.isReadable()) {
            return;
        }
        byte potentialFlags = buffer.getByte(buffer.readerIndex());
        if ((potentialFlags & FLAG_VALID) == 0) {
            // Not a RakNet datagram
            list.add(buffer.retain());
            return;
        }

        if ((potentialFlags & FLAG_ACK) != 0 || (potentialFlags & FLAG_NACK) != 0) {
            // Do not handle Acknowledge packets here
            list.add(buffer.retain());
            return;
        }

        if (buffer.readableBytes() < 4) {
            throw new CorruptedFrameException("Truncated RakNet datagram header");
        }

        RakDatagramPacket packet = RakDatagramPacket.newInstance();
        try {
            packet.setFlags(buffer.readByte());
            packet.setSequenceIndex(buffer.readUnsignedMediumLE());
            if (!buffer.isReadable()) {
                throw new CorruptedFrameException("A RakNet datagram must contain an encapsulated packet");
            }
            while (buffer.isReadable()) {
                EncapsulatedPacket encapsulated = EncapsulatedPacket.newInstance();
                try {
                    encapsulated.decode(buffer);
                    if (!encapsulated.getBuffer().isReadable()) {
                        throw new CorruptedFrameException("An encapsulated packet must contain a payload");
                    }
                    packet.getPackets().add(encapsulated.retain());
                } finally {
                    encapsulated.release();
                }
            }
            list.add(packet.retain());
        } catch (IndexOutOfBoundsException cause) {
            throw new CorruptedFrameException("Truncated encapsulated packet", cause);
        } finally {
            packet.release();
        }
    }
}

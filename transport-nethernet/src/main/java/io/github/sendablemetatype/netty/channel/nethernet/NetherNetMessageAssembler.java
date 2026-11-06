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

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;

/**
 * Joins the segments of the reliable data channel into whole messages, copying
 * each engine buffer once into Netty ownership and joining the copies without
 * copying them again.
 *
 * <p>Every segment starts with a countdown header: how many segments of the
 * message follow it. Vanilla closes the session for a segment of one byte or
 * less (a header with no payload) and for a countdown that is not one less
 * than the previous segment's, and nothing else about a message makes it close
 * (decompiled client 1.26.52 and 1.26.60, BDS 1.26.52.3 and 1.26.60.28; see
 * {@code docs/client-behavior.md}). This assembler follows that rule, and adds
 * a cap on the reassembled size, since vanilla has none. After a break nothing
 * more is delivered: the reliable channel promises every message, so skipping
 * one would leave the session silently out of step.
 *
 * <p>Not used for the unreliable channel, which carries only unsplit messages.
 */
final class NetherNetMessageAssembler {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetMessageAssembler.class);

    private CompositeByteBuf assembly;
    private int expected = -1;
    private boolean closed;

    /**
     * Decodes one segment. The returned message belongs to the caller and may
     * outlive the engine callback.
     *
     * @param segment the segment, valid only during the call
     * @param allocator where the copies come from
     * @param maxMessageBytes the largest reassembled message allowed
     * @param metrics counts split segments and failures, or null
     * @return the completed message, or null while a split message is still
     *         arriving and after the assembler closed
     * @throws NetherNetFramingException for framing the stream cannot continue past
     */
    synchronized ByteBuf decode(ByteBuffer segment, ByteBufAllocator allocator, int maxMessageBytes,
                                NetherChannelMetrics metrics) {
        if (closed) {
            return null;
        }
        if (segment.remaining() < 2) {
            throw broken(metrics, TransportDisconnectReason.BAD_PACKET,
                    "A segment of " + segment.remaining() + " bytes");
        }
        int countdown = segment.get() & 0xFF;
        if (expected != -1 && countdown != expected) {
            throw broken(metrics, TransportDisconnectReason.BAD_PACKET,
                    "Expected segment " + expected + ", got " + countdown);
        }
        int assembled = assembly == null ? 0 : assembly.readableBytes();
        if (segment.remaining() > maxMessageBytes - assembled) {
            throw broken(metrics, TransportDisconnectReason.SPLIT_QUEUE_TOO_LONG,
                    "A message over " + maxMessageBytes + " bytes");
        }

        try {
            if (expected == -1 && countdown == 0) {
                return copy(segment, allocator);
            }
            if (assembly == null) {
                // Room for every remaining segment, so the composite never consolidates.
                assembly = allocator.compositeBuffer(countdown + 1);
            }
            // addComponent takes over the copy's reference.
            assembly.addComponent(true, copy(segment, allocator));
            if (countdown != 0) {
                expected = countdown - 1;
                if (metrics != null) {
                    // A failing metric must not reach the catch below, which drops the partial message.
                    try {
                        metrics.fragmentsIn(1);
                    } catch (RuntimeException e) {
                        log.debug("NetherNet metrics failed", e);
                    }
                }
                return null;
            }
            ByteBuf message = assembly;
            assembly = null;
            expected = -1;
            return message;
        } catch (RuntimeException | Error e) {
            clear();
            throw e;
        }
    }

    /** Releases a partial message; nothing is decoded after. */
    synchronized void close() {
        closed = true;
        clear();
    }

    private NetherNetFramingException broken(NetherChannelMetrics metrics, TransportDisconnectReason reason,
                                             String what) {
        close();
        if (metrics != null) {
            // A failing metric must not replace the framing exception the channel closes on.
            try {
                metrics.decodeFail(1);
            } catch (RuntimeException e) {
                log.debug("NetherNet metrics failed", e);
            }
        }
        return new NetherNetFramingException(reason, what + " on the reliable channel");
    }

    private static ByteBuf copy(ByteBuffer data, ByteBufAllocator allocator) {
        ByteBuf payload = allocator.buffer(data.remaining(), data.remaining());
        try {
            payload.writeBytes(data);
            return payload;
        } catch (RuntimeException | Error e) {
            payload.release();
            throw e;
        }
    }

    private void clear() {
        if (assembly != null) {
            assembly.release();
            assembly = null;
        }
        expected = -1;
    }
}

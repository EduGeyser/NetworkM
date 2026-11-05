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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

/**
 * A whole message on the data channel it names.
 *
 * <p>Writing: a plain {@link ByteBuf} goes on the reliable channel; a packet
 * goes on the channel it names. An unreliable message must fit one segment,
 * as Bedrock never splits those.
 *
 * <p>Reading: a message from the reliable channel arrives as a plain
 * {@link ByteBuf}, one from the unreliable channel as a packet with
 * {@code reliable} false.
 */
public final class NetherNetPacket extends DefaultByteBufHolder {
    private final boolean reliable;

    /**
     * @param content the message; the packet takes ownership of its reference
     * @param reliable whether it goes on the reliable channel
     */
    public NetherNetPacket(ByteBuf content, boolean reliable) {
        super(content);
        this.reliable = reliable;
    }

    /** Whether the message goes, or came, on the reliable channel. */
    public boolean reliable() {
        return reliable;
    }

    @Override
    public NetherNetPacket replace(ByteBuf content) {
        return new NetherNetPacket(content, reliable);
    }
}

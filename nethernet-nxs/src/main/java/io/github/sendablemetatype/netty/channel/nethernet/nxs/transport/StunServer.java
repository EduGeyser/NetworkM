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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

/** A STUN server from the provider's discovery document, as a name or a numeric address. The endpoint resolves it. */
public record StunServer(String host, int port) {
    public StunServer {
        if (host == null || host.isEmpty() || host.length() > 253 || !host.matches("[A-Za-z0-9.:-]+")
                || port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid provider STUN server");
        }
    }
}

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

package io.github.sendablemetatype.netty.channel.nethernet.config;

import java.net.InetSocketAddress;

/**
 * Decides whether a join from an address may go ahead, before anything is created for it. Part
 * of the server channel's admission stage for every join source; set it with
 * {@link NetherChannelOption#NETHER_SERVER_THROTTLE}. Joins whose source cannot tell where the
 * client is (Xbox RPC) are not throttled.
 *
 * @see DefaultNetherServerThrottle
 */
public interface NetherServerThrottle {

    /**
     * Whether {@link #accept} would let a join from the address go ahead now, without counting
     * anything. A source asks it before an offer arrives, to decline early; true is no promise.
     *
     * @param address the address the client signals from
     * @return false if a join from it would certainly be refused
     */
    default boolean admits(InetSocketAddress address) {
        return true;
    }

    /**
     * @param address the address the client signals from
     * @return whether the join may go ahead. An accepted join is followed by {@link #closed} with
     *         the same address once its connection is gone.
     */
    boolean accept(InetSocketAddress address);

    /**
     * @param address the address a join was accepted for
     */
    void closed(InetSocketAddress address);
}

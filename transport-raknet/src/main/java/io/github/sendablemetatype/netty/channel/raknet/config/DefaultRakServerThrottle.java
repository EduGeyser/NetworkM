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

package io.github.sendablemetatype.netty.channel.raknet.config;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultRakServerThrottle implements RakServerThrottle {
    private final Map<InetAddress, Integer> connections;
    private final int connectionsMax;

    private final ExpiringMap<InetAddress, AtomicInteger> connects;
    private final int connectsMax;

    public DefaultRakServerThrottle() {
        this(10, 4_000, 3);
    }

    public DefaultRakServerThrottle(int connectionsMax, long connectWindowInMs, int connectsMax) {
        this.connections = new ConcurrentHashMap<>();
        this.connectionsMax = connectionsMax;

        this.connects = ExpiringMap.builder()
                .expiration(connectWindowInMs, TimeUnit.MILLISECONDS)
                .expirationPolicy(ExpirationPolicy.CREATED)
                .build();
        this.connectsMax = connectsMax;
    }

    @Override
    public boolean accept(InetSocketAddress address) {
        return this.accept(address, false);
    }

    /**
     * Charges another connect attempt while retaining an existing connection slot. The caller must transfer
     * that slot to the replacement, or keep it with the original connection if replacement construction fails.
     */
    public boolean acceptReplacement(InetSocketAddress address) {
        return this.accept(address, true);
    }

    private boolean accept(InetSocketAddress address, boolean replacing) {
        boolean[] accepted = {false};
        // Child closes and accepts can arrive from different event loops.
        this.connections.compute(address.getAddress(), (ip, connections) -> {
            int count = connections == null ? 0 : connections;
            if (replacing ? count == 0 : count >= this.connectionsMax) {
                return connections;
            }

            AtomicInteger connects = this.connects.computeIfAbsent(ip, ignored -> new AtomicInteger());
            if (connects.get() >= this.connectsMax) {
                return connections;
            }

            connects.incrementAndGet();
            accepted[0] = true;
            return replacing ? count : count + 1;
        });
        return accepted[0];
    }

    @Override
    public void closed(InetSocketAddress address) {
        this.connections.computeIfPresent(address.getAddress(), (ip, count) -> count > 1 ? count - 1 : null);
    }
}

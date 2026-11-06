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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Caps the signaling connections open at once: all of them, and per client address. Anyone on
 * the internet can reach the endpoint, and each connection holds a socket and up to one request
 * body, so without caps a single peer could hold as many as the host has descriptors.
 * <p>
 * An IPv6 client counts by its /64, since one host commonly holds a whole /64. Players behind one
 * NAT share an address, which is why the per-address default is high (a classroom), unlike
 * upstream's 8.
 */
final class HttpConnectionLimits {
    private final int maxConnections;
    private final int maxPerAddress;
    private final AtomicInteger open = new AtomicInteger();
    private final Map<InetAddress, Integer> perAddress = new ConcurrentHashMap<>();

    HttpConnectionLimits(int maxConnections, int maxPerAddress) {
        this.maxConnections = maxConnections;
        this.maxPerAddress = maxPerAddress;
    }

    /**
     * Counts a new connection against the global cap.
     *
     * @return whether it fits; a connection that does not is not counted
     */
    boolean open() {
        if (open.incrementAndGet() > maxConnections) {
            open.decrementAndGet();
            return false;
        }
        return true;
    }

    /** Gives back a connection {@link #open} counted. */
    void closed() {
        open.decrementAndGet();
    }

    /**
     * Counts a connection against its client's address.
     *
     * @param client the client's address
     * @return the key it was counted under, to {@link #release} later, or null when the address
     *         already holds its share and the connection was not counted
     */
    InetAddress admit(InetAddress client) {
        InetAddress key = key(client);
        if (perAddress.merge(key, 1, Integer::sum) > maxPerAddress) {
            release(key);
            return null;
        }
        return key;
    }

    /** Gives back a connection {@link #admit} counted under this key. */
    void release(InetAddress key) {
        perAddress.computeIfPresent(key, (address, held) -> held <= 1 ? null : held - 1);
    }

    /** The address itself for IPv4, its /64 network for IPv6. */
    static InetAddress key(InetAddress client) {
        byte[] raw = client.getAddress();
        if (raw.length == 4) {
            return client;
        }
        byte[] network = new byte[16];
        System.arraycopy(raw, 0, network, 0, 8);
        try {
            return InetAddress.getByAddress(network);
        } catch (UnknownHostException impossible) {
            throw new AssertionError("sixteen bytes are always an address", impossible);
        }
    }
}

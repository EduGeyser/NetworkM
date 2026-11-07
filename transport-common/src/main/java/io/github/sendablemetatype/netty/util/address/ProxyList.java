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

package io.github.sendablemetatype.netty.util.address;

import java.util.List;
import java.util.Objects;

/**
 * One configured list of proxies as {@link TrustedProxies#parseList} read it: the ranges it
 * trusts, and the entries it could not read. An entry cannot be read when it is not an address, a
 * CIDR range or a name that resolves, or when it is a list that could not be downloaded or names
 * nothing.
 * <p>
 * The unreadable entries tell a list that names nothing apart from one that named proxies none of
 * which could be read. Where a list only grants trust, both trust nobody, which fails closed. The
 * relays in front of a NetherNet server's ICE port are different: with no relays players connect
 * directly, and relays that could not be read must not lead to that. The server therefore refuses
 * to bind on a relay list that is {@link #allUnreadable()}; to bind without relays on purpose, pass
 * {@link #NONE}.
 *
 * @param ranges     The ranges read from the list
 * @param unreadable The entries that could not be read, trimmed, from the configuration or from a
 *                   downloaded list
 */
public record ProxyList(IpRangeSet ranges, List<String> unreadable) {
    /** A list that names no proxy. */
    public static final ProxyList NONE = new ProxyList(IpRangeSet.empty(), List.of());

    public ProxyList {
        Objects.requireNonNull(ranges, "ranges");
        unreadable = List.copyOf(unreadable);
    }

    /**
     * @return Whether the list named entries and none of them could be read, so that it trusts
     *         nobody although proxies were configured
     */
    public boolean allUnreadable() {
        return this.ranges.isEmpty() && !this.unreadable.isEmpty();
    }
}

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

import io.netty.util.NetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * An immutable set of addresses written as single hosts or CIDR ranges, such as
 * {@code 10.0.0.0/8} or {@code 2001:db8::/32}. An address with no prefix matches only itself,
 * and a host name every address it resolved to when the set was parsed.
 * <p>
 * Parsing may look names up, so parse once at startup, off any I/O thread, and share the
 * result: matching never blocks.
 */
public final class IpRangeSet {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(IpRangeSet.class);

    private static final IpRangeSet EMPTY = new IpRangeSet(List.of());
    // At least one letter, so that a mistyped literal is never sent to a resolver
    private static final Pattern NAME = Pattern.compile("(?=.*[A-Za-z])[A-Za-z0-9.-]{1,253}");

    /** Looks a name up; a test seam, the default being the system resolver. */
    @FunctionalInterface
    interface Resolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    private final List<Range> ranges;

    private IpRangeSet(List<Range> ranges) {
        this.ranges = ranges;
    }

    /**
     * @return The set that matches nothing
     */
    public static IpRangeSet empty() {
        return EMPTY;
    }

    /**
     * Parses the entries, logging and skipping any that are not an address, a CIDR range or a
     * name that resolves. A list of only bad entries matches nothing. Blocks while names resolve.
     *
     * @param entries Addresses, CIDR ranges or host names
     * @return The parsed set
     */
    public static IpRangeSet parse(Collection<String> entries) {
        return parse(entries, InetAddress::getAllByName);
    }

    static IpRangeSet parse(Collection<String> entries, Resolver resolver) {
        if (entries == null || entries.isEmpty()) {
            return EMPTY;
        }
        Map<String, Range> ranges = new LinkedHashMap<>();
        for (String entry : entries) {
            String trimmed = entry == null ? "" : entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                for (Range range : read(trimmed, resolver)) {
                    ranges.putIfAbsent(range.toString(), range);
                }
            } catch (UnknownHostException | IllegalArgumentException e) {
                log.warn("Ignoring {}, which is not an address, a CIDR range or a name that resolves: {}",
                        trimmed, e.getMessage());
            }
        }
        return ranges.isEmpty() ? EMPTY : new IpRangeSet(List.copyOf(ranges.values()));
    }

    private static List<Range> read(String entry, Resolver resolver) throws UnknownHostException {
        int slash = entry.indexOf('/');
        if (slash >= 0) {
            InetAddress address = EndpointAddress.parse(entry.substring(0, slash).trim());
            String prefix = entry.substring(slash + 1).trim();
            if (!prefix.matches("[0-9]{1,3}")) {
                throw new IllegalArgumentException("Invalid prefix length " + prefix);
            }
            return List.of(Range.of(address, Integer.parseInt(prefix)));
        }
        try {
            InetAddress address = EndpointAddress.parse(entry);
            return List.of(Range.of(address, address.getAddress().length * 8));
        } catch (UnknownHostException notALiteral) {
            if (!NAME.matcher(entry).matches()) {
                throw notALiteral;
            }
        }
        InetAddress[] addresses = resolver.resolve(entry);
        List<Range> ranges = new ArrayList<>(addresses.length);
        for (InetAddress address : addresses) {
            ranges.add(Range.of(address, address.getAddress().length * 8));
        }
        return ranges;
    }

    /**
     * @return Whether the set matches nothing
     */
    public boolean isEmpty() {
        return this.ranges.isEmpty();
    }

    /**
     * @param other The set to add
     * @return A set matching any address either set matches
     */
    public IpRangeSet union(IpRangeSet other) {
        if (other.isEmpty()) {
            return this;
        }
        if (this.isEmpty()) {
            return other;
        }
        Map<String, Range> ranges = new LinkedHashMap<>();
        for (Range range : this.ranges) {
            ranges.putIfAbsent(range.toString(), range);
        }
        for (Range range : other.ranges) {
            ranges.putIfAbsent(range.toString(), range);
        }
        return new IpRangeSet(List.copyOf(ranges.values()));
    }

    /**
     * The set as numeric CIDR ranges, such as {@code 192.168.1.5/32}, with names resolved. This is
     * the form the ICE endpoint takes for its PROXY protocol relays:
     * {@code endpoint.setProxyRelays(set.entries().toArray(String[]::new))}. The endpoint refuses
     * more than 4096 ranges.
     *
     * @return The ranges, once each, in the order they were given
     */
    public List<String> entries() {
        return this.ranges.stream().map(Range::toString).toList();
    }

    /**
     * @param address An address; an unresolved one matches nothing
     * @return Whether a range of the set holds it
     */
    public boolean contains(InetSocketAddress address) {
        return address != null && contains(address.getAddress());
    }

    /**
     * @param address An address, IPv4-mapped IPv6 matching as IPv4
     * @return Whether a range of the set holds it
     */
    public boolean contains(InetAddress address) {
        if (address == null) {
            return false;
        }
        byte[] raw = normalize(address.getAddress());
        for (Range range : this.ranges) {
            if (range.contains(raw)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "IpRangeSet" + entries();
    }

    /** An IPv4-mapped IPv6 address as its IPv4 form, anything else as it is. */
    private static byte[] normalize(byte[] raw) {
        if (raw.length != 16) {
            return raw;
        }
        for (int i = 0; i < 10; i++) {
            if (raw[i] != 0) {
                return raw;
            }
        }
        if (raw[10] != (byte) 0xff || raw[11] != (byte) 0xff) {
            return raw;
        }
        return new byte[]{raw[12], raw[13], raw[14], raw[15]};
    }

    /** A network, its host bits cleared, and the prefix length that defines it. */
    private record Range(byte[] network, int prefix) {
        static Range of(InetAddress address, int prefix) {
            byte[] raw = normalize(address.getAddress());
            if (prefix < 0 || prefix > raw.length * 8) {
                throw new IllegalArgumentException("Prefix length " + prefix + " is out of range");
            }
            byte[] network = raw.clone();
            for (int bit = prefix; bit < network.length * 8; bit++) {
                network[bit / 8] &= (byte) ~(0x80 >>> (bit % 8));
            }
            return new Range(network, prefix);
        }

        boolean contains(byte[] address) {
            if (address.length != this.network.length) {
                return false;
            }
            int full = this.prefix / 8;
            for (int i = 0; i < full; i++) {
                if (address[i] != this.network[i]) {
                    return false;
                }
            }
            int rest = this.prefix % 8;
            if (rest == 0) {
                return true;
            }
            int mask = (0xff << (8 - rest)) & 0xff;
            return (address[full] & mask) == (this.network[full] & 0xff);
        }

        @Override
        public String toString() {
            return NetUtil.bytesToIpAddress(this.network) + "/" + this.prefix;
        }
    }
}

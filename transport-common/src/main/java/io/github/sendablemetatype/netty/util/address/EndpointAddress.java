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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses and classifies IP literals and endpoints. Never looks up a name, so nothing here can
 * block on DNS or let a remote party make the host contact a resolver.
 */
public final class EndpointAddress {
    private static final Pattern ENDPOINT = Pattern.compile("(?:\\[([^\\]]+)\\]|([^:]+)):([0-9]{1,5})");
    // The charset also turns away a zone id, brackets and surrounding space, which Netty would take
    private static final Pattern LITERAL = Pattern.compile("[0-9a-fA-F:.]{1,45}");

    /** Where an address sits, see {@link #scope}. */
    public enum Scope {
        /** Globally reachable unicast. */
        PUBLIC,
        /** Reachable from the same network or overlay: RFC 1918, carrier grade NAT, unique local. */
        PRIVATE,
        /** This host only. */
        LOOPBACK,
        /** Special purpose, documentation, or otherwise no use to a peer. */
        UNUSABLE
    }

    private EndpointAddress() {
    }

    /**
     * Reads an IP literal, never a name. An IPv4-mapped IPv6 literal comes back as the IPv4
     * address, and a leading zero reads as decimal, never octal.
     *
     * @param value The literal, without brackets
     * @return The address
     * @throws UnknownHostException If the value is not an IP literal
     */
    public static InetAddress parse(String value) throws UnknownHostException {
        if (value == null || !LITERAL.matcher(value).matches()) {
            throw new UnknownHostException("Expected a numeric IP address");
        }
        InetAddress address = NetUtil.createInetAddressFromIpAddressString(value);
        if (address == null) {
            throw new UnknownHostException("Invalid IP address");
        }
        return address;
    }

    /**
     * Reads {@code IPv4:port} or {@code [IPv6]:port}, or a bare IP literal, bracketed or not, as
     * port 0. Never a name.
     *
     * @param value The endpoint
     * @return The endpoint, resolved
     * @throws IllegalArgumentException If the value is none of those
     */
    public static InetSocketAddress parseEndpoint(String value) {
        Matcher endpoint = ENDPOINT.matcher(value);
        try {
            if (!endpoint.matches()) {
                boolean bracketed = value.startsWith("[") && value.endsWith("]");
                return new InetSocketAddress(parse(bracketed ? value.substring(1, value.length() - 1) : value), 0);
            }
            int port = Integer.parseInt(endpoint.group(3));
            if (port >= 1 && port <= 65535) {
                return new InetSocketAddress(parse(endpoint.group(1) == null ? endpoint.group(2) : endpoint.group(1)),
                        port);
            }
        } catch (UnknownHostException invalid) {
            // Falls through to the one message
        }
        throw new IllegalArgumentException("Expected an IP literal, IPv4:port or [IPv6]:port, got " + value);
    }

    /**
     * Where an address sits, per the IANA special-purpose registries reviewed 2026-09-07.
     *
     * @param address The address
     * @return Its scope
     */
    public static Scope scope(InetAddress address) {
        byte[] raw = address.getAddress();
        if (raw.length == 4) {
            int a = raw[0] & 255, b = raw[1] & 255, c = raw[2] & 255, d = raw[3] & 255;
            if (a == 10 || (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168)
                    || (a == 100 && b >= 64 && b <= 127)) {
                return Scope.PRIVATE;
            }
            if (a == 127) {
                return Scope.LOOPBACK;
            }
            // 192.0.0.9 and .10 are the PCP and TURN anycast addresses, which a peer can reach
            if (a == 0 || a >= 224 || (a == 169 && b == 254) || (a == 203 && b == 0 && c == 113)
                    || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                    || (a == 192 && ((b == 88 && c == 99)
                    || (b == 0 && (c == 2 || (c == 0 && d != 9 && d != 10)))))) {
                return Scope.UNUSABLE;
            }
            return Scope.PUBLIC;
        }
        int a = word(raw, 0), b = word(raw, 1);
        if ((a & 0xfe00) == 0xfc00) {
            return Scope.PRIVATE;
        }
        if (address.isLoopbackAddress()) {
            return Scope.LOOPBACK;
        }
        // Global unicast is 2000::/3 alone, less 6to4, the protocol block and the two doc ranges.
        // NAT64 (64:ff9b::/96) falls outside it, which is right for anything announced to a peer.
        if ((a & 0xe000) != 0x2000 || a == 0x2002 || (a == 0x2001 && (b < 0x200 || b == 0xdb8))
                || (a == 0x3fff && b < 0x1000)) {
            return Scope.UNUSABLE;
        }
        return Scope.PUBLIC;
    }

    private static int word(byte[] raw, int index) {
        return ((raw[index * 2] & 255) << 8) | (raw[index * 2 + 1] & 255);
    }

    /**
     * Whether an address is worth announcing to a peer. A private, carrier grade NAT or unique
     * local address is, since a peer on the same network or overlay reaches it; loopback only
     * for local development.
     *
     * @param address          The address
     * @param localDevelopment Whether loopback counts
     * @return Whether a peer could reach it
     */
    public static boolean advertisable(InetAddress address, boolean localDevelopment) {
        Scope scope = scope(address);
        return scope == Scope.PUBLIC || scope == Scope.PRIVATE
                || (localDevelopment && scope == Scope.LOOPBACK);
    }
}

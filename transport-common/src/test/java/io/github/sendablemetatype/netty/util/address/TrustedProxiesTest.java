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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrustedProxiesTest {
    private static final InetSocketAddress LOCAL_PROXY = new InetSocketAddress("127.0.0.1", 40000);

    private static TrustedProxies forwarders(String... ranges) {
        return new TrustedProxies(IpRangeSet.empty(), IpRangeSet.parse(List.of(ranges)));
    }

    private static Optional<InetSocketAddress> client(TrustedProxies trusted, String... lines) {
        return trusted.forwardedClient(LOCAL_PROXY, List.of(lines));
    }

    private static String address(Optional<InetSocketAddress> client) {
        return client.orElseThrow().getAddress().getHostAddress();
    }

    // Lists

    @Test
    void readsOneAddressPerLineFromAList() throws Exception {
        List<URI> fetched = new ArrayList<>();
        IpRangeSet trusted = IpRangeSet.parse(TrustedProxies.expand(List.of("192.0.2.1", "https://lists.example/proxies"),
                uri -> {
                    fetched.add(uri);
                    return "198.51.100.7\n# a comment\n\n203.0.113.0/24\n";
                }));

        assertEquals(List.of(URI.create("https://lists.example/proxies")), fetched);
        assertTrue(trusted.contains(InetAddress.getByName("192.0.2.1")));
        assertTrue(trusted.contains(InetAddress.getByName("198.51.100.7")));
        assertTrue(trusted.contains(InetAddress.getByName("203.0.113.9")));
        // Comments and blank lines are not addresses
        assertFalse(trusted.contains(InetAddress.getByName("198.51.100.8")));
    }

    @Test
    void refusesAListOverPlainHttp() {
        // Anyone on the path could add trusted proxies to a list fetched in the clear
        for (String url : List.of("http://lists.example/proxies", " HTTP://lists.example/proxies")) {
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> TrustedProxies.parseList(List.of("192.0.2.1", url)), url);
            assertTrue(refused.getMessage().contains("https://"), refused.getMessage());
        }
    }

    @Test
    void widensTrustNoFurtherWhenAListCannotBeFetched() throws Exception {
        IpRangeSet trusted = IpRangeSet.parse(TrustedProxies.expand(List.of("192.0.2.1", "https://lists.example/a"),
                uri -> {
                    throw new IOException("returned HTTP 503");
                }));

        assertTrue(trusted.contains(InetAddress.getByName("192.0.2.1")));
        assertEquals(List.of("192.0.2.1/32"), trusted.entries());
    }

    @Test
    void keepsEntriesThatAreNotUrls() {
        assertEquals(List.of("192.0.2.1", "10.0.0.0/8"),
                TrustedProxies.expand(Arrays.asList("192.0.2.1", null, "", "  ", " 10.0.0.0/8 "), uri -> {
                    throw new AssertionError("nothing to fetch");
                }));
        assertTrue(TrustedProxies.parseList(null).isEmpty());
    }

    // Roles

    @Test
    void eachListTrustsItsOwnRoleOnly() {
        TrustedProxies trusted = new TrustedProxies(IpRangeSet.parse(List.of("10.0.0.0/8")),
                IpRangeSet.parse(List.of("192.168.0.0/16")));
        InetSocketAddress haproxy = new InetSocketAddress("10.1.2.3", 1);
        InetSocketAddress forwarder = new InetSocketAddress("192.168.1.2", 1);

        assertTrue(trusted.trustsProxyProtocol(haproxy));
        assertFalse(trusted.trustsForwardedFor(haproxy));
        assertTrue(trusted.forwardedClient(haproxy, List.of("203.0.113.7")).isEmpty(),
                "a PROXY protocol proxy's X-Forwarded-For is not read");
        assertTrue(trusted.trustsForwardedFor(forwarder));
        assertFalse(trusted.trustsProxyProtocol(forwarder));
        assertTrue(trusted.isProxy(haproxy) && trusted.isProxy(forwarder));
        assertFalse(trusted.isProxy(new InetSocketAddress("203.0.113.7", 1)));
    }

    // X-Forwarded-For

    @Test
    void honoursForwardedForFromATrustedProxy() {
        Optional<InetSocketAddress> client = client(forwarders("127.0.0.0/8"), "203.0.113.7");

        assertEquals(new InetSocketAddress("203.0.113.7", LOCAL_PROXY.getPort()), client.orElseThrow(),
                "the header names no port, so the forwarder connection's stands");
    }

    /** A proxy appends to the header the client sent, so entries left of its own are the client's to choose. */
    @Test
    void takesTheEntryTheTrustedProxyAppended() {
        assertEquals("198.51.100.2", address(client(forwarders("127.0.0.0/8"), "203.0.113.7, 198.51.100.2")));
    }

    @Test
    void readsNothingLeftOfTheClient() {
        assertEquals("198.51.100.2", address(client(forwarders("127.0.0.0/8"), "not an address, 198.51.100.2")));
    }

    @Test
    void walksPastTrustedProxiesInAChain() {
        assertEquals("198.51.100.2",
                address(client(forwarders("127.0.0.0/8", "10.0.0.0/8"), "203.0.113.7, 198.51.100.2, 10.1.2.3")));
    }

    @Test
    void takesTheLeftmostWhenEveryEntryIsTrusted() {
        assertEquals("10.0.0.9", address(client(forwarders("127.0.0.0/8", "10.0.0.0/8"), "10.0.0.9, 10.1.2.3")));
    }

    @Test
    void anEntryAProxyAppendedThatIsNoAddressLeavesThePeer() {
        assertTrue(client(forwarders("127.0.0.0/8"), "203.0.113.7, unknown").isEmpty());
    }

    @Test
    void ignoresForwardedForFromAnUntrustedSource() {
        assertTrue(client(forwarders("10.0.0.0/8"), "203.0.113.7").isEmpty());
        assertTrue(client(TrustedProxies.NONE, "203.0.113.7").isEmpty());
    }

    @Test
    void ignoresAHostnameRatherThanResolvingIt() {
        // Resolving would block the caller's event loop on DNS and let the header stand for
        // whatever the answer happened to be
        assertTrue(client(forwarders("127.0.0.0/8"), "localhost").isEmpty());
    }

    @Test
    void ignoresAValueThatIsNotAnAddressAtAll() {
        assertTrue(client(forwarders("127.0.0.0/8"), "not an address").isEmpty());
        assertTrue(client(forwarders("127.0.0.0/8"), "   ").isEmpty());
        assertTrue(client(forwarders("127.0.0.0/8")).isEmpty(), "no header at all");
    }

    @Test
    void honoursAnIpv6LiteralBareOrInBrackets() {
        assertEquals("2001:db8:0:0:0:0:0:1", address(client(forwarders("127.0.0.0/8"), "[2001:db8::1]")));
        assertEquals("2001:db8:0:0:0:0:0:1", address(client(forwarders("127.0.0.0/8"), "2001:db8::1")));
    }

    @Test
    void readsEveryHeaderLineInOrder() {
        // HAProxy's option forwardfor adds a line of its own: the client's line comes first and
        // must not get to choose the address
        TrustedProxies trusted = forwarders("127.0.0.0/8");
        assertEquals("198.51.100.2", address(client(trusted, "203.0.113.7", "198.51.100.2")));
        assertEquals("198.51.100.2",
                address(client(forwarders("127.0.0.0/8", "10.0.0.0/8"), "203.0.113.7, 198.51.100.2", "10.1.2.3")));
    }

    @Test
    void acceptsEntriesWithAPort() {
        TrustedProxies trusted = forwarders("127.0.0.0/8", "10.0.0.0/8");

        assertEquals(new InetSocketAddress("203.0.113.7", 5678),
                client(trusted, "203.0.113.7:5678").orElseThrow());
        assertEquals(new InetSocketAddress("2001:db8::1", 5678),
                client(trusted, "[2001:db8::1]:5678").orElseThrow());
        assertEquals("203.0.113.7", address(client(trusted, "203.0.113.7:5678, 10.1.2.3:443")),
                "a trusted entry with a port is still walked past");
    }
}

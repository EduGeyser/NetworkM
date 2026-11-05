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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IpRangeSetTest {

    private static boolean contains(IpRangeSet set, String address) throws Exception {
        return set.contains(InetAddress.getByName(address));
    }

    /** Answers only the names in the map, and records every name asked for. */
    private static IpRangeSet parse(List<String> entries, Map<String, String[]> dns, List<String> asked) {
        return IpRangeSet.parse(entries, host -> {
            asked.add(host);
            String[] answer = dns.get(host);
            if (answer == null) {
                throw new UnknownHostException(host);
            }
            InetAddress[] addresses = new InetAddress[answer.length];
            for (int i = 0; i < answer.length; i++) {
                addresses[i] = EndpointAddress.parse(answer[i]);
            }
            return addresses;
        });
    }

    @Test
    void aUnionMatchesEitherSet() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("10.0.0.0/8")).union(IpRangeSet.parse(List.of("192.168.1.5")));

        assertTrue(contains(set, "10.0.0.1"));
        assertTrue(contains(set, "192.168.1.5"));
        assertFalse(contains(set, "192.168.1.6"));
        assertEquals(List.of("10.0.0.0/8", "192.168.1.5/32"), set.entries());
    }

    @Test
    void keepsOnlyTheEntriesItCouldParse() {
        assertEquals(List.of("10.0.0.0/8"), IpRangeSet.parse(List.of(" 10.0.0.0/8 ", "not an address")).entries());
    }

    @Test
    void writesEntriesAsNumericCidr() {
        assertEquals(List.of("2001:db8::1/128", "2001:db8::/32"),
                IpRangeSet.parse(List.of("2001:0db8:0:0::1", "2001:db8::/ 32")).entries());
    }

    @Test
    void clearsHostBitsAndListsEachRangeOnce() {
        // The relay list goes to native code as these strings, so they must be canonical
        assertEquals(List.of("10.1.0.0/16", "203.0.113.7/32"),
                IpRangeSet.parse(List.of("10.1.2.3/16", "10.1.0.0/16", "203.0.113.7", "203.0.113.7/32")).entries());
    }

    @Test
    void trustsEveryAddressANameResolvesTo() throws Exception {
        List<String> asked = new ArrayList<>();
        IpRangeSet set = parse(List.of("proxy.example.test"),
                Map.of("proxy.example.test", new String[]{"198.51.100.7", "2001:db8::7"}), asked);

        assertTrue(contains(set, "198.51.100.7"));
        assertTrue(contains(set, "2001:db8::7"));
        assertFalse(contains(set, "198.51.100.8"));
    }

    @Test
    void neverLooksUpWhatCannotBeAName() {
        List<String> asked = new ArrayList<>();
        IpRangeSet set = parse(List.of("10.0.0.999", "not an address", "1.2.3.4/8x", "proxy.example.test/24"),
                Map.of(), asked);

        assertTrue(set.isEmpty());
        assertEquals(List.of(), asked, "a mistyped literal or range must not reach a resolver");
    }

    @Test
    void matchesInsideAnIpv4Range() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("10.0.0.0/8"));

        assertTrue(contains(set, "10.0.0.1"));
        assertTrue(contains(set, "10.255.255.254"));
        assertFalse(contains(set, "11.0.0.1"));
    }

    @Test
    void matchesPrefixesThatEndInsideAByte() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("100.64.0.0/10", "2001:db8:8000::/33"));

        assertTrue(contains(set, "100.64.0.1"));
        assertTrue(contains(set, "100.127.255.255"));
        assertFalse(contains(set, "100.128.0.0"));
        assertFalse(contains(set, "100.63.255.255"));
        assertTrue(contains(set, "2001:db8:ffff::1"));
        assertFalse(contains(set, "2001:db8:7fff::1"));
    }

    @Test
    void zeroPrefixMatchesTheWholeFamilyOnly() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("0.0.0.0/0"));

        assertTrue(contains(set, "203.0.113.1"));
        assertFalse(contains(set, "2001:db8::1"));
    }

    @Test
    void matchesInsideAnIpv6Range() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("2001:db8::/32"));

        assertTrue(contains(set, "2001:db8::1"));
        assertTrue(contains(set, "2001:db8:dead:beef::5"));
        assertFalse(contains(set, "2001:db9::1"));
    }

    @Test
    void anIpv4MappedAddressMatchesItsIpv4Range() throws Exception {
        // A dual-stack socket reports IPv4 peers in this form
        InetAddress mapped = Inet6Address.getByAddress(null,
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xff, (byte) 0xff, 10, 1, 2, 3}, -1);
        assertTrue(IpRangeSet.parse(List.of("10.0.0.0/8")).contains(mapped));
        assertTrue(IpRangeSet.parse(List.of("::ffff:10.1.2.3")).contains(InetAddress.getByName("10.1.2.3")));
    }

    @Test
    void aBareAddressCoversOnlyItself() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("192.168.1.5"));

        assertTrue(contains(set, "192.168.1.5"));
        assertFalse(contains(set, "192.168.1.6"));
    }

    @Test
    void combinesEntriesAndSkipsBrokenOnes() throws Exception {
        IpRangeSet set = parse(List.of("10.0.0.0/8", "not-an-address", "192.168.1.5", ""), Map.of(),
                new ArrayList<>());

        assertTrue(contains(set, "10.1.2.3"));
        assertTrue(contains(set, "192.168.1.5"));
        assertFalse(contains(set, "172.16.0.1"));
        assertFalse(set.isEmpty());
    }

    @Test
    void trustsNothingWhenNothingIsConfigured() {
        // Trust has to start closed, whatever shape the absence of configuration takes
        assertTrue(IpRangeSet.parse(null).isEmpty());
        assertTrue(IpRangeSet.parse(List.of()).isEmpty());
        assertTrue(IpRangeSet.parse(Arrays.asList(null, "", "   ")).isEmpty());
    }

    @Test
    void trustsNothingWhenEveryEntryIsBroken() {
        IpRangeSet set = IpRangeSet.parse(List.of("not an address", "10.0.0.0/x", "10.0.0.0/99", "::/129"));

        assertTrue(set.isEmpty(), "a file of typos must not become a wildcard");
    }

    @Test
    void aBareIpv6AddressCoversOnlyItself() throws Exception {
        IpRangeSet set = IpRangeSet.parse(List.of("2001:db8::1"));

        assertTrue(set.contains(InetAddress.getByName("2001:db8::1")));
        assertFalse(set.contains(InetAddress.getByName("2001:db8::2")));
    }

    @Test
    void matchesNothingItCannotRead() {
        IpRangeSet set = IpRangeSet.parse(List.of("10.0.0.0/8"));

        assertFalse(set.contains((InetSocketAddress) null));
        assertFalse(set.contains((InetAddress) null));
        assertFalse(set.contains(InetSocketAddress.createUnresolved("10.0.0.1", 1)),
                "an unresolved address names no host, so it can match no range");
    }

    @Test
    void anEmptySetMatchesNothing() throws Exception {
        assertTrue(IpRangeSet.parse(List.of()).isEmpty());
        assertTrue(IpRangeSet.empty().isEmpty());
        assertFalse(contains(IpRangeSet.empty(), "10.0.0.1"));
    }
}

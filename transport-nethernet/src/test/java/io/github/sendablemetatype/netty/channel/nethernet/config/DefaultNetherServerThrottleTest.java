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

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNetherServerThrottleTest {
    private final AtomicLong now = new AtomicLong();

    private DefaultNetherServerThrottle throttle(int maxOpen, long windowMillis, int maxJoins) {
        return new DefaultNetherServerThrottle(maxOpen, Duration.ofMillis(windowMillis), maxJoins, now::get);
    }

    private void advance(long millis) {
        now.addAndGet(Duration.ofMillis(millis).toNanos());
    }

    private static InetSocketAddress at(String host) {
        return new InetSocketAddress(host, 19132);
    }

    @Test
    void limitsJoinsPerWindow() {
        var throttle = throttle(10, 60_000, 3);
        for (int i = 0; i < 3; i++) {
            assertTrue(throttle.accept(at("192.0.2.1")));
        }
        assertFalse(throttle.accept(at("192.0.2.1")));
        assertTrue(throttle.accept(at("192.0.2.2")), "another address has a window of its own");
    }

    @Test
    void allowsJoinsAgainOnceTheWindowPasses() {
        var throttle = throttle(10, 200, 1);
        assertTrue(throttle.accept(at("192.0.2.1")));
        assertFalse(throttle.accept(at("192.0.2.1")));
        advance(200);
        assertTrue(throttle.accept(at("192.0.2.1")));
    }

    @Test
    void limitsOpenConnectionsUntilOneCloses() {
        var throttle = throttle(2, 60_000, 100);
        assertTrue(throttle.accept(at("192.0.2.1")));
        assertTrue(throttle.accept(at("192.0.2.1")));
        assertFalse(throttle.accept(at("192.0.2.1")));
        throttle.closed(at("192.0.2.1"));
        assertTrue(throttle.accept(at("192.0.2.1")));
    }

    @Test
    void countsAnIpv6Slash64AsOneAddress() {
        var throttle = throttle(10, 60_000, 1);
        assertTrue(throttle.accept(at("2001:db8::1")));
        assertFalse(throttle.accept(at("2001:db8::2")));
        assertTrue(throttle.accept(at("2001:db8:0:1::1")));
    }

    @Test
    void theDefaultsLetAClassroomBehindOneAddressJoinAtOnce() {
        var throttle = new DefaultNetherServerThrottle();
        for (int i = 0; i < DefaultNetherServerThrottle.DEFAULT_MAX_JOINS; i++) {
            assertTrue(throttle.accept(at("192.0.2.1")));
        }
        assertFalse(throttle.accept(at("192.0.2.1")));
    }

    @Test
    void admitsAnswersWithoutCounting() {
        var throttle = throttle(10, 60_000, 1);
        assertTrue(throttle.admits(at("192.0.2.1")));
        assertTrue(throttle.admits(at("192.0.2.1")));
        assertEquals(0, throttle.entryCount());
        assertTrue(throttle.accept(at("192.0.2.1")));
        assertFalse(throttle.admits(at("192.0.2.1")));
    }

    /** Upstream left an entry for a refused address that nothing removed. */
    @Test
    void keepsNoEntryForAnAddressWithNothingLeftToCount() {
        var throttle = throttle(1, 1_000, 5);
        assertTrue(throttle.accept(at("192.0.2.1")));
        assertFalse(throttle.accept(at("192.0.2.1")), "one connection is open");
        throttle.closed(at("192.0.2.1"));
        assertEquals(1, throttle.entryCount(), "the window still counts its join");

        advance(1_000);
        assertTrue(throttle.accept(at("192.0.2.2")), "a later join sweeps quiet addresses");
        assertEquals(1, throttle.entryCount());
        throttle.closed(at("192.0.2.2"));
        advance(1_000);
        assertTrue(throttle.admits(at("192.0.2.2")));
        assertEquals(0, throttle.entryCount());
    }

    @Test
    void aCloseWithoutAnAcceptChangesNothing() {
        var throttle = throttle(1, 60_000, 5);
        throttle.closed(at("192.0.2.1"));
        assertEquals(0, throttle.entryCount());
        assertTrue(throttle.accept(at("192.0.2.1")));
        assertFalse(throttle.accept(at("192.0.2.1")));
    }
}

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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Limits the connections open from one address and the joins it may start per window, like
 * RakNet's {@code DefaultRakServerThrottle}. IPv6 addresses count per /64, which one host usually
 * holds whole.
 *
 * <p>The defaults, {@value #DEFAULT_MAX_OPEN} open connections and {@value #DEFAULT_MAX_JOINS}
 * joins per {@value #DEFAULT_WINDOW_SECONDS} s, are ten times upstream's: a school shares one NAT
 * address between whole classes, which then join within seconds of each other.
 *
 * <p>A join is checked and counted in one step, so concurrent joins cannot pass a limit together.
 * An address with nothing open and no joins in its current window holds no entry.
 */
public class DefaultNetherServerThrottle implements NetherServerThrottle {
    public static final int DEFAULT_MAX_OPEN = 100;
    public static final int DEFAULT_MAX_JOINS = 30;
    public static final int DEFAULT_WINDOW_SECONDS = 4;

    private final int maxOpen;
    private final long windowNanos;
    private final int maxJoins;
    private final LongSupplier clock;

    // Guarded by this.
    private final Map<InetAddress, Entry> entries = new HashMap<>();
    private long nextSweep;

    /** One address's open connections and its current window. */
    private static final class Entry {
        int open;
        long windowStart;
        int joins;
    }

    /** {@value #DEFAULT_MAX_OPEN} open connections and {@value #DEFAULT_MAX_JOINS} joins per {@value #DEFAULT_WINDOW_SECONDS} s. */
    public DefaultNetherServerThrottle() {
        this(DEFAULT_MAX_OPEN, Duration.ofSeconds(DEFAULT_WINDOW_SECONDS), DEFAULT_MAX_JOINS);
    }

    /**
     * @param maxOpen the most connections open from one address at once
     * @param window  how long a window lasts, counted from its first join
     * @param maxJoins the most joins one address may start per window
     */
    public DefaultNetherServerThrottle(int maxOpen, Duration window, int maxJoins) {
        this(maxOpen, window, maxJoins, System::nanoTime);
    }

    DefaultNetherServerThrottle(int maxOpen, Duration window, int maxJoins, LongSupplier clock) {
        if (maxOpen < 1 || maxJoins < 1 || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("Throttle limits and window must be positive");
        }
        this.maxOpen = maxOpen;
        this.windowNanos = window.toNanos();
        this.maxJoins = maxJoins;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.nextSweep = clock.getAsLong() + windowNanos;
    }

    @Override
    public synchronized boolean admits(InetSocketAddress address) {
        Entry entry = current(key(address), clock.getAsLong());
        return entry == null || (entry.open < maxOpen && entry.joins < maxJoins);
    }

    @Override
    public synchronized boolean accept(InetSocketAddress address) {
        long now = clock.getAsLong();
        sweep(now);
        InetAddress key = key(address);
        Entry entry = current(key, now);
        if (entry != null && (entry.open >= maxOpen || entry.joins >= maxJoins)) {
            return false;
        }
        if (entry == null) {
            entry = new Entry();
            entry.windowStart = now;
            entries.put(key, entry);
        }
        entry.open++;
        entry.joins++;
        return true;
    }

    @Override
    public synchronized void closed(InetSocketAddress address) {
        long now = clock.getAsLong();
        InetAddress key = key(address);
        Entry entry = current(key, now);
        if (entry != null && entry.open > 0 && --entry.open == 0 && entry.joins == 0) {
            entries.remove(key);
        }
    }

    /** The address's entry with its window brought up to date, or null when it has none left. */
    private Entry current(InetAddress key, long now) {
        Entry entry = entries.get(key);
        if (entry != null && now - entry.windowStart >= windowNanos) {
            entry.windowStart = now;
            entry.joins = 0;
            if (entry.open == 0) {
                entries.remove(key);
                return null;
            }
        }
        return entry;
    }

    /** Drops the entries of addresses that went quiet, at most once per window. */
    private void sweep(long now) {
        if (now - nextSweep < 0) {
            return;
        }
        nextSweep = now + windowNanos;
        entries.values().removeIf(entry -> entry.open == 0 && now - entry.windowStart >= windowNanos);
    }

    synchronized int entryCount() {
        return entries.size();
    }

    private static InetAddress key(InetSocketAddress socketAddress) {
        InetAddress address = Objects.requireNonNull(socketAddress.getAddress(), "an unresolved address");
        if (!(address instanceof Inet6Address)) {
            return address;
        }
        byte[] prefix = address.getAddress();
        Arrays.fill(prefix, 8, 16, (byte) 0);
        try {
            return InetAddress.getByAddress(prefix);
        } catch (UnknownHostException e) {
            throw new AssertionError(e); // only thrown for a wrong length
        }
    }
}

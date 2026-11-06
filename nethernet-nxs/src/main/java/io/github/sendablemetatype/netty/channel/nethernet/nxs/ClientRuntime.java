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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ControlCarrier;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ProviderTransport;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * What one client's parts share. Fields without {@code volatile} belong to the lifecycle executor; the volatile ones
 * are also read by application threads.
 */
final class ClientRuntime {
    /** A clock difference under the tolerance is left alone; one past the warning threshold is reported. */
    private static final long CLOCK_TOLERANCE_MILLIS = 2000, CLOCK_WARNING_MILLIS = 10000;

    final ProviderClient.Configuration config;
    final String origin;
    final ProviderClock clock;
    final ScheduledExecutorService executor;
    final ProviderLog log;
    final ProviderTransport transport;
    final StateKeeper keeper;
    Wire.Discovery discovery;
    /** Registered and checking in. */
    boolean started;
    /** Draining: still counted and reported, no longer offered to players. */
    boolean draining;
    /** The registration is no longer this process's, so it stops without draining or deregistering. */
    boolean fenced;
    volatile boolean closing;
    boolean closed;
    /** Stops the client; set by the client once it exists. */
    Runnable stop = () -> {
    };
    /**
     * The provider's clock minus this machine's, from its replies; positive when this machine is behind. Zero within
     * the tolerance, and moved only by a change past it, so it is what the operator is told.
     */
    private volatile long clockOffsetMillis;
    /** The same as the latest reply measured it, unrounded, which assisted deadlines are read through. */
    private volatile long measuredOffsetMillis;
    private long warnedClockOffsetMillis;
    private volatile ControlCarrier lastCarrier = ControlCarrier.NONE;

    ClientRuntime(ProviderClient.Configuration config, ProviderClock clock, ScheduledExecutorService executor,
                  ProviderLog log, ProviderTransport transport, ProviderStateStore store) {
        this.config = config;
        this.origin = ProviderOrigin.canonicalize(config.provider());
        this.clock = clock;
        this.executor = executor;
        this.log = log;
        this.transport = transport;
        this.keeper = new StateKeeper(store, log, () -> stop.run());
    }

    long clockOffsetMillis() {
        return clockOffsetMillis;
    }

    /**
     * The latest measured offset. A provider sets an assisted join's deadline on its own clock, and a host only a
     * second behind would otherwise see a full-length attempt as longer than any may be.
     */
    long measuredOffsetMillis() {
        return measuredOffsetMillis;
    }

    ControlCarrier lastCarrier() {
        return lastCarrier;
    }

    void carried(ControlCarrier carrier) {
        lastCarrier = carrier;
    }

    /** Measures the provider's clock against this machine's from a reply. */
    void observeProviderTime(long providerMillis, long receivedMillis) {
        if (providerMillis <= 0 || providerMillis > 9007199254740991L) {
            return;
        }
        long offset = providerMillis - receivedMillis;
        measuredOffsetMillis = offset;
        if (Math.abs(offset) < CLOCK_TOLERANCE_MILLIS) {
            offset = 0;
        }
        if (Math.abs(offset - clockOffsetMillis) < CLOCK_TOLERANCE_MILLIS) {
            return;
        }
        clockOffsetMillis = offset;
        if (Math.abs(offset) >= CLOCK_WARNING_MILLIS) {
            // Said again only when the difference itself moves, not on every jittery reply
            if (Math.abs(offset - warnedClockOffsetMillis) >= CLOCK_WARNING_MILLIS) {
                warnedClockOffsetMillis = offset;
                log.clockSkew(offset);
            }
        } else if (warnedClockOffsetMillis != 0) {
            warnedClockOffsetMillis = 0;
            log.clockInStep();
        }
    }

    /** @throws IOException While a WebSocket operation's fate is unknown, which ordinary recovery must settle first */
    void requireResolved() throws IOException {
        if (keeper.state().pendingOperation() != null) {
            throw new IOException("Unresolved provider operation; ordinary registration recovery required");
        }
    }

    Wire.SavedRegistration registration() {
        Wire.SavedRegistration registration = keeper.state().registration();
        if (registration == null) {
            throw new IllegalStateException("Not registered");
        }
        return registration;
    }
}

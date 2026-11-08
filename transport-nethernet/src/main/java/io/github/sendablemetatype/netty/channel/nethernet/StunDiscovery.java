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

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.ice.StunObserver;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Asks STUN servers for this host's public address from the shared ICE port, one
 * {@link StunObserver} per server, and hands out the mappings that are still fresh.
 * {@link #refresh} polls them on the endpoint's event loop and keeps what {@link #fresh} hands to
 * answers, so an answer never touches a monitor.
 */
final class StunDiscovery implements AutoCloseable {
    /**
     * How often {@link #refresh} should run, which is also how late a new mapping reaches answers.
     * Reading a monitor sends nothing; the engine asks the servers on its own, every 15 seconds.
     */
    static final Duration REFRESH = Duration.ofSeconds(1);

    private final int icePort;
    private final List<StunObserver> observers = new ArrayList<>();
    private volatile List<InetSocketAddress> fresh = List.of();
    private boolean closed;

    /**
     * Opens a monitor for every server.
     *
     * @param servers The servers to ask, numeric or unresolved names
     * @param maxAge  How long a mapping is still handed out after its last answer
     * @param icePort The port the monitors and the peers share
     * @throws RuntimeException If one cannot be opened, with any already opened closed again
     */
    StunDiscovery(List<InetSocketAddress> servers, Duration maxAge, int icePort, StunObserver.MonitorFactory factory,
                  LongSupplier nanoTime) {
        this.icePort = icePort;
        try {
            for (InetSocketAddress server : servers) {
                StunObserver observer = new StunObserver(server, maxAge, icePort, factory, nanoTime);
                this.observers.add(observer);
                observer.start();
            }
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * The fresh public mappings as of the last {@link #refresh}, for an answer to carry. One on
     * another port than the ICE port comes with its address on the ICE port too, see
     * {@link StunObserver#withIcePort}.
     *
     * @return The endpoints, empty when there are none
     */
    List<InetSocketAddress> fresh() {
        return this.fresh;
    }

    /** Polls every server, keeps the mappings {@link #fresh} hands out and logs what changed. Called on a timer. */
    synchronized void refresh() {
        if (this.closed) {
            return;
        }
        Set<InetSocketAddress> mappings = new LinkedHashSet<>();
        for (StunObserver observer : this.observers) {
            observer.poll();
            mappings.addAll(observer.mappings());
        }
        this.fresh = StunObserver.withIcePort(mappings, this.icePort);
        // A server that goes quiet, as one blocked where this host is does, only matters while no other answers
        boolean covered = !this.fresh.isEmpty();
        for (StunObserver observer : this.observers) {
            observer.report(covered);
        }
    }

    @Override
    public synchronized void close() {
        this.closed = true;
        this.fresh = List.of();
        for (StunObserver observer : this.observers) {
            try {
                observer.close();
            } catch (RuntimeException e) {
                // Logged by the observer, which keeps trying on another close
            }
        }
    }
}

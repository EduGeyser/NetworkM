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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import io.netty.util.NetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Watches one numeric STUN server through a monitor on the gameplay port. The monitor (webrtc-java's
 * {@code IceUdpEndpoint.monitorStun}) only reports readings; this class keeps the per-server state on top of them.
 * The latest mapping counts while younger than the max age, and a fresh one survives a reopened monitor until it
 * ages out. A monitor that fails is closed, its mapping withdrawn, and opened again after {@link #RETRY_DELAY}.
 * Polled by its owner, one thread at a time.
 */
final class StunObservation implements AutoCloseable {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(StunObservation.class);

    /** How long a failed monitor waits before it is opened again. */
    static final Duration RETRY_DELAY = Duration.ofSeconds(30);
    /** How long a server may stay silent after opening before that is reported. */
    static final Duration FIRST_ANSWER = Duration.ofSeconds(30);

    /**
     * The server as of the last poll.
     *
     * @param fresh           Whether its mapping counted, being younger than the max age, at that poll
     * @param usable          Whether its mapping is public, so players elsewhere can reach it
     * @param freshUntilNanos Until when its mapping counts, on the observation's clock
     */
    record Observed(StunReading reading, boolean fresh, boolean usable, long freshUntilNanos) {
    }

    private final long maxAgeNanos;
    private final int icePort;
    private final StunMonitor.Factory factory;
    private final LongSupplier nanoTime;
    private InetSocketAddress server;
    private StunMonitor monitor;
    /** Whether the monitor failed to close, so it is kept until a replacement or close gets it closed. */
    private boolean retained;
    private long epoch;
    private long openedAt;
    private boolean failed;
    private long retryAt;
    private Observed latest;
    private InetSocketAddress announced;
    private boolean started, answered, silenceReported, unusableReported, failureReported, closed;

    StunObservation(InetSocketAddress server, Duration maxAge, int icePort, StunMonitor.Factory factory,
                    LongSupplier nanoTime) {
        if (maxAge.isNegative() || maxAge.isZero()) {
            throw new IllegalArgumentException("The max age must be positive");
        }
        this.server = Objects.requireNonNull(server);
        this.maxAgeNanos = saturatedNanos(maxAge);
        this.icePort = icePort;
        this.factory = Objects.requireNonNull(factory);
        this.nanoTime = Objects.requireNonNull(nanoTime);
    }

    /** Reads the monitor, opening it first when there is none and no retry is pending. */
    void poll() {
        if (closed || retained) {
            return;
        }
        long now = nanoTime.getAsLong();
        if (monitor == null) {
            if (failed && now - retryAt < 0) {
                return;
            }
            try {
                open(now);
            } catch (RuntimeException e) {
                fail(now, e);
                return;
            }
        }
        StunReading reading;
        try {
            reading = monitor.read();
            check(reading);
        } catch (RuntimeException e) {
            fail(now, e);
            return;
        }
        update(reading, now, nanoTime.getAsLong());
        failureReported = false;
    }

    /**
     * Logs what changed since the last call.
     *
     * @param covered Whether another family gives this host a public address, which turns a silent or lost server
     *                from a warning into a debug line
     */
    void report(boolean covered) {
        if (closed || failed || retained) {
            return;
        }
        InetSocketAddress usable = latest != null && latest.fresh() && latest.usable() ? latest.reading().mapped() : null;
        if (!Objects.equals(usable, announced)) {
            if (usable != null) {
                log.info("STUN discovery through {} maps this host to {}{}", label(), NetUtil.toSocketAddressString(usable),
                        usable.getPort() != icePort ? ", a different port than udp/" + icePort : "");
            } else {
                warnUnless(covered, "STUN discovery through {} has no current mapping anymore", label());
            }
            announced = usable;
        }
        if (latest != null && latest.fresh()) {
            answered = true;
            if (!latest.usable() && !unusableReported) {
                log.warn("STUN discovery through {} maps this host to {}, which players elsewhere cannot reach, so it "
                        + "is not announced", label(), NetUtil.toSocketAddressString(latest.reading().mapped()));
                unusableReported = true;
            }
            return;
        }
        if (!answered && !silenceReported && nanoTime.getAsLong() - openedAt >= saturatedNanos(FIRST_ANSWER)) {
            warnUnless(covered, "STUN discovery has had no answer from {} yet", label());
            silenceReported = true;
        }
    }

    /** The server as of the last poll, including a fresh mapping carried over a reopen. */
    Optional<Observed> observed() {
        return Optional.ofNullable(latest);
    }

    /** How many monitors were opened, so a reading can be told from one of an earlier monitor. */
    long epoch() {
        return epoch;
    }

    /** Whether the monitor failed and has not been opened again yet. */
    boolean failed() {
        return failed || retained;
    }

    /**
     * Withdraws what was found and asks another server from the next poll.
     *
     * @throws RuntimeException If the old monitor fails to close again, keeping it and the old server
     */
    void replace(InetSocketAddress server) {
        if (closed) {
            throw new IllegalStateException("STUN observation closed");
        }
        withdraw();
        closeLogged();
        this.server = Objects.requireNonNull(server);
        failed = false;
        started = false;
        answered = false;
        silenceReported = false;
        unusableReported = false;
        failureReported = false;
    }

    /**
     * Stops for good. Closing again retries a monitor that failed to close.
     *
     * @throws RuntimeException If the monitor fails to close
     */
    @Override
    public void close() {
        closed = true;
        withdraw();
        closeLogged();
    }

    private void open(long now) {
        epoch++;
        monitor = Objects.requireNonNull(factory.open(server));
        openedAt = now;
        failed = false;
        if (!started) {
            log.info("Running STUN discovery through {} on udp/{}", label(), icePort);
            started = true;
        }
    }

    /** Refuses a reading from another server, or a mapping of another family than the server's. */
    private void check(StunReading reading) {
        boolean family = reading.mapped() == null
                || AddressFamily.of(reading.mapped().getAddress()) == AddressFamily.of(reading.server().getAddress());
        if (!server.equals(reading.server()) || !family) {
            throw new IllegalStateException("STUN reading from " + reading.server() + " does not match the server");
        }
    }

    private void update(StunReading reading, long sampledAt, long read) {
        if (latest == null || latest.reading().state() != reading.state()) {
            log.debug("STUN discovery through {}: {}, {} answers, {} failures", label(), reading.state(),
                    reading.successfulResponses(), reading.failedTransactions());
        }
        Observed next = observe(reading, sampledAt, read);
        // A reopened monitor starts without a mapping; the one found before counts until it ages out
        if (!next.fresh() && latest != null && latest.fresh() && read - latest.freshUntilNanos() < 0) {
            return;
        }
        latest = next;
    }

    /** A mapping counts while its age, plus however long the read took, is under the max age. */
    private Observed observe(StunReading reading, long sampledAt, long read) {
        if (!reading.answered()) {
            return new Observed(reading, false, false, sampledAt);
        }
        long age = saturatedNanos(reading.lastSuccessAge().orElseThrow());
        long elapsed = Math.max(0, read - sampledAt);
        boolean fresh = age < maxAgeNanos && elapsed < maxAgeNanos - age;
        boolean usable = EndpointAddress.scope(reading.mapped().getAddress()) == EndpointAddress.Scope.PUBLIC;
        return new Observed(reading, fresh, usable, fresh ? sampledAt + (maxAgeNanos - age) : sampledAt);
    }

    private void fail(long now, RuntimeException e) {
        withdraw();
        failed = true;
        retryAt = now + saturatedNanos(RETRY_DELAY);
        if (!failureReported) {
            log.warn("STUN discovery through {} failed, retrying every {} s: {}", label(), RETRY_DELAY.toSeconds(),
                    e.toString());
            failureReported = true;
        } else {
            log.debug("STUN discovery through {} failed again: {}", label(), e.toString());
        }
        if (retained) {
            return;
        }
        try {
            closeMonitor();
        } catch (RuntimeException closing) {
            log.warn("STUN discovery through {} could not close its monitor, which stays stopped until it is "
                    + "replaced or closed: {}", label(), closing.toString());
        }
    }

    private void closeLogged() {
        try {
            closeMonitor();
        } catch (RuntimeException e) {
            log.warn("STUN discovery through {} could not close its monitor: {}", label(), e.toString());
            throw e;
        }
    }

    /** @throws RuntimeException If the monitor fails to close, keeping it */
    private void closeMonitor() {
        StunMonitor current = monitor;
        if (current == null) {
            return;
        }
        try {
            current.close();
        } catch (RuntimeException e) {
            retained = true;
            throw e;
        }
        monitor = null;
        retained = false;
    }

    private void withdraw() {
        latest = null;
        announced = null;
    }

    private void warnUnless(boolean covered, String message, Object... args) {
        if (covered) {
            log.debug(message, args);
        } else {
            log.warn(message, args);
        }
    }

    private String label() {
        return NetUtil.toSocketAddressString(server);
    }

    private static long saturatedNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException tooLong) {
            return Long.MAX_VALUE;
        }
    }
}

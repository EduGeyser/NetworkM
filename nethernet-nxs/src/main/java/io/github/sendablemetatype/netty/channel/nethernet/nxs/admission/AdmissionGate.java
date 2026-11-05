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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * Bounded admission reservations and the records of used tickets. A ticket reserves one source tuple; the same
 * ticket from another tuple, or another ticket on an occupied tuple, is a replay. A used-ticket record stays until
 * the ticket expires, and when the records are full new admissions are refused rather than old records evicted.
 * Capacity is released only by {@link #finish}, once the peer's resources are gone.
 *
 * <p>The transport's identity verifier reports each login decision through {@link #loginDecided}; a session whose
 * login is refused, revoked, or still undecided at the ticket's deadline is handed back by {@link #sweep} for
 * closing, even when its data channels are open.
 */
public final class AdmissionGate {
    /**
     * @param sessions        Reserved tuples, players connected or connecting
     * @param claims          Used-ticket records, kept until each ticket expires
     * @param pending         Reservations whose peer is not created yet
     * @param handshakeMillis How long an admitted peer may take to connect
     */
    public record Limits(int sessions, int claims, int pending, long handshakeMillis) {
        public Limits {
            if (sessions < 1 || sessions > 65536 || claims < sessions || claims > 262144 || pending < 1
                    || pending > sessions || handshakeMillis < 100 || handshakeMillis > 120_000) {
                throw new IllegalArgumentException("Admission limits");
            }
        }

        public static Limits defaults() {
            return new Limits(1024, 8192, 1024, 15_000);
        }
    }

    private enum Login {
        PENDING, ACCEPTED, REJECTED
    }

    /** One reserved ticket and tuple. Its fields belong to the gate. */
    public static final class Reservation {
        private AdmissionContext admission;
        private final TicketId ticketId;
        private final InetSocketAddress tuple;
        private final long expiresAt, acceptedNanos, loginDeadlineNanos;
        private Login login = Login.PENDING;
        private boolean ready, connected, closing, closed;

        private Reservation(AdmissionContext admission, InetSocketAddress tuple, long millis, long nanos) {
            this.admission = admission;
            this.ticketId = admission.ticketId();
            this.tuple = tuple;
            this.expiresAt = admission.expiresAt();
            this.acceptedNanos = nanos;
            this.loginDeadlineNanos = nanos + (admission.expiresAt() - millis) * 1_000_000L;
        }

        public TicketId ticketId() {
            return ticketId;
        }

        public InetSocketAddress tuple() {
            return tuple;
        }

        public long acceptedNanos() {
            return acceptedNanos;
        }

        @Override
        public String toString() {
            return "Reservation[ticketId=" + ticketId + "]";
        }
    }

    public record Stats(int sessions, int pending, int claims, long invalid, long replayRejected, long capacityRejected,
                        long accepted) {
    }

    public record PendingLimitWarning(int pending, int limit, long rejected) {
    }

    private static final long WARNING_INTERVAL_NANOS = 5_000_000_000L;
    private final Limits limits;
    private final AdmissionValidator validator;
    private final Map<TicketId, Reservation> claims = new HashMap<>();
    private final Map<InetSocketAddress, Reservation> tuples = new HashMap<>();
    private int pending;
    private boolean draining, closed;
    private long invalid, replayRejected, capacityRejected, accepted;
    private long pendingLimitRejected, lastPendingWarningNanos;
    private int pendingAtRejection;
    private boolean pendingWarningEmitted;

    public AdmissionGate(Limits limits, AdmissionValidator validator) {
        this.limits = Objects.requireNonNull(limits);
        this.validator = Objects.requireNonNull(validator);
    }

    /**
     * Reserves capacity after token validation. Native STUN verification must still succeed.
     *
     * @return The reservation, or null when the ticket is invalid, replayed or over capacity
     */
    public synchronized Reservation reserve(AdmissionRequest request, long nowMillis, long nowNanos) {
        if (closed) {
            return null;
        }
        VerifiedAdmission verified = validator.validate(request, nowMillis);
        if (verified == null) {
            invalid++;
            return null;
        }
        return reserveAuthenticated(verified, request.address(), nowMillis, nowNanos);
    }

    /**
     * Reserves capacity for a join authenticated some other way, such as an assisted join over the provider's
     * WebSocket. Releases the admission's login binding when it refuses.
     */
    public synchronized Reservation reserveAuthenticated(AdmissionContext admission, InetSocketAddress address,
                                                         long nowMillis, long nowNanos) {
        if (closed || admission.networkId().matches("0+") || admission.expiresAt() <= nowMillis) {
            invalid++;
            admission.loginBinding().release();
            return null;
        }
        if (claims.containsKey(admission.ticketId()) || tuples.containsKey(address)) {
            replayRejected++;
            admission.loginBinding().release();
            return null;
        }
        if (draining) {
            capacityRejected++;
            admission.loginBinding().release();
            return null;
        }
        if (pending >= limits.pending()) {
            capacityRejected++;
            pendingLimitRejected++;
            pendingAtRejection = pending;
            admission.loginBinding().release();
            return null;
        }
        if (tuples.size() >= limits.sessions() || claims.size() >= limits.claims()) {
            capacityRejected++;
            admission.loginBinding().release();
            return null;
        }
        Reservation reservation = new Reservation(admission, address, nowMillis, nowNanos);
        claims.put(reservation.ticketId, reservation);
        tuples.put(reservation.tuple, reservation);
        pending++;
        return reservation;
    }

    /** @return The reservation's admission, or null once it is closing or finished */
    public synchronized AdmissionContext admission(Reservation reservation) {
        return current(reservation) && !reservation.closing ? reservation.admission : null;
    }

    /**
     * Native STUN verification and peer creation succeeded. A used ticket cannot allocate another peer.
     */
    public synchronized boolean ready(Reservation reservation) {
        if (!current(reservation) || reservation.closing || reservation.ready) {
            return false;
        }
        reservation.ready = true;
        pending--;
        accepted++;
        return true;
    }

    /** Both data channels are open. Opening them does not complete the login. */
    public synchronized void connected(Reservation reservation) {
        if (current(reservation) && !reservation.closing) {
            reservation.connected = true;
        }
    }

    /**
     * The transport's identity verifier decided the login, or a trusted forwarding listener accepted it.
     *
     * @param accepted Whether the login may proceed; a refused one is closed at the next sweep
     */
    public synchronized void loginDecided(Reservation reservation, boolean accepted) {
        if (current(reservation) && reservation.login == Login.PENDING) {
            reservation.login = accepted ? Login.ACCEPTED : Login.REJECTED;
        }
    }

    public synchronized void invalidNativeRequest() {
        invalid++;
    }

    /**
     * Call only once native teardown is complete, or when native creation never started.
     */
    public synchronized boolean finish(Reservation reservation) {
        if (!current(reservation)) {
            return false;
        }
        if (!reservation.ready) {
            pending--;
        }
        tuples.remove(reservation.tuple);
        reservation.closed = true;
        reservation.admission.loginBinding().release();
        reservation.admission = null;
        // A copied ticket with forged STUN integrity must not consume the real client's ticket
        if (!reservation.ready || closed) {
            claims.remove(reservation.ticketId);
        }
        return true;
    }

    private boolean current(Reservation reservation) {
        return !reservation.closed && claims.get(reservation.ticketId) == reservation;
    }

    /**
     * Marks for closing the sessions whose login was refused or lapsed and the handshakes that timed out. Capacity
     * stays reserved until {@link #finish} is called for each.
     */
    public synchronized List<Reservation> sweep(long nowMillis, long nowNanos) {
        List<Reservation> timedOut = new ArrayList<>();
        for (Reservation reservation : claims.values()) {
            if (reservation.closed || reservation.closing) {
                continue;
            }
            boolean undecided = reservation.login == Login.PENDING;
            boolean refused = reservation.login == Login.REJECTED
                    || undecided && (!reservation.admission.loginBinding().usable()
                    || nowNanos - reservation.loginDeadlineNanos >= 0);
            boolean stalled = !reservation.connected
                    && nowNanos - reservation.acceptedNanos >= limits.handshakeMillis() * 1_000_000L;
            if (refused || stalled) {
                reservation.closing = true;
                timedOut.add(reservation);
            }
        }
        claims.values().removeIf(reservation -> reservation.closed && reservation.expiresAt <= nowMillis);
        return timedOut;
    }

    public synchronized void drain() {
        draining = true;
    }

    /** Admission policy state only; capacity and installed credentials are checked separately. */
    public synchronized boolean isServing() {
        return !closed && !draining;
    }

    /** @return The sessions still reserved, each now closing; finish each once its peer is gone */
    public synchronized List<Reservation> close() {
        closed = true;
        List<Reservation> active = new ArrayList<>(tuples.values());
        for (Reservation reservation : active) {
            reservation.closing = true;
            reservation.admission.loginBinding().release();
        }
        claims.values().removeIf(reservation -> reservation.closed);
        return active;
    }

    public synchronized Stats stats() {
        return new Stats(tuples.size(), pending, claims.size(), invalid, replayRejected, capacityRejected, accepted);
    }

    /**
     * One aggregate warning about refusals at the pending limit, at most every five seconds.
     *
     * @return The warning, or null when none is due
     */
    public synchronized PendingLimitWarning pollPendingLimitWarning(long nowNanos) {
        if (pendingLimitRejected == 0 || (pendingWarningEmitted
                && nowNanos - lastPendingWarningNanos < WARNING_INTERVAL_NANOS)) {
            return null;
        }
        var warning = new PendingLimitWarning(pendingAtRejection, limits.pending(), pendingLimitRejected);
        pendingLimitRejected = 0;
        lastPendingWarningNanos = nowNanos;
        pendingWarningEmitted = true;
        return warning;
    }
}

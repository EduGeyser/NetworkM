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

import io.github.sendablemetatype.netty.util.nethernet.IdentityKeyVerifier;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * What NXS alone keeps about the tickets it admits: the records of used tickets, which source tuple each holds, and
 * how its login stands. A ticket reserves one source tuple; the same ticket from another tuple, or another ticket on
 * an occupied tuple, is a replay. A used-ticket record stays until the ticket expires, and when the records are full
 * new admissions are refused rather than old records evicted. A reservation ends only by {@link #finish}, once the
 * peer's resources are gone.
 *
 * <p>A record is kept by the ticket's token, the host's ICE ufrag, whichever way the ticket came in: an assisted
 * join's ufrag is itself an {@code NXS1} token, which the stateless path would otherwise admit again once the
 * assisted session ended.
 *
 * <p>The ledger holds no capacity of its own: sessions and pending handshakes are the NetherNet endpoint's one budget
 * for every join source (ruling A-2), which each admitted ticket passes after the ledger. The used-ticket records
 * are bounded here, since only NXS has them.
 *
 * <p>Each reservation's login is decided through its {@linkplain #verifier verifier}, which the host installs as the
 * child's login binding (ruling C-4); a session whose login is refused, revoked, or still undecided at the ticket's
 * deadline is handed back by {@link #sweep} for closing, even when its data channels are open. The sweep reads the
 * verifier's own phase, so a login accepted at any moment is never taken for a lapsed one.
 */
public final class TicketLedger {
    /**
     * The ledger's bounds.
     *
     * @param claims          Used-ticket records, kept until each ticket expires
     * @param handshakeMillis How long an admitted peer may take to connect
     */
    public record Limits(int claims, long handshakeMillis) {
        public Limits {
            if (claims < 1 || claims > 262144 || handshakeMillis < 100 || handshakeMillis > 120_000) {
                throw new IllegalArgumentException("Ticket ledger limits");
            }
        }

        /** The limits a host uses unless it has a reason to change them. */
        public static Limits defaults() {
            return new Limits(8192, 15_000);
        }
    }

    /** One reserved ticket and tuple. Its fields belong to the ledger. */
    public static final class Reservation {
        private AdmissionContext admission;
        private final TicketId ticketId;
        /** The key of the used-ticket record: the token's ticket ID, which for a stateless ticket is its ID. */
        private final TicketId token;
        private final InetSocketAddress tuple;
        private final long recordUntil, acceptedNanos, loginDeadlineNanos;
        private final IdentityKeyVerifier verifier;
        private boolean ready, connected, closing, closed;

        private Reservation(AdmissionContext admission, TicketId token, InetSocketAddress tuple, long recordUntil,
                            long millis, long nanos) {
            this.admission = admission;
            this.ticketId = admission.ticketId();
            this.token = token;
            this.tuple = tuple;
            this.recordUntil = recordUntil;
            this.acceptedNanos = nanos;
            this.loginDeadlineNanos = nanos + (admission.expiresAt() - millis) * 1_000_000L;
            this.verifier = new LoginVerifier(admission.loginBinding());
        }

        /** The admitted ticket. */
        public TicketId ticketId() {
            return ticketId;
        }

        /** The client's address this reservation holds, or null for an assisted join, which holds none. */
        public InetSocketAddress tuple() {
            return tuple;
        }

        /** When the ledger accepted the request, on the clock the caller passed. */
        public long acceptedNanos() {
            return acceptedNanos;
        }

        @Override
        public String toString() {
            return "Reservation[ticketId=" + ticketId + "]";
        }
    }

    /** Counts for monitoring: what the ledger holds now, and what it refused or accepted so far. */
    public record Stats(int sessions, int claims, long invalid, long replayRejected, long capacityRejected,
                        long accepted) {
    }

    private final Limits limits;
    private final AdmissionValidator validator;
    private final Map<TicketId, Reservation> claims = new HashMap<>();
    private final Map<InetSocketAddress, Reservation> tuples = new HashMap<>();
    private int sessions;
    private boolean draining, closed;
    private long invalid, replayRejected, capacityRejected, accepted;

    /**
     * @param validator Checks each request before the ledger reserves anything for it
     */
    public TicketLedger(Limits limits, AdmissionValidator validator) {
        this.limits = Objects.requireNonNull(limits);
        this.validator = Objects.requireNonNull(validator);
    }

    /**
     * Reserves the ticket after token validation. Native STUN verification must still succeed.
     *
     * @return The reservation, or null when the ticket is invalid, replayed or the records are full
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
        return reserveAuthenticated(verified, request.address(), verified.expiresAt(), nowMillis, nowNanos);
    }

    /**
     * Reserves a join authenticated some other way, such as an assisted join over the provider's WebSocket. Releases
     * the admission's login binding when it refuses.
     *
     * @param address        The source tuple the ticket holds; null for an assisted join, which only knows the
     *                       candidates the client claims, so it is kept by its ticket alone (area 9, finding F4)
     * @param tokenExpiresAt When the admission's token expires if that is later than the admission itself, as for an
     *                       assisted join whose ufrag is a token that outlives the join; 0 when unknown. The record
     *                       stays until both passed.
     */
    public synchronized Reservation reserveAuthenticated(AdmissionContext admission, InetSocketAddress address,
                                                         long tokenExpiresAt, long nowMillis, long nowNanos) {
        if (closed || admission.networkId().matches("0+") || admission.expiresAt() <= nowMillis) {
            invalid++;
            admission.loginBinding().release();
            return null;
        }
        TicketId token = TicketId.ofToken(admission.localUfrag());
        if (claims.containsKey(token) || (address != null && tuples.containsKey(address))) {
            replayRejected++;
            admission.loginBinding().release();
            return null;
        }
        if (draining || claims.size() >= limits.claims()) {
            capacityRejected++;
            admission.loginBinding().release();
            return null;
        }
        Reservation reservation = new Reservation(admission, token, address,
                Math.max(admission.expiresAt(), tokenExpiresAt), nowMillis, nowNanos);
        claims.put(token, reservation);
        if (address != null) {
            tuples.put(address, reservation);
        }
        sessions++;
        return reservation;
    }

    /** @return The reservation's admission, or null once it is closing or finished */
    public synchronized AdmissionContext admission(Reservation reservation) {
        return current(reservation) && !reservation.closing ? reservation.admission : null;
    }

    /** The peer was created for the reservation. A used ticket cannot allocate another peer. */
    public synchronized boolean ready(Reservation reservation) {
        if (!current(reservation) || reservation.closing || reservation.ready) {
            return false;
        }
        reservation.ready = true;
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
     * The one-shot verifier that decides the reservation's login, to install as the child's login binding with
     * {@code TransportIdentityBinding.of}. It consults the admission's {@link LoginKeyBinding}.
     */
    public IdentityKeyVerifier verifier(Reservation reservation) {
        return reservation.verifier;
    }

    /** Counts a request the endpoint rejected before it reached the ledger, so {@link #stats} include it. */
    public synchronized void invalidNativeRequest() {
        invalid++;
    }

    /**
     * Call only once the peer's teardown is complete, or when no peer was ever created.
     */
    public synchronized boolean finish(Reservation reservation) {
        if (!current(reservation)) {
            return false;
        }
        sessions--;
        if (reservation.tuple != null) {
            tuples.remove(reservation.tuple);
        }
        reservation.closed = true;
        reservation.verifier.close();
        reservation.admission.loginBinding().release();
        reservation.admission = null;
        // A copied ticket with forged STUN integrity must not consume the real client's ticket
        if (!reservation.ready || closed) {
            claims.remove(reservation.token);
        }
        return true;
    }

    private boolean current(Reservation reservation) {
        return !reservation.closed && claims.get(reservation.token) == reservation;
    }

    /**
     * Marks for closing the sessions whose login was refused or lapsed and the handshakes that timed out. Each stays
     * reserved until {@link #finish} is called for it.
     */
    public synchronized List<Reservation> sweep(long nowMillis, long nowNanos) {
        List<Reservation> timedOut = new ArrayList<>();
        for (Reservation reservation : claims.values()) {
            if (reservation.closed || reservation.closing) {
                continue;
            }
            // rejected() also covers a pending login whose binding lapsed or was revoked
            boolean refused = reservation.verifier.rejected()
                    || reservation.verifier.pending() && nowNanos - reservation.loginDeadlineNanos >= 0;
            boolean stalled = !reservation.connected
                    && nowNanos - reservation.acceptedNanos >= limits.handshakeMillis() * 1_000_000L;
            if (refused || stalled) {
                reservation.closing = true;
                timedOut.add(reservation);
            }
        }
        claims.values().removeIf(reservation -> reservation.closed && reservation.recordUntil <= nowMillis);
        return timedOut;
    }

    /** Refuses new reservations from now on; reserved sessions carry on. */
    public synchronized void drain() {
        draining = true;
    }

    /** Admission policy state only; capacity and installed credentials are checked separately. */
    public synchronized boolean isServing() {
        return !closed && !draining;
    }

    /**
     * Refuses new reservations and revokes the pending logins.
     *
     * @return The sessions still reserved, each now closing; finish each once its peer is gone
     */
    public synchronized List<Reservation> close() {
        closed = true;
        List<Reservation> active = claims.values().stream().filter(reservation -> !reservation.closed).toList();
        for (Reservation reservation : active) {
            reservation.closing = true;
            reservation.verifier.close();
            reservation.admission.loginBinding().release();
        }
        claims.values().removeIf(reservation -> reservation.closed);
        return active;
    }

    /** Asks the admission's binding; erases it once the login is decided or the session ends. */
    private static final class LoginVerifier extends IdentityKeyVerifier {
        private final LoginKeyBinding binding;

        private LoginVerifier(LoginKeyBinding binding) {
            this.binding = binding;
        }

        @Override
        protected boolean usable() {
            return binding.usable();
        }

        @Override
        protected boolean matches(byte[] canonicalKey) {
            return binding.matches(canonicalKey);
        }

        @Override
        protected void release() {
            binding.release();
        }
    }

    /** The current counts. */
    public synchronized Stats stats() {
        return new Stats(sessions, claims.size(), invalid, replayRejected, capacityRejected, accepted);
    }
}

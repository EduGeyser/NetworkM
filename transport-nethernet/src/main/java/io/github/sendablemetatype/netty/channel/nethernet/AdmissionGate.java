package io.github.sendablemetatype.netty.channel.nethernet;

/**
 * One server endpoint's budget, shared by every join source: sessions, held from a join's
 * admission until its connection's termination completed, and pending handshakes, held from
 * admission until the connection goes active or its termination completed. Joins waiting for
 * their identity check count as pending, so the pending budget also bounds the checks in flight
 * and the peers still tearing down that never went active. Thread-safe.
 */
final class AdmissionGate {
    // Guarded by this.
    private int sessions;
    private int pending;

    /**
     * Whether {@link #reserve} would succeed now.
     *
     * @param maxSessions the most sessions at once
     * @param maxPending the most pending handshakes at once
     */
    synchronized boolean admits(int maxSessions, int maxPending) {
        return sessions < maxSessions && pending < maxPending;
    }

    /**
     * Takes one session and one pending handshake.
     *
     * @return the reservation, or null when either budget is spent
     */
    synchronized Reservation reserve(int maxSessions, int maxPending) {
        if (!admits(maxSessions, maxPending)) {
            return null;
        }
        sessions++;
        pending++;
        return new Reservation();
    }

    synchronized int sessions() {
        return sessions;
    }

    synchronized int pending() {
        return pending;
    }

    /** What one join holds. Each part is given back once, however often it is released. */
    final class Reservation {
        private boolean pendingHeld = true;
        private boolean sessionHeld = true;

        private Reservation() {
        }

        /** The connection went active: it is no longer a pending handshake. */
        void handshakeDone() {
            synchronized (AdmissionGate.this) {
                if (pendingHeld) {
                    pendingHeld = false;
                    pending--;
                }
            }
        }

        /** Gives everything back: a refused join, or a connection whose termination completed. */
        void release() {
            synchronized (AdmissionGate.this) {
                handshakeDone();
                if (sessionHeld) {
                    sessionHeld = false;
                    sessions--;
                }
            }
        }
    }
}

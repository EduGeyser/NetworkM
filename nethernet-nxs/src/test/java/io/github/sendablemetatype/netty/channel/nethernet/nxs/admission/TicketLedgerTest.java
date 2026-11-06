package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class TicketLedgerTest extends AdmissionFixture {
    final InetSocketAddress other = new InetSocketAddress("127.0.0.1", 23451);

    TicketLedger gate() {
        return new TicketLedger(new TicketLedger.Limits(2, 1000));
    }

    AdmissionRequest elsewhere() {
        return new AdmissionRequest(token, remote, other);
    }

    /** A second ticket, minted for the same client so only the source tuple differs. */
    private String anotherToken() throws Exception {
        var trusted = validator().validate(request(), now);
        return TestSignalingProvider.answer(trusted.remoteDescription(), trusted.remoteFingerprint().value(), 49199,
                now + 30_000, f.getAsJsonObject("context").get("audience").getAsString(), false).token();
    }

    @Test
    void refusesLimitsItCouldNotHonour() {
        assertDoesNotThrow(TicketLedger.Limits::defaults);

        assertThrows(IllegalArgumentException.class, () -> new TicketLedger.Limits(0, 1000), "no claims");
        assertThrows(IllegalArgumentException.class, () -> new TicketLedger.Limits(262145, 1000),
                "more claims than it will hold");
        assertThrows(IllegalArgumentException.class, () -> new TicketLedger.Limits(2, 99),
                "a handshake window too short to complete one");
        assertThrows(IllegalArgumentException.class, () -> new TicketLedger.Limits(2, 120_001),
                "or long enough to hold capacity all day");
    }

    @Test
    void holdsOneReservationPerSourceTuple() throws Exception {
        // Another ticket from the same address must not take a second slot
        var gate = gate();
        assertNotNull(reserve(gate, request(), now, 0));

        assertNull(reserve(gate, request(anotherToken(), remote), now, 0));
        assertEquals(1, gate.stats().replayRejected());
        assertEquals(1, gate.stats().sessions());
    }

    @Test
    void refusesWhenTheClaimsItRemembersAreFull() throws Exception {
        // Sessions are free, but every token it has seen is still remembered
        var gate = new TicketLedger(new TicketLedger.Limits(2, 1000));
        var first = reserve(gate, request(), now, 0);
        assertNotNull(first);
        assertTrue(gate.ready(first));
        gate.finish(first);

        assertNotNull(reserve(gate, request(anotherToken(), remote), now, 0));
        assertNull(reserve(gate, new AdmissionRequest(anotherToken(), remote, other), now, 0));
        assertEquals(1, gate.stats().capacityRejected());
    }

    @Test
    void tellsTheCallerWhatItReservedWithoutPrintingIt() {
        var gate = gate();
        var reservation = reserve(gate, request(), now, 12345);

        assertEquals(validator().validate(request(), now).ticketId(), reservation.ticketId());
        assertEquals(new InetSocketAddress("127.0.0.1", 23450), reservation.tuple());
        assertEquals(12345, reservation.acceptedNanos());
        assertFalse(reservation.toString().contains(token), "a ticket must never reach a log");
    }

    @Test
    void letsAReservationBecomeReadyOnlyOnce() {
        // A used token must not allocate a second peer
        var gate = gate();
        var reservation = reserve(gate, request(), now, 0);

        assertTrue(gate.ready(reservation));
        assertFalse(gate.ready(reservation));
        assertEquals(1, gate.stats().accepted());
    }

    @Test
    void refusesToReadyAReservationThatTimedOut() {
        var gate = gate();
        var reservation = reserve(gate, request(), now, 0);

        assertEquals(List.of(reservation), gate.sweep(now, 2_000_000_000L));
        assertFalse(gate.ready(reservation), "a handshake that ran out of time cannot finish later");
        assertNull(gate.admission(reservation));
    }

    @Test
    void finishesAReservationOnlyOnce() {
        var gate = gate();
        var reservation = reserve(gate, request(), now, 0);

        assertTrue(gate.finish(reservation));
        assertFalse(gate.finish(reservation), "a second teardown must not free capacity twice");
        assertNull(gate.admission(reservation));
        assertFalse(gate.ready(reservation));
    }

    @Test
    void invalidTokensNeverReserveCapacity() {
        var gate = gate();
        for (int i = 0; i < 1000; i++) {
            assertNull(reserve(gate, request("invalidToken", remote), now, 0));
        }
        assertEquals(0, gate.stats().sessions());
        assertEquals(0, gate.stats().claims());
        assertEquals(1000, gate.stats().invalid());
    }

    @Test
    void concurrentAttemptsShareOneTokenReservation() throws Exception {
        var gate = gate();
        var executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<TicketLedger.Reservation>> calls = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                calls.add(() -> reserve(gate, request(), now, 0));
            }
            int reserved = 0;
            for (var result : executor.invokeAll(calls)) {
                if (result.get() != null) {
                    reserved++;
                }
            }
            assertEquals(1, reserved);
        } finally {
            executor.shutdown();
        }
        assertEquals(1, gate.stats().sessions());
        assertNull(reserve(gate, elsewhere(), now, 0));
    }

    @Test
    void nativeVerificationFailureDoesNotConsumeToken() {
        var gate = gate();
        var forged = reserve(gate, request(), now, 0);
        assertNotNull(forged);
        assertEquals(0, gate.stats().accepted());
        gate.invalidNativeRequest();
        assertTrue(gate.finish(forged));
        assertEquals(0, gate.stats().claims());
        assertEquals(0, gate.stats().sessions());
        var legitimate = reserve(gate, elsewhere(), now, 1);
        assertNotNull(legitimate);
        assertTrue(gate.ready(legitimate));
        assertFalse(gate.ready(legitimate));
        assertEquals(1, gate.stats().accepted());
    }

    @Test
    void acceptedTokensCannotAllocateAgainAndActiveSessionsOutliveTokenExpiry() {
        var gate = gate();
        var r = reserve(gate, request(), now, 0);
        assertTrue(gate.ready(r));
        gate.connected(r);
        gate.verifier(r).acceptForwardedIdentity();
        assertNull(reserve(gate, elsewhere(), now, 0));
        assertEquals(1, gate.stats().replayRejected());
        assertTrue(gate.sweep(now + 120_000, 120_000_000_000L).isEmpty());
        assertNotNull(gate.admission(r));
        assertTrue(gate.finish(r));
        assertNull(gate.admission(r));
        assertNull(reserve(gate, request(), now, 0));
        assertEquals(1, gate.stats().claims());
        gate.sweep(now + 120_000, 120_000_000_000L);
        assertEquals(0, gate.stats().claims());
    }

    @Test
    void timeoutAndShutdownKeepCapacityUntilNativeTeardownCompletes() {
        var gate = gate();
        var r = reserve(gate, request(), now, 0);
        assertTrue(gate.ready(r));
        assertEquals(List.of(r), gate.sweep(now + 1000, 1_000_000_000L));
        assertTrue(gate.sweep(now + 2000, 2_000_000_000L).isEmpty());
        assertNull(gate.admission(r));
        assertEquals(1, gate.stats().sessions());
        assertTrue(gate.finish(r));
        assertEquals(0, gate.stats().sessions());
        gate = gate();
        r = reserve(gate, request(), now, 0);
        assertEquals(List.of(r), gate.close());
        assertFalse(gate.ready(r));
        assertNull(gate.admission(r));
        assertEquals(1, gate.stats().sessions());
        assertNull(reserve(gate, request(), now, 0));
        assertTrue(gate.finish(r));
        assertEquals(0, gate.stats().sessions());
        assertEquals(0, gate.stats().claims());
    }

    @Test
    void closingHandsBackForClosingOnlyTheSessionsWhoseLoginWasNotAccepted() throws Exception {
        var gate = gate();
        var player = reserve(gate, request(), now, 0);
        assertTrue(gate.ready(player));
        gate.verifier(player).acceptForwardedIdentity();
        var undecided = reserve(gate, new AdmissionRequest(anotherToken(), remote, other), now, 0);
        assertTrue(gate.ready(undecided));

        assertEquals(List.of(undecided), gate.close());
        assertTrue(gate.verifier(undecided).rejected(), "its login can no longer be accepted");
    }

    @Test
    void drainingRejectsNewReservations() {
        var gate = gate();
        assertTrue(gate.isServing());
        gate.drain();
        assertFalse(gate.isServing());
        assertNull(reserve(gate, request(), now, 0));
        assertEquals(0, gate.stats().claims());
        assertEquals(1, gate.stats().capacityRejected());
    }

    @Test
    void anAssistedReservationIsKeptByItsTicketAlone() throws Exception {
        // An assisted join knows only the candidates the client claims, which must not block a tuple
        var gate = gate();
        var assisted = validator().validate(request(), now);
        assertNotNull(gate.reserve(assisted, null, 0, now, 0));
        assertNull(gate.reserve(validator().validate(request(), now), null, 0, now, 0),
                "the same ticket twice is a replay");

        assertNotNull(reserve(gate, request(anotherToken(), remote), now, 0),
                "a stateless ticket from the claimed address still gets in");
        assertEquals(2, gate.stats().sessions());
    }

    @Test
    void aDiagnosticAttemptRunsOnceUntilItsPermitExpires() {
        var gate = gate();
        String attempt = "0123456789abcdef0123456789abcdef";
        var record = gate.recordDiagnostic(attempt, now + 30_000, now);
        assertNotNull(record);
        assertNull(gate.recordDiagnostic(attempt, now + 30_000, now), "a second permit for the attempt is a replay");
        gate.finishDiagnostic(record, true);
        assertNull(gate.recordDiagnostic(attempt, now + 30_000, now + 29_999), "its peer used the attempt up");
        assertEquals(List.of(), gate.sweep(now + 30_000, 0));
        assertEquals(0, gate.stats().diagnostics(), "the record goes with the permit");
        assertEquals(0, gate.stats().sessions(), "a diagnostic is no player");
        assertNotNull(gate.recordDiagnostic(attempt, now + 60_000, now + 30_000));
    }

    @Test
    void aDiagnosticAttemptThatGotNoPeerCanRunAgain() {
        var gate = gate();
        String attempt = "fedcba9876543210fedcba9876543210";
        var refused = gate.recordDiagnostic(attempt, now + 30_000, now);
        gate.finishDiagnostic(refused, false);
        var record = gate.recordDiagnostic(attempt, now + 30_000, now);
        assertNotNull(record);
        gate.finishDiagnostic(refused, false);
        assertNull(gate.recordDiagnostic(attempt, now + 30_000, now), "an old record cannot release a newer one");
    }

    @Test
    void diagnosticRecordsAreBoundedAndEndWithTheLedger() {
        var gate = gate();
        for (int i = 0; i < TicketLedger.MAX_DIAGNOSTIC_RECORDS; i++) {
            assertNotNull(gate.recordDiagnostic("%032x".formatted(i), now + 30_000, now));
        }
        assertNull(gate.recordDiagnostic("%032x".formatted(99), now + 30_000, now), "the history is full");
        assertEquals(1, gate.stats().capacityRejected());

        var other = gate();
        other.close();
        assertNull(other.recordDiagnostic("%032x".formatted(1), now + 30_000, now));
    }
}

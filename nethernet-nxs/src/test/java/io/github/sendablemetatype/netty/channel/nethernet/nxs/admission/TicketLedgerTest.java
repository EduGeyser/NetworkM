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
        return new TicketLedger(new TicketLedger.Limits(2, 1000), validator());
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
        assertNotNull(gate.reserve(request(), now, 0));

        assertNull(gate.reserve(request(anotherToken(), remote), now, 0));
        assertEquals(1, gate.stats().replayRejected());
        assertEquals(1, gate.stats().sessions());
    }

    @Test
    void refusesWhenTheClaimsItRemembersAreFull() throws Exception {
        // Sessions are free, but every token it has seen is still remembered
        var gate = new TicketLedger(new TicketLedger.Limits(2, 1000), validator());
        var first = gate.reserve(request(), now, 0);
        assertNotNull(first);
        assertTrue(gate.ready(first));
        gate.finish(first);

        assertNotNull(gate.reserve(request(anotherToken(), remote), now, 0));
        assertNull(gate.reserve(new AdmissionRequest(anotherToken(), remote, other), now, 0));
        assertEquals(1, gate.stats().capacityRejected());
    }

    @Test
    void tellsTheCallerWhatItReservedWithoutPrintingIt() {
        var gate = gate();
        var reservation = gate.reserve(request(), now, 12345);

        assertEquals(validator().validate(request(), now).ticketId(), reservation.ticketId());
        assertEquals(new InetSocketAddress("127.0.0.1", 23450), reservation.tuple());
        assertEquals(12345, reservation.acceptedNanos());
        assertFalse(reservation.toString().contains(token), "a ticket must never reach a log");
    }

    @Test
    void letsAReservationBecomeReadyOnlyOnce() {
        // A used token must not allocate a second peer
        var gate = gate();
        var reservation = gate.reserve(request(), now, 0);

        assertTrue(gate.ready(reservation));
        assertFalse(gate.ready(reservation));
        assertEquals(1, gate.stats().accepted());
    }

    @Test
    void refusesToReadyAReservationThatTimedOut() {
        var gate = gate();
        var reservation = gate.reserve(request(), now, 0);

        assertEquals(List.of(reservation), gate.sweep(now, 2_000_000_000L));
        assertFalse(gate.ready(reservation), "a handshake that ran out of time cannot finish later");
        assertNull(gate.admission(reservation));
    }

    @Test
    void finishesAReservationOnlyOnce() {
        var gate = gate();
        var reservation = gate.reserve(request(), now, 0);

        assertTrue(gate.finish(reservation));
        assertFalse(gate.finish(reservation), "a second teardown must not free capacity twice");
        assertNull(gate.admission(reservation));
        assertFalse(gate.ready(reservation));
    }

    @Test
    void invalidTokensNeverReserveCapacity() {
        var gate = gate();
        for (int i = 0; i < 1000; i++) {
            assertNull(gate.reserve(request("invalidToken", remote), now, 0));
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
                calls.add(() -> gate.reserve(request(), now, 0));
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
        assertNull(gate.reserve(elsewhere(), now, 0));
    }

    @Test
    void nativeVerificationFailureDoesNotConsumeToken() {
        var gate = gate();
        var forged = gate.reserve(request(), now, 0);
        assertNotNull(forged);
        assertEquals(0, gate.stats().accepted());
        gate.invalidNativeRequest();
        assertTrue(gate.finish(forged));
        assertEquals(0, gate.stats().claims());
        assertEquals(0, gate.stats().sessions());
        var legitimate = gate.reserve(elsewhere(), now, 1);
        assertNotNull(legitimate);
        assertTrue(gate.ready(legitimate));
        assertFalse(gate.ready(legitimate));
        assertEquals(1, gate.stats().accepted());
    }

    @Test
    void acceptedTokensCannotAllocateAgainAndActiveSessionsOutliveTokenExpiry() {
        var gate = gate();
        var r = gate.reserve(request(), now, 0);
        assertTrue(gate.ready(r));
        gate.connected(r);
        gate.loginDecided(r, true);
        assertNull(gate.reserve(elsewhere(), now, 0));
        assertEquals(1, gate.stats().replayRejected());
        assertTrue(gate.sweep(now + 120_000, 120_000_000_000L).isEmpty());
        assertNotNull(gate.admission(r));
        assertTrue(gate.finish(r));
        assertNull(gate.admission(r));
        assertNull(gate.reserve(request(), now, 0));
        assertEquals(1, gate.stats().claims());
        gate.sweep(now + 120_000, 120_000_000_000L);
        assertEquals(0, gate.stats().claims());
    }

    @Test
    void timeoutAndShutdownKeepCapacityUntilNativeTeardownCompletes() {
        var gate = gate();
        var r = gate.reserve(request(), now, 0);
        assertTrue(gate.ready(r));
        assertEquals(List.of(r), gate.sweep(now + 1000, 1_000_000_000L));
        assertTrue(gate.sweep(now + 2000, 2_000_000_000L).isEmpty());
        assertNull(gate.admission(r));
        assertEquals(1, gate.stats().sessions());
        assertTrue(gate.finish(r));
        assertEquals(0, gate.stats().sessions());
        gate = gate();
        r = gate.reserve(request(), now, 0);
        assertEquals(List.of(r), gate.close());
        assertFalse(gate.ready(r));
        assertNull(gate.admission(r));
        assertEquals(1, gate.stats().sessions());
        assertNull(gate.reserve(request(), now, 0));
        assertTrue(gate.finish(r));
        assertEquals(0, gate.stats().sessions());
        assertEquals(0, gate.stats().claims());
    }

    @Test
    void drainingRejectsNewReservations() {
        var gate = gate();
        assertTrue(gate.isServing());
        gate.drain();
        assertFalse(gate.isServing());
        assertNull(gate.reserve(request(), now, 0));
        assertEquals(0, gate.stats().claims());
        assertEquals(1, gate.stats().capacityRejected());
    }

    @Test
    void anAssistedReservationIsKeptByItsTicketAlone() throws Exception {
        // An assisted join knows only the candidates the client claims, which must not block a tuple
        var gate = gate();
        var assisted = validator().validate(request(), now);
        assertNotNull(gate.reserveAuthenticated(assisted, null, now, 0));
        assertNull(gate.reserveAuthenticated(validator().validate(request(), now), null, now, 0),
                "the same ticket twice is a replay");

        assertNotNull(gate.reserve(request(anotherToken(), remote), now, 0),
                "a stateless ticket from the claimed address still gets in");
        assertEquals(2, gate.stats().sessions());
    }
}

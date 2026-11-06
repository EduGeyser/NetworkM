package io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.EndpointConnectivityController.*;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.EndpointSelection.*;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.EndpointSelectionTest.endpoint;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.EndpointSelectionTest.hint;
import static org.junit.jupiter.api.Assertions.*;

class EndpointConnectivityControllerTest {
    static final Map<AddressFamily, InetSocketAddress> SERVERS =
            Map.of(
                    AddressFamily.IPV4,
                    endpoint("1.1.1.1", 3478),
                    AddressFamily.IPV6,
                    endpoint("2606:4700:4700::1111", 3478));

    static final class FakeMonitor implements StunMonitor {
        final InetSocketAddress server;
        Optional<StunReading> sample = Optional.empty();
        boolean closed, fail;
        int closeFailures, closeCalls;
        Runnable beforeRead = () -> {};

        FakeMonitor(InetSocketAddress server) {
            this.server = server;
        }

        /** Before an answer, a monitor reports a pending transaction without a mapping. */
        public StunReading read() {
            if (closed || fail) {
                throw new IllegalStateException();
            }
            beforeRead.run();
            return sample.orElse(new StunReading(server, null, null, StunReading.State.PENDING, 0, 0, 0,
                    Optional.empty()));
        }

        public void close() {
            closeCalls++;
            int remainingFailures = closeFailures;
            closeFailures--;
            if (remainingFailures > 0) {
                throw new IllegalStateException("close failed");
            }
            closed = true;
        }

        void success(AddressFamily family, String ip, int port, long revision, long responses, long age) {
            sample =
                    Optional.of(
                            new StunReading(
                                    SERVERS.get(family),
                                    null,
                                    endpoint(ip, port),
                                    StunReading.State.SUCCEEDED,
                                    responses,
                                    0,
                                    revision,
                                    Optional.of(Duration.ofMillis(age))));
        }
    }

    static final class Fixture {
        final AtomicLong clock = new AtomicLong(1_000_000_000L);
        final Map<AddressFamily, FakeMonitor> monitors = new EnumMap<>(AddressFamily.class);
        int opens;
        final EndpointConnectivityController controller;

        Fixture(EndpointSelection selection) {
            controller =
                    new EndpointConnectivityController(
                            selection,
                            SERVERS,
                            Duration.ofSeconds(30),
                            server -> {
                                opens++;
                                var monitor = new FakeMonitor(server);
                                monitors.put(AddressFamily.of(server.getAddress()), monitor);
                                return monitor;
                            },
                            clock::get);
        }

        Snapshot snapshot() {
            return controller.snapshot();
        }

        State state(AddressFamily family) {
            return snapshot().families().get(family).state();
        }
    }

    static EndpointSelection automatic(List<Candidate> hints) {
        return select(endpoint("::", 19133), List.of(), hints);
    }

    @Test
    void explicitEndpointsDisableStunAcrossBothFamilies() {
        var f =
                new Fixture(
                        select(
                                endpoint("::", 19133),
                                List.of(endpoint("8.8.8.8", 40000)),
                                List.of()));
        assertEquals(State.CONFIGURED, f.state(AddressFamily.IPV4));
        assertEquals(State.DISABLED_BY_CONFIG, f.state(AddressFamily.IPV6));
        assertEquals(0, f.opens);
        f.controller.close();
    }

    @Test
    void publicLocalCandidatesSuppressStunForTheirFamilies() {
        var f =
                new Fixture(
                        automatic(
                                List.of(
                                        hint("8.8.8.8", Provenance.NATIVE_HOST),
                                        hint(
                                                "2606:4700:4700::1111",
                                                Provenance.SERVER_PROPERTIES))));
        assertEquals(State.PUBLIC_DIRECT, f.state(AddressFamily.IPV4));
        assertEquals(State.PUBLIC_DIRECT, f.state(AddressFamily.IPV6));
        assertEquals(0, f.opens);
        f.controller.close();
    }

    @Test
    void remapsRefreshesExpiryAndRecoveryPreserveIndependentFamilyState() {
        var f = new Fixture(automatic(List.of()));
        f.snapshot();
        assertEquals(2, f.opens);
        var v4 = f.monitors.get(AddressFamily.IPV4);
        var v6 = f.monitors.get(AddressFamily.IPV6);
        v4.success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 1, 0);
        v6.success(AddressFamily.IPV6, "2606:4700:4700::1111", 40000, 1, 1, 0);
        var first = f.snapshot();
        assertEquals(State.STUN_FRESH, first.families().get(AddressFamily.IPV4).state());
        v4.success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 2, 0);
        assertEquals(
                first.candidateRevision(),
                f.snapshot().candidateRevision(),
                "Unchanged keepalive is not a candidate change");
        v4.success(AddressFamily.IPV4, "8.8.8.8", 40001, 2, 3, 0);
        var remapped = f.snapshot();
        assertEquals(first.candidateRevision() + 1, remapped.candidateRevision());
        assertEquals(
                40001,
                remapped.families().get(AddressFamily.IPV4).freshStunEndpoint().orElseThrow().getPort());
        var observation = remapped.families().get(AddressFamily.IPV4).observation().orElseThrow();
        assertTrue(observation.freshAt(f.clock.get()));
        f.clock.addAndGet(Duration.ofSeconds(30).toNanos());
        assertFalse(
                observation.freshAt(f.clock.get()),
                "Retaining an old snapshot cannot extend its deadline");
        v4.success(AddressFamily.IPV4, "8.8.8.8", 40001, 2, 3, 30_000);
        var stale = f.snapshot();
        assertEquals(State.STUN_STALE, stale.families().get(AddressFamily.IPV4).state());
        assertTrue(stale.families().get(AddressFamily.IPV4).freshStunEndpoint().isEmpty());
        assertEquals(State.STUN_FRESH, stale.families().get(AddressFamily.IPV6).state());
        assertEquals(remapped.candidateRevision() + 1, stale.candidateRevision());
        v4.success(AddressFamily.IPV4, "8.8.8.8", 40001, 2, 4, 0);
        assertEquals(State.STUN_FRESH, f.state(AddressFamily.IPV4));
        assertEquals(2, f.opens, "Native retries keep the existing monitor");
        f.controller.close();
        assertTrue(v4.closed && v6.closed);
        assertTrue(
                f.snapshot().families().values().stream()
                        .allMatch(
                                s ->
                                        s.state() == State.CLOSED
                                                && s.directCandidates().isEmpty()
                                                && s.freshStunEndpoint().isEmpty()));
        assertEquals(2, f.opens);
    }

    @Test
    void replacementEpochWithdrawsOldStateAndFailuresNeverSpinNewMonitors() {
        var f = new Fixture(automatic(List.of()));
        f.snapshot();
        var first = f.monitors.get(AddressFamily.IPV4);
        first.success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 1, 0);
        long epoch =
                f.snapshot().families().get(AddressFamily.IPV4).observation().orElseThrow().monitorEpoch();
        f.controller.replaceStunServer(AddressFamily.IPV4, SERVERS.get(AddressFamily.IPV4));
        assertTrue(first.closed);
        assertTrue(f.snapshot().families().get(AddressFamily.IPV4).freshStunEndpoint().isEmpty());
        var second = f.monitors.get(AddressFamily.IPV4);
        second.success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 1, 0);
        assertEquals(
                epoch + 1,
                f.snapshot()
                        .families()
                        .get(AddressFamily.IPV4)
                        .observation()
                        .orElseThrow()
                        .monitorEpoch());
        second.fail = true;
        assertEquals(State.MONITOR_FAILED, f.state(AddressFamily.IPV4));
        assertTrue(second.closed);
        assertEquals(State.MONITOR_FAILED, f.state(AddressFamily.IPV4));
        assertEquals(3, f.opens);
        assertThrows(
                IllegalArgumentException.class,
                () -> f.controller.replaceStunServer(AddressFamily.IPV4, SERVERS.get(AddressFamily.IPV6)));
        f.controller.close();
    }

    @Test
    void aFailedMonitorIsOpenedAgainAfterTheRetryDelay() {
        var f = new Fixture(automatic(List.of()));
        f.snapshot();
        var first = f.monitors.get(AddressFamily.IPV4);
        first.success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 1, 0);
        assertEquals(State.STUN_FRESH, f.state(AddressFamily.IPV4));

        first.fail = true;
        assertEquals(State.MONITOR_FAILED, f.state(AddressFamily.IPV4));
        f.clock.addAndGet(StunObservation.RETRY_DELAY.toNanos() - 1);
        assertEquals(State.MONITOR_FAILED, f.state(AddressFamily.IPV4));
        assertEquals(2, f.opens, "No new monitor before the delay");

        f.clock.addAndGet(1);
        assertEquals(State.STUN_PENDING, f.state(AddressFamily.IPV4));
        assertEquals(3, f.opens, "One once it is over, so STUN does not stay down for good");
        f.monitors.get(AddressFamily.IPV4).success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 1, 0);
        assertEquals(State.STUN_FRESH, f.state(AddressFamily.IPV4));
        f.controller.close();
    }

    @Test
    void privateMappingsAndWrongSourcesNeverBecomePublicCandidates() {
        var f = new Fixture(automatic(List.of()));
        f.snapshot();
        var monitor = f.monitors.get(AddressFamily.IPV4);
        monitor.success(AddressFamily.IPV4, "192.168.1.10", 40000, 1, 1, 0);
        assertEquals(State.STUN_INELIGIBLE, f.state(AddressFamily.IPV4));
        assertTrue(f.snapshot().families().get(AddressFamily.IPV4).freshStunEndpoint().isEmpty());
        monitor.sample =
                Optional.of(
                        new StunReading(
                                SERVERS.get(AddressFamily.IPV6),
                                null,
                                endpoint("8.8.8.8", 40000),
                                StunReading.State.SUCCEEDED,
                                1,
                                0,
                                1,
                                Optional.of(Duration.ZERO)));
        assertEquals(State.MONITOR_FAILED, f.state(AddressFamily.IPV4));
        f.controller.close();
    }

    @Test
    void unsupportedFamilyNeverCreatesAnIndependentSocket() {
        var f = new Fixture(select(endpoint("0.0.0.0", 19133), List.of(), List.of()));
        assertEquals(State.UNSUPPORTED_FAMILY, f.state(AddressFamily.IPV6));
        assertEquals(1, f.opens);
        f.controller.close();
    }

    @Test
    void failedRefreshRetainsHistoricalObservationOnlyUntilItsOriginalDeadline() {
        var f = new Fixture(automatic(List.of()));
        f.snapshot();
        var monitor = f.monitors.get(AddressFamily.IPV4);
        monitor.sample =
                Optional.of(
                        new StunReading(
                                SERVERS.get(AddressFamily.IPV4),
                                null,
                                null,
                                StunReading.State.FAILED,
                                0,
                                1,
                                0,
                                Optional.empty()));
        assertEquals(State.STUN_FAILED, f.state(AddressFamily.IPV4));
        monitor.sample =
                Optional.of(
                        new StunReading(
                                SERVERS.get(AddressFamily.IPV4),
                                null,
                                endpoint("8.8.8.8", 40000),
                                StunReading.State.FAILED,
                                1,
                                2,
                                1,
                                Optional.of(Duration.ofSeconds(29))));
        var warm = f.snapshot().families().get(AddressFamily.IPV4);
        assertEquals(State.STUN_FRESH, warm.state());
        assertEquals(StunReading.State.FAILED, warm.observation().orElseThrow().transactionState());
        monitor.beforeRead = () -> f.clock.addAndGet(Duration.ofSeconds(2).toNanos());
        var delayed = f.snapshot().families().get(AddressFamily.IPV4);
        assertEquals(
                State.STUN_STALE,
                delayed.state(),
                "A delayed native read consumes the remaining observation lifetime");
        assertTrue(delayed.freshStunEndpoint().isEmpty());
        assertEquals(
                2,
                f.opens,
                "Native transaction failure must not create fresh monitors or reset native age");
        f.controller.close();
    }

    @Test
    void failedCloseRetainsOwnedHandleWithoutFreshEndpointOrDuplicateMonitor() {
        var f = new Fixture(automatic(List.of()));
        f.snapshot();
        var monitor = f.monitors.get(AddressFamily.IPV4);
        monitor.success(AddressFamily.IPV4, "8.8.8.8", 40000, 1, 1, 0);
        assertTrue(f.snapshot().families().get(AddressFamily.IPV4).freshStunEndpoint().isPresent());
        monitor.fail = true;
        monitor.closeFailures = 3;
        assertEquals(State.MONITOR_FAILED, f.state(AddressFamily.IPV4));
        assertTrue(f.snapshot().families().get(AddressFamily.IPV4).freshStunEndpoint().isEmpty());
        assertEquals(1, monitor.closeCalls, "Ordinary snapshots do not spin a failed close");
        assertThrows(
                IllegalStateException.class,
                () -> f.controller.replaceStunServer(AddressFamily.IPV4, SERVERS.get(AddressFamily.IPV4)));
        assertEquals(
                2, f.opens, "Replacement cannot open while its previous handle failed to close");
        assertThrows(IllegalStateException.class, f.controller::close);
        assertFalse(monitor.closed);
        assertTrue(
                f.snapshot().families().values().stream()
                        .allMatch(
                                s -> s.state() == State.CLOSED && s.freshStunEndpoint().isEmpty()));
        f.controller.close();
        assertTrue(monitor.closed);
        assertEquals(4, monitor.closeCalls);
        assertEquals(2, f.opens);
    }
}

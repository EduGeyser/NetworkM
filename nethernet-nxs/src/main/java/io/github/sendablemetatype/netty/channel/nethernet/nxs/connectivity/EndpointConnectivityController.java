package io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/**
 * STUN observation for the families that need it, one monitor per family on the gameplay port. Monitors send and
 * refresh on their own; {@link #snapshot()} is polled periodically to notice remaps and expiry. It emits no HTTP,
 * no provider profile and no reachability verdict. All operations are serialized, including monitor replacement
 * and close. The owner stops it before its listener and never uses a snapshot past its freshness deadline.
 */
public final class EndpointConnectivityController implements AutoCloseable {
    public enum State {
        CONFIGURED,
        DISABLED_BY_CONFIG,
        UNSUPPORTED_FAMILY,
        PUBLIC_DIRECT,
        STUN_NOT_CONFIGURED,
        STUN_PENDING,
        STUN_FAILED,
        STUN_FRESH,
        STUN_INELIGIBLE,
        STUN_STALE,
        MONITOR_FAILED,
        CLOSED
    }

    /**
     * @param lastSuccessAgeMillis -1 before the first answer
     * @param freshUntilNanos      On the controller's clock; freshness is never a reachability verdict
     */
    public record Observation(
            InetSocketAddress server,
            InetSocketAddress mapped,
            StunReading.State transactionState,
            long monitorEpoch,
            long mappingRevision,
            long successfulResponses,
            long failedTransactions,
            long lastSuccessAgeMillis,
            long freshUntilNanos) {
        public boolean freshAt(long nowNanos) {
            return lastSuccessAgeMillis >= 0 && nowNanos - freshUntilNanos < 0;
        }
    }

    public record FamilySnapshot(
            State state,
            List<EndpointSelection.Candidate> directCandidates,
            Optional<Observation> observation,
            Optional<InetSocketAddress> freshStunEndpoint) {
        public FamilySnapshot {
            directCandidates = List.copyOf(directCandidates);
        }
    }

    /** The revision changes only when the available endpoint set changes, not on unchanged STUN refreshes. */
    public record Snapshot(long candidateRevision, Map<AddressFamily, FamilySnapshot> families) {
        public Snapshot {
            families = Map.copyOf(families);
        }
    }

    private static final class Lane {
        final List<EndpointSelection.Candidate> direct;
        StunObservation observation;
        InetSocketAddress fresh;

        Lane(List<EndpointSelection.Candidate> direct) {
            this.direct = direct;
        }
    }

    private final EndpointSelection selection;
    private final StunMonitor.Factory factory;
    private final LongSupplier nanoTime;
    private final Duration maxAge;
    private final EnumMap<AddressFamily, Lane> lanes = new EnumMap<>(AddressFamily.class);
    private long candidateRevision = 1;
    private boolean closed;

    /**
     * @param numericServers    The STUN server per family, numeric; families without one get no monitor
     * @param maxObservationAge How long a mapping counts after its last answer, at most five minutes
     * @param factory           Opens monitors on the listener's exact bind address and port, never a utility socket
     */
    public EndpointConnectivityController(EndpointSelection selection, Map<AddressFamily, InetSocketAddress> numericServers,
                                          Duration maxObservationAge, StunMonitor.Factory factory) {
        this(selection, numericServers, maxObservationAge, factory, System::nanoTime);
    }

    EndpointConnectivityController(EndpointSelection selection, Map<AddressFamily, InetSocketAddress> numericServers,
                                   Duration maxObservationAge, StunMonitor.Factory factory, LongSupplier nanoTime) {
        this.selection = Objects.requireNonNull(selection);
        this.factory = Objects.requireNonNull(factory);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        Objects.requireNonNull(numericServers);
        if (maxObservationAge.isNegative() || maxObservationAge.isZero()
                || maxObservationAge.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException("STUN observation age must be positive and at most five minutes");
        }
        maxAge = maxObservationAge;
        numericServers.forEach(EndpointConnectivityController::checkServer);
        for (AddressFamily family : AddressFamily.values()) {
            Lane lane = new Lane(selection.configured() ? selection.candidates(family) : selection.publicCandidates(family));
            InetSocketAddress server = numericServers.get(family);
            if (server != null) {
                lane.observation = observation(server);
            }
            lanes.put(family, lane);
        }
    }

    /** Uses another server for a family. The old mapping is withdrawn at once. */
    public synchronized void replaceStunServer(AddressFamily family, InetSocketAddress numericServer) {
        requireOpen();
        checkServer(family, numericServer);
        Lane lane = lanes.get(family);
        setFresh(lane, null);
        if (lane.observation == null) {
            lane.observation = observation(numericServer);
        } else {
            lane.observation.replace(numericServer);
        }
    }

    public synchronized Snapshot snapshot() {
        EnumMap<AddressFamily, FamilySnapshot> result = new EnumMap<>(AddressFamily.class);
        List<StunObservation> polled = new ArrayList<>();
        for (AddressFamily family : AddressFamily.values()) {
            Lane lane = lanes.get(family);
            State inactive = inactive(family, lane);
            if (inactive != null) {
                result.put(family, view(lane, inactive, null));
            } else {
                result.put(family, sample(lane));
                polled.add(lane.observation);
            }
        }
        // A family that STUN cannot serve only matters while the other has no public mapping either
        boolean covered = lanes.values().stream().anyMatch(lane -> lane.fresh != null);
        for (StunObservation observation : polled) {
            observation.report(covered);
        }
        return new Snapshot(candidateRevision, result);
    }

    /** Why a family has no observation to sample, or null when it has one. */
    private State inactive(AddressFamily family, Lane lane) {
        if (closed) {
            return State.CLOSED;
        }
        if (selection.configured()) {
            return lane.direct.isEmpty() ? State.DISABLED_BY_CONFIG : State.CONFIGURED;
        }
        if (!selection.socketFamilies().contains(family)) {
            return State.UNSUPPORTED_FAMILY;
        }
        if (!lane.direct.isEmpty()) {
            return State.PUBLIC_DIRECT;
        }
        if (lane.observation == null) {
            return State.STUN_NOT_CONFIGURED;
        }
        return null;
    }

    private FamilySnapshot sample(Lane lane) {
        StunObservation observation = lane.observation;
        observation.poll();
        if (observation.failed()) {
            setFresh(lane, null);
            return view(lane, State.MONITOR_FAILED, null);
        }
        Optional<StunObservation.Observed> latest = observation.observed();
        if (latest.isEmpty()) {
            setFresh(lane, null);
            return view(lane, State.STUN_PENDING, null);
        }
        StunObservation.Observed observed = latest.get();
        StunReading reading = observed.reading();
        boolean answered = reading.answered();
        Observation view = new Observation(
                reading.server(),
                reading.mapped(),
                reading.state(),
                observation.epoch(),
                reading.mappingRevision(),
                reading.successfulResponses(),
                reading.failedTransactions(),
                answered ? ageMillis(reading) : -1,
                observed.freshUntilNanos());
        setFresh(lane, observed.fresh() && observed.usable() ? reading.mapped() : null);
        return view(lane, state(answered, reading.state(), observed.fresh(), observed.usable()), view);
    }

    private static State state(boolean answered, StunReading.State transaction, boolean fresh, boolean usable) {
        if (!answered) {
            return transaction == StunReading.State.FAILED ? State.STUN_FAILED : State.STUN_PENDING;
        }
        if (!fresh) {
            return State.STUN_STALE;
        }
        return usable ? State.STUN_FRESH : State.STUN_INELIGIBLE;
    }

    private FamilySnapshot view(Lane lane, State state, Observation observation) {
        return new FamilySnapshot(state, closed ? List.of() : lane.direct, Optional.ofNullable(observation),
                Optional.ofNullable(lane.fresh));
    }

    private StunObservation observation(InetSocketAddress server) {
        return new StunObservation(server, maxAge, selection.bind().getPort(), factory, nanoTime);
    }

    private void setFresh(Lane lane, InetSocketAddress fresh) {
        if (!Objects.equals(lane.fresh, fresh)) {
            candidateRevision = Math.incrementExact(candidateRevision);
            lane.fresh = fresh;
        }
    }

    private static long ageMillis(StunReading reading) {
        try {
            return reading.lastSuccessAge().orElseThrow().toMillis();
        } catch (ArithmeticException tooOld) {
            return Long.MAX_VALUE;
        }
    }

    private static void checkServer(AddressFamily family, InetSocketAddress server) {
        Objects.requireNonNull(family);
        EndpointSelection.requireEndpoint(server);
        if (AddressFamily.of(server.getAddress()) != family || server.getAddress().isAnyLocalAddress()
                || server.getAddress().isMulticastAddress()) {
            throw new IllegalArgumentException("Numeric same-family STUN server required");
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Connectivity controller closed");
        }
    }

    /** Closing again retries a monitor that failed to close. */
    @Override
    public synchronized void close() {
        if (!closed) {
            candidateRevision = Math.incrementExact(candidateRevision);
        }
        closed = true;
        RuntimeException failure = null;
        for (Lane lane : lanes.values()) {
            setFresh(lane, null);
            if (lane.observation == null) {
                continue;
            }
            try {
                lane.observation.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}

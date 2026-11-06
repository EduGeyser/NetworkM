package io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.CandidateMethod;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ConnectivityCheck;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ConnectivityOutcome;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ProfileReplacedException;
import io.netty.util.NetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * The host's publication policy for maintained candidates. It publishes the fresh STUN mapping as {@code srflx},
 * plus a twin on the gameplay port for routers that move the port outbound but forward the gameplay port itself.
 * Public endpoints that failed regional checks are withheld from players unless another region reached them, but
 * stay probe targets so they can recover without a restart. Without a public address, or after such a failure,
 * the private addresses are offered for players on the same network; with a mapping, the private address the host
 * routes to its STUN server from is offered after the public ones.
 *
 * <p>Candidates come in preference order within each type: public host candidates, then private ones; the mapping
 * before its twin. The profile encoder ranks by type first and by this order second.
 */
public final class MaintainedCandidatePublisher implements AutoCloseable {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(MaintainedCandidatePublisher.class);

    /**
     * One refresh's result.
     *
     * @param mappingRevision  Changes when a family's STUN mapping or its monitor changes
     * @param candidates       What players are offered, at most 32, in preference order
     * @param probeCandidates  Where connection checks may test, at most 32, including withheld endpoints
     * @param assistedFamilies The families assisted joins may serve
     * @param guard            Holds while the publisher is open, the mapping revision unchanged and every mapping fresh
     */
    public record Publication(long mappingRevision, List<HostCandidate> candidates, List<HostCandidate> probeCandidates,
                              Set<AddressFamily> assistedFamilies, OwnershipGuard guard) {
        public Publication {
            candidates = List.copyOf(candidates);
            probeCandidates = List.copyOf(probeCandidates);
            assistedFamilies = Set.copyOf(assistedFamilies);
            Objects.requireNonNull(guard);
        }
    }

    private record Mapping(InetSocketAddress endpoint, long epoch, long revision) {
    }

    private record CheckKey(String region, CandidateMethod method, InetSocketAddress target) {
    }

    private final EndpointSelection selection;
    private final EndpointConnectivityController controller;
    private final LongSupplier nanoTime;
    private final boolean assistedJoins;
    private final Function<InetSocketAddress, Optional<InetAddress>> routeSource;
    private final Map<AddressFamily, Mapping> mappings = new EnumMap<>(AddressFamily.class);
    private final Map<CheckKey, ConnectivityCheck> checks = new HashMap<>();
    private final Map<AddressFamily, InetSocketAddress> servers = new EnumMap<>(AddressFamily.class);
    /** The private listener address each family reaches its STUN server from, for players on this network. */
    private final Map<AddressFamily, InetSocketAddress> lan = new EnumMap<>(AddressFamily.class);
    private final Set<AddressFamily> assistedFamilies;
    private volatile long mappingRevision = 1;
    private volatile boolean closed;

    /**
     * @param controller    The STUN observation, or null for configured endpoints and for assistance without warming
     * @param assistedJoins Whether assisted joins are on, which keeps failed public candidates published
     */
    public MaintainedCandidatePublisher(EndpointSelection selection, EndpointConnectivityController controller,
                                        boolean assistedJoins) {
        this(selection, controller, assistedJoins, System::nanoTime, MaintainedCandidatePublisher::routeSource);
    }

    MaintainedCandidatePublisher(EndpointSelection selection, EndpointConnectivityController controller,
                                 boolean assistedJoins, LongSupplier nanoTime,
                                 Function<InetSocketAddress, Optional<InetAddress>> routeSource) {
        this.selection = Objects.requireNonNull(selection);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        this.routeSource = Objects.requireNonNull(routeSource);
        this.assistedJoins = assistedJoins;
        if (selection.configured() && controller != null) {
            throw new IllegalArgumentException("Configured endpoints suppress STUN");
        }
        this.controller = controller;
        Set<AddressFamily> families = EnumSet.noneOf(AddressFamily.class);
        if (selection.configured()) {
            selection.candidates().stream()
                    .filter(c -> EndpointAddress.scope(c.endpoint().getAddress()) == EndpointAddress.Scope.PUBLIC)
                    .forEach(c -> families.add(AddressFamily.of(c.endpoint().getAddress())));
        } else {
            families.addAll(selection.socketFamilies());
        }
        assistedFamilies = Set.copyOf(families);
    }

    /** Whether a family without a public local address can use the provider's STUN servers. */
    public boolean needsStunServers() {
        return !selection.configured() && (controller != null || assistedJoins)
                && selection.socketFamilies().stream().anyMatch(f -> selection.publicCandidates(f).isEmpty());
    }

    /**
     * Points STUN discovery at numeric servers, one per family, for each family that has no public local
     * address. Does nothing when the operator configured the endpoints.
     */
    public synchronized void configureStunServers(Map<AddressFamily, InetSocketAddress> numericServers) {
        requireOpen();
        if (selection.configured()) {
            return;
        }
        numericServers.forEach((family, server) -> {
            if (selection.socketFamilies().contains(family) && selection.publicCandidates(family).isEmpty()
                    && !server.equals(servers.get(family))) {
                if (controller != null) {
                    controller.replaceStunServer(family, server);
                }
                servers.put(family, server);
                updateLan(family, server);
            }
        });
    }

    private void updateLan(AddressFamily family, InetSocketAddress server) {
        InetSocketAddress address = routeSource.apply(server)
                .filter(a -> EndpointAddress.scope(a) == EndpointAddress.Scope.PRIVATE)
                .map(a -> new InetSocketAddress(a, selection.bind().getPort()))
                .filter(e -> selection.candidates(family).stream().anyMatch(c -> c.endpoint().equals(e)))
                .orElse(null);
        if (!Objects.equals(lan.get(family), address)) {
            if (address == null) {
                lan.remove(family);
            } else {
                lan.put(family, address);
                log.info("Offering {} to players on this host's network as well", NetUtil.toSocketAddressString(address));
            }
        }
    }

    /** The local address the system sends to the target from. Connecting a UDP socket sends nothing. */
    static Optional<InetAddress> routeSource(InetSocketAddress target) {
        try (var socket = new DatagramSocket()) {
            socket.connect(target);
            InetAddress local = socket.getLocalAddress();
            return local.isAnyLocalAddress() ? Optional.empty() : Optional.of(InetAddress.getByAddress(local.getAddress()));
        } catch (IOException | RuntimeException unroutable) {
            return Optional.empty();
        }
    }

    /** The STUN server per family an assisted join may gather its answer candidate through. */
    public synchronized Map<AddressFamily, InetSocketAddress> assistedStunServers() {
        requireOpen();
        Map<AddressFamily, InetSocketAddress> selected = new EnumMap<>(AddressFamily.class);
        servers.forEach((family, server) -> {
            if (assistedFamilies.contains(family)) {
                selected.put(family, server);
            }
        });
        return Map.copyOf(selected);
    }

    /** The candidates to publish now, from the selection and the fresh STUN readings. */
    public synchronized Publication refresh() {
        requireOpen();
        List<HostCandidate> direct = new ArrayList<>(), reflexive = new ArrayList<>(), local = new ArrayList<>();
        List<EndpointConnectivityController.Observation> observations = new ArrayList<>();
        Map<AddressFamily, Mapping> nextMappings = new EnumMap<>(AddressFamily.class);
        var snapshot = controller == null ? null : controller.snapshot();
        for (AddressFamily family : AddressFamily.values()) {
            List<EndpointSelection.Candidate> chosen = selection.configured()
                    ? selection.candidates(family)
                    : selection.publicCandidates(family);
            List<HostCandidate> familyProbes = new ArrayList<>();
            chosen.forEach(c -> familyProbes.add(HostCandidate.host(c.endpoint())));
            direct.addAll(familyProbes);
            var lane = snapshot == null ? null : snapshot.families().get(family);
            if (lane != null && lane.freshStunEndpoint().isPresent()) {
                var observation = lane.observation().orElseThrow();
                InetSocketAddress endpoint = lane.freshStunEndpoint().orElseThrow();
                Mapping mapping = new Mapping(endpoint, observation.monitorEpoch(), observation.mappingRevision());
                nextMappings.put(family, mapping);
                observations.add(observation);
                // The gameplay port too, for a router that moved the port on the way out; provider checks withdraw
                // it on their own where nothing forwards it
                for (InetSocketAddress announced : withGameplayPort(endpoint)) {
                    HostCandidate candidate = new HostCandidate(announced, CandidateType.SRFLX);
                    reflexive.add(candidate);
                    familyProbes.add(candidate);
                }
            }
            InetSocketAddress twin = twin(nextMappings.get(family));
            if (!Objects.equals(mappings.get(family), nextMappings.get(family))) {
                checks.entrySet().removeIf(e -> e.getValue().family() == family
                        && e.getKey().method() == CandidateMethod.WARM_STUN);
            }
            // A failing twin only withdraws itself; it says nothing about the family
            boolean failed = familyProbes.stream()
                    .filter(c -> !c.endpoint().equals(twin))
                    .anyMatch(c -> failed(c.endpoint()));
            if (!selection.configured() && (chosen.isEmpty() && !nextMappings.containsKey(family) || failed)) {
                selection.candidates(family).stream()
                        .filter(c -> EndpointAddress.scope(c.endpoint().getAddress()) == EndpointAddress.Scope.PRIVATE)
                        .forEach(c -> local.add(HostCandidate.host(c.endpoint())));
            } else if (lan.containsKey(family)) {
                // Players on this network reach it without looping through the router
                local.add(HostCandidate.host(lan.get(family)));
            }
        }
        if (!nextMappings.equals(mappings)) {
            mappingRevision = Math.incrementExact(mappingRevision);
        }
        mappings.clear();
        mappings.putAll(nextMappings);
        List<HostCandidate> probes = distinct(direct, reflexive);
        // Test destinations survive withdrawal from player offers. Private addresses stay useful on a LAN or VPN.
        List<HostCandidate> offered = new ArrayList<>(probes.stream()
                .filter(c -> assistedJoins || !failed(c.endpoint()))
                .toList());
        offered.addAll(local);
        checks.entrySet().removeIf(e -> probes.stream().noneMatch(c -> c.endpoint().equals(e.getKey().target())));
        long capturedRevision = mappingRevision;
        return new Publication(
                mappingRevision,
                distinct(offered, List.of()),
                probes.stream().limit(32).toList(),
                assistedFamilies,
                () -> {
                    if (closed) {
                        throw new IllegalStateException("Candidate publisher closed");
                    }
                    if (mappingRevision != capturedRevision
                            || observations.stream().anyMatch(o -> !o.freshAt(nanoTime.getAsLong()))) {
                        throw new ProfileReplacedException();
                    }
                });
    }

    /**
     * Records regional results. Only current, public, directly tested endpoints affect offers; a check never turns
     * assistance off.
     */
    public synchronized void reportDirectChecks(List<ConnectivityCheck> reports, long nowMillis) {
        requireOpen();
        for (ConnectivityCheck check : reports) {
            if (check.target() == null
                    || check.checkedAt() > nowMillis
                    || check.expiresAt() <= nowMillis
                    || check.method() == CandidateMethod.PER_JOIN
                    || check.outcome() == ConnectivityOutcome.UNKNOWN
                    || check.outcome() == ConnectivityOutcome.UNAVAILABLE
                    || EndpointAddress.scope(check.target().getAddress()) != EndpointAddress.Scope.PUBLIC) {
                continue;
            }
            Mapping mapping = mappings.get(check.family());
            boolean matches = check.method() == CandidateMethod.WARM_STUN
                    ? mapping != null && (mapping.endpoint().equals(check.target()) || check.target().equals(twin(mapping)))
                    : selection.publicCandidates(check.family()).stream().anyMatch(c -> c.endpoint().equals(check.target()));
            CheckKey key = new CheckKey(check.region(), check.method(), check.target());
            ConnectivityCheck previous = checks.get(key);
            if (matches && (previous == null
                    || check.checkedAt() > previous.checkedAt()
                    || check.checkedAt() == previous.checkedAt() && check.outcome() == ConnectivityOutcome.NOT_ESTABLISHED)) {
                checks.put(key, check);
            }
        }
    }

    /**
     * A successful region proves a usable path even when another region fails. The latest result per region
     * survives feedback expiry; only another result or a replaced endpoint changes it.
     */
    private boolean failed(InetSocketAddress endpoint) {
        var matching = checks.values().stream().filter(c -> endpoint.equals(c.target())).toList();
        return matching.stream().anyMatch(c -> c.outcome() == ConnectivityOutcome.NOT_ESTABLISHED)
                && matching.stream().noneMatch(c -> c.outcome() == ConnectivityOutcome.ESTABLISHED);
    }

    private List<InetSocketAddress> withGameplayPort(InetSocketAddress mapped) {
        InetSocketAddress twin = new InetSocketAddress(mapped.getAddress(), selection.bind().getPort());
        return twin.equals(mapped) ? List.of(mapped) : List.of(mapped, twin);
    }

    /** The mapping's address on the gameplay port, or null when the mapping already uses it. */
    private InetSocketAddress twin(Mapping mapping) {
        if (mapping == null || mapping.endpoint().getPort() == selection.bind().getPort()) {
            return null;
        }
        return new InetSocketAddress(mapping.endpoint().getAddress(), selection.bind().getPort());
    }

    private static List<HostCandidate> distinct(List<HostCandidate> first, List<HostCandidate> second) {
        LinkedHashSet<HostCandidate> all = new LinkedHashSet<>(first);
        all.addAll(second);
        return all.stream().limit(32).toList();
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Candidate publisher closed");
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (controller != null) {
            controller.close();
        }
    }
}

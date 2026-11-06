package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

/** A NetherNet endpoint as the client sees it, with every answer under the test's control. */
class FakeTransport implements ProviderTransport {
    static final String INCARNATION = "0123456789abcdef0123456789abcdef";
    static final DtlsFingerprint FINGERPRINT = DtlsFingerprint.of(new byte[]{
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11});

    final CompletableFuture<Void> closed = new CompletableFuture<>();
    final Queue<TransportOutcome> outcomes = new ConcurrentLinkedQueue<>();
    final List<List<AdmissionKey>> installs = new CopyOnWriteArrayList<>();
    final List<DiagnosticHostPolicy> policies = new CopyOnWriteArrayList<>();
    final List<List<ConnectivityCheck>> reports = new CopyOnWriteArrayList<>();
    final List<List<StunServer>> stunServers = new CopyOnWriteArrayList<>();
    volatile List<HostCandidate> candidates = List.of(HostCandidate.host(new InetSocketAddress("127.0.0.1", 19133)));
    /** Null for the same as the candidates. */
    volatile List<HostCandidate> probes;
    volatile Set<AddressFamily> assistedFamilies = Set.of();
    volatile long revision, version;
    volatile OwnershipGuard guard = OwnershipGuard.ALWAYS;
    volatile boolean refuseKeys, assisted, diagnostics, refuseCapture;
    volatile BiFunction<AssistedJoin, OwnershipGuard, CompletionStage<AssistedAnswer>> joins =
            (join, current) -> CompletableFuture.failedFuture(new IllegalStateException("No joins expected"));
    volatile int drains, captures;
    volatile boolean diagnosticsEnabled;
    private volatile String activeKeyId;

    @Override
    public CompletionStage<HostProfileSnapshot> captureHostProfile() {
        captures++;
        if (refuseCapture || activeKeyId == null || closed.isDone()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Endpoint unavailable"));
        }
        HostProfile profile = new HostProfile(candidates, FINGERPRINT, activeKeyId, 5000, 262144, INCARNATION);
        return CompletableFuture.completedFuture(new HostProfileSnapshot(profile,
                probes == null ? candidates : probes, assistedFamilies, revision, version, guard));
    }

    @Override
    public long candidatePublicationVersion() {
        return version;
    }

    @Override
    public CompletionStage<Void> installAdmissionKeys(List<AdmissionKey> keys) {
        if (refuseKeys) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unusable admission key"));
        }
        installs.add(List.copyOf(keys));
        activeKeyId = keys.isEmpty() ? null : keys.getLast().keyId();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public List<TransportOutcome> pollOutcomes(int maximum) {
        List<TransportOutcome> batch = new ArrayList<>();
        TransportOutcome outcome;
        while (batch.size() < maximum && (outcome = outcomes.poll()) != null) {
            batch.add(outcome);
        }
        return batch;
    }

    @Override
    public CompletionStage<Void> configureStunServers(List<StunServer> servers) {
        stunServers.add(servers);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> reportConnectivityChecks(long candidateRevision, List<ConnectivityCheck> checks) {
        reports.add(checks);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean supportsAssistedJoins() {
        return assisted;
    }

    @Override
    public CompletionStage<AssistedAnswer> assistedJoin(AssistedJoin join, long deadlineNanos, OwnershipGuard guard) {
        return joins.apply(join, guard);
    }

    @Override
    public boolean supportsDiagnosticAdmission() {
        return diagnostics;
    }

    @Override
    public CompletionStage<Void> configureDiagnostics(DiagnosticHostPolicy policy, long deadlineNanos) {
        policies.add(policy);
        diagnosticsEnabled = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> disableDiagnostics() {
        diagnosticsEnabled = false;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> drain() {
        drains++;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> close() {
        closed.complete(null);
        return closed;
    }
}

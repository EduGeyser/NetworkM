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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.*;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.*;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Check-ins. A heartbeat reports acceptance, capacity, the actual player count and, when it changed, the host profile
 * and installed admission keys; its reply carries the profile revision, readiness, key updates and the next
 * schedule. Changes to acceptance, the profile and key acknowledgements go out at once; listing and count changes no
 * sooner than the provider's minimum interval. A reply that needs another exchange, such as a delivered key, gets
 * one at once, up to three per heartbeat. Used on the lifecycle executor.
 */
final class Heartbeats {
    private static final long REFRESH_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int EXCHANGES = 3;

    /** What one exchange captured and sent, which its reply is applied against. */
    private static final class Round {
        HostProfileSnapshot snapshot;
        HostProfileSnapshot diagnosticSnapshot;
        Wire.PublishedProfile hostProfile;
        Health health;
        ServerStatus status;
        boolean accepting, sentLocation, sentProtocols, diagnosticsAdvertised;
        long sentMillis, sentNanos;
    }

    private final ClientRuntime runtime;
    private final Exchanger exchanger;
    private final KeyManager keys;
    private final Diagnostics diagnostics;
    private final Assistance assistance;
    private final Supplier<ServerStatus> statusSupplier;
    private final Supplier<Health> healthSupplier;
    private final ConnectivityLog connectivity;
    private final OperatorActions actions;
    private final Supplier<CompletableFuture<Void>> recover;
    private Map<String, ProtocolExtension> extensions = Map.of();

    private String profileRevision;
    private Wire.PublishedProfile lastProfile;
    /** The admission key of the last acknowledged profile, which is the provider's active epoch. */
    private String publishedKeyId;
    private HostProfileSnapshot publishedSnapshot;
    private long publishedRevision, publishedVersion, observedRevision, observedVersion, nextCandidateRefresh;
    /** Set with each acknowledged profile, cleared once the provider may no longer hand it out. */
    private volatile PublishedEndpoints published;

    private boolean scheduledCheckIns;
    private long minUpdateIntervalMillis = 1000;
    private long nextHeartbeat, nextStatusUpdate;
    /** Consecutive failures, which lengthen the wait before the next attempt. */
    private int failures;
    private Boolean lastGameOutcomes;
    private boolean locationReported, bedrockProtocolsReported;
    private ServerStatus lastStatus;
    private Health lastHealth;
    private Readiness lastReadiness = new Readiness(false, List.of());

    /** @param recover Settles an ambiguous WebSocket operation through ordinary recovery and reinstalls the keys */
    Heartbeats(ClientRuntime runtime, Exchanger exchanger, KeyManager keys, Diagnostics diagnostics,
               Assistance assistance, Supplier<ServerStatus> statusSupplier, Supplier<Health> healthSupplier,
               ConnectivityLog connectivity, OperatorActions actions, Supplier<CompletableFuture<Void>> recover) {
        this.runtime = runtime;
        this.exchanger = exchanger;
        this.keys = keys;
        this.diagnostics = diagnostics;
        this.assistance = assistance;
        this.statusSupplier = statusSupplier;
        this.healthSupplier = Objects.requireNonNull(healthSupplier);
        this.connectivity = connectivity;
        this.actions = actions;
        this.recover = recover;
    }

    Optional<PublishedEndpoints> published() {
        return Optional.ofNullable(published);
    }

    Readiness lastReadiness() {
        return lastReadiness;
    }

    void extensions(Map<String, ProtocolExtension> extensions) {
        this.extensions = extensions;
    }

    /** Stops offering the published endpoints, as a drain or stop does. */
    void forgetPublished() {
        published = null;
    }

    CompletableFuture<Optional<OperatorAction>> operatorAction() {
        return actions.current(runtime, exchanger);
    }

    CompletableFuture<Void> disableDiagnostics() {
        return diagnostics.disable();
    }

    /** Sends a heartbeat now, settling an ambiguous operation first. */
    CompletableFuture<Void> beat() {
        CompletableFuture<Void> settled = Async.done();
        if (runtime.keeper.state().pendingOperation() != null) {
            // An ambiguous send may have outlived its native mapping; neither replay it nor stall until a restart
            assistance.revoke();
            settled = diagnostics.disable()
                    .thenCompose(ignored -> recover.get())
                    .thenApply(ignored -> {
                        profileRevision = null;
                        lastProfile = null;
                        published = null;
                        return null;
                    });
        }
        return settled.thenCompose(ignored -> rounds(0));
    }

    private CompletableFuture<Void> rounds(int round) {
        if (round == EXCHANGES) {
            nextHeartbeat = runtime.clock.nanos() + TimeUnit.SECONDS.toNanos(1);
            return Async.done();
        }
        return exchange().handleAsync((again, failure) -> {
            if (failure == null) {
                return again ? rounds(round + 1) : Async.done();
            }
            if (!(Async.unwrap(failure) instanceof ProfileReplacedException)) {
                return CompletableFuture.<Void>failedFuture(failure);
            }
            return invalidatePublished().thenCompose(ignored -> {
                // An ambiguous carrier attempt must go through ordinary recovery before any fresh operation
                if (runtime.keeper.state().pendingOperation() != null) {
                    nextHeartbeat = runtime.clock.nanos() + TimeUnit.SECONDS.toNanos(1);
                    return Async.done();
                }
                return rounds(round + 1);
            });
        }, runtime.executor).thenCompose(next -> next);
    }

    /** One exchange; completes with whether another is needed at once. */
    private CompletableFuture<Boolean> exchange() {
        Round round = new Round();
        return capture(round)
                .thenCompose(ignored -> send(round))
                .thenCompose(reply -> apply(round, reply));
    }

    private CompletableFuture<Void> capture(Round round) {
        if (keys.activeKeyId() == null || runtime.draining) {
            return Async.done();
        }
        return Async.within(runtime.transport.captureHostProfile(), Duration.ofSeconds(10), runtime.executor,
                        "Host profile capture")
                .thenApply(snapshot -> {
                    snapshot.requireCurrent();
                    try {
                        if (assistance.update(snapshot)) {
                            lastProfile = null;
                        }
                        if (assistance.mode() && !runtime.transport.supportsAssistedJoins()) {
                            throw new IOException("Assisted transport unavailable");
                        }
                        if (diagnostics.enabled() && !runtime.transport.supportsDiagnosticAdmission()) {
                            throw new IOException("Diagnostic transport unavailable");
                        }
                    } catch (IOException unavailable) {
                        throw new java.io.UncheckedIOException(unavailable);
                    }
                    round.snapshot = snapshot;
                    if (diagnostics.enabled() && diagnostics.eligible(snapshot)) {
                        round.diagnosticSnapshot = snapshot;
                    }
                    return snapshot;
                })
                .thenCompose(snapshot -> diagnostics.enabled() && round.diagnosticSnapshot == null
                        ? diagnostics.disable()
                        : Async.done())
                .exceptionallyCompose(failure -> diagnostics.disable()
                        .thenCompose(ignored -> CompletableFuture.failedFuture(failure)));
    }

    private CompletableFuture<Wire.HeartbeatReply> send(Round round) {
        HostProfileSnapshot snapshot = round.snapshot;
        Wire.PublishedProfile profile = snapshot == null ? null : new Wire.PublishedProfile(snapshot.profile(),
                assistance.mode() ? snapshot.assistedFamilies() : Set.of());
        round.hostProfile = profile != null && !profile.equals(lastProfile) ? profile : null;
        String revision = round.hostProfile == null ? profileRevision : null;
        String active = keys.activeKeyId();
        // The active epoch changes only with a matching profile
        List<String> acknowledged = active != null && (round.hostProfile != null || active.equals(publishedKeyId))
                ? keys.acknowledged()
                : null;
        round.health = healthSupplier.get();
        round.accepting = round.health.acceptingPlayers() && active != null && !runtime.draining;
        round.diagnosticsAdvertised = round.diagnosticSnapshot != null && diagnostics.current(round.diagnosticSnapshot);
        WireCodec.ConnectivityReport report = null;
        CompletableFuture<Void> withdrawn = Async.done();
        if (snapshot != null && snapshot.candidateRevision() > 0) {
            snapshot.requireCurrent();
            List<HostCandidate> candidates = snapshot.profile().candidates();
            boolean reflexive = candidates.stream().anyMatch(c -> c.type() == CandidateType.SRFLX);
            report = new WireCodec.ConnectivityReport(round.diagnosticsAdvertised, snapshot.candidateRevision(),
                    snapshot.probeCandidates().equals(candidates) ? null : snapshot.probeCandidates(),
                    reflexive ? CandidateMethod.WARM_STUN : runtime.config.connectivityMethod());
        } else if (round.diagnosticSnapshot == null || diagnostics.installed()) {
            withdrawn = diagnostics.disable();
        }
        boolean gameOutcomes = runtime.config.gameOutcomes();
        boolean full = round.hostProfile != null;
        Software software = round.health.software() != null && (full || lastHealth == null
                || !round.health.software().equals(lastHealth.software())) ? round.health.software() : null;
        round.sentLocation = full || !locationReported;
        round.sentProtocols = runtime.config.bedrockProtocols() != null && (full || !bedrockProtocolsReported);
        try {
            round.status = statusSupplier == null ? null : statusSupplier.get();
            runtime.log.recovered(ProviderLog.Operation.SERVER_STATUS);
        } catch (RuntimeException failure) {
            runtime.log.failed(ProviderLog.Operation.SERVER_STATUS, ProviderFailures.describe(failure));
        }
        var request = new WireCodec.HeartbeatRequest(round.hostProfile, revision, acknowledged,
                runtime.keeper.state().keyRequestId(), round.accepting, extensions, report,
                round.health.capacity(), round.health.connectedPlayers(),
                full || !Objects.equals(lastGameOutcomes, gameOutcomes) ? gameOutcomes : null,
                software, round.sentLocation, runtime.config.location(),
                round.sentProtocols ? runtime.config.bedrockProtocols() : null, round.status);
        String body = WireCodec.heartbeat(request);
        OwnershipGuard guard = snapshot == null ? OwnershipGuard.ALWAYS : snapshot.guard();
        return withdrawn.thenCompose(ignored -> {
            round.sentMillis = runtime.clock.millis();
            round.sentNanos = runtime.clock.nanos();
            return exchanger.signed("heartbeat", body, UUID.randomUUID().toString(), guard);
        }).thenApply(json -> {
            try {
                return WireCodec.heartbeatReply(json);
            } catch (IOException invalid) {
                throw new java.io.UncheckedIOException(invalid);
            }
        });
    }

    private CompletableFuture<Boolean> apply(Round round, Wire.HeartbeatReply reply) {
        failures = 0;
        long successNanos = runtime.clock.nanos(), successMillis = runtime.clock.millis();
        boolean again = false;
        HostProfileSnapshot snapshot = round.snapshot, diagnostic = round.diagnosticSnapshot;
        CompletableFuture<Void> withdrawn = Async.done();
        if (snapshot != null) {
            try {
                snapshot.requireCurrent();
            } catch (ProfileReplacedException replaced) {
                // The operation committed: keep its keys and lease, but grant no authority to stale endpoints
                withdrawn = invalidatePublished();
                snapshot = null;
                diagnostic = null;
                again = true;
            } catch (RuntimeException unavailable) {
                return invalidatePublished().thenCompose(ignored -> CompletableFuture.failedFuture(unavailable));
            }
        }
        if (runtime.config.assistedJoins()) {
            if (assistance.mode() && runtime.lastCarrier() == ControlCarrier.WEBSOCKET && snapshot != null
                    && round.health.acceptingPlayers() && !runtime.draining) {
                assistance.grant(assistance.authorityFor(snapshot, round.sentMillis, round.sentNanos));
            } else {
                assistance.revoke();
            }
        }
        if (round.hostProfile != null) {
            if (reply.hostProfileRevision() == null) {
                return CompletableFuture.failedFuture(new IOException("Profile acknowledgement missing"));
            }
            publishedKeyId = round.hostProfile.profile().admissionKeyId();
            if (snapshot != null) {
                profileRevision = reply.hostProfileRevision();
                lastProfile = round.hostProfile;
                published = new PublishedEndpoints(round.hostProfile.profile().candidates().stream()
                        .map(HostCandidate::endpoint).toList(), round.hostProfile.assistedFamilies());
            }
        } else if (profileRevision != null && !profileRevision.equals(reply.hostProfileRevision())) {
            // The provider no longer holds the published profile, so send it in full
            profileRevision = null;
            published = null;
            lastProfile = null;
            again |= snapshot != null;
        }
        CompletableFuture<Void> keyed = Async.done();
        String requestId = runtime.keeper.state().keyRequestId();
        if (reply.admissionKey() != null) {
            if (requestId == null || reply.keyRequest() == null || !requestId.equals(reply.keyRequest().id())
                    || !reply.admissionKey().keyId().equals(reply.keyRequest().keyId())) {
                return CompletableFuture.failedFuture(new IOException("Unbound admission key response"));
            }
            keys.deliver(reply.admissionKey());
            // A key the transport will not take must not outlive this heartbeat, in memory or on disk
            keyed = keys.install()
                    .thenApply(ignored -> {
                        save(runtime.keeper.state().withKeyRequestId(null));
                        return (Void) null;
                    })
                    .exceptionallyCompose(failure -> {
                        keys.discardDelivered();
                        return CompletableFuture.failedFuture(failure);
                    });
            lastProfile = null;
            again = true;
        } else if (requestId != null && reply.keyRequest() != null && requestId.equals(reply.keyRequest().id())) {
            // The provider confirms delivery but the one-time reply was lost
            save(runtime.keeper.state().withKeyRequestId(UUID.randomUUID().toString()));
            again = true;
        }
        long now = runtime.clock.nanos();
        if (reply.checkIn() != null) {
            scheduledCheckIns = true;
            minUpdateIntervalMillis = reply.checkIn().minUpdateIntervalMillis();
            nextHeartbeat = round.sentNanos + TimeUnit.MILLISECONDS.toNanos(reply.checkIn().afterMillis());
        } else {
            scheduledCheckIns = false;
            nextHeartbeat = round.sentNanos + TimeUnit.MILLISECONDS.toNanos(intervalMillis());
        }
        nextStatusUpdate = now + TimeUnit.MILLISECONDS.toNanos(scheduledCheckIns ? minUpdateIntervalMillis
                : intervalMillis());
        lastStatus = round.status;
        lastHealth = round.health;
        lastGameOutcomes = runtime.config.gameOutcomes();
        locationReported |= round.sentLocation;
        bedrockProtocolsReported |= round.sentProtocols;
        actions.available(reply.operatorActionAvailable());
        lastReadiness = reply.readiness();
        runtime.log.readiness(reply.readiness(), round.accepting);
        if (snapshot != null) {
            publishedSnapshot = snapshot;
            publishedRevision = snapshot.candidateRevision();
            publishedVersion = snapshot.publicationVersion();
            nextCandidateRefresh = now + REFRESH_NANOS;
            connectivity.published(snapshot);
        }
        boolean[] more = {again};
        HostProfileSnapshot current = snapshot, checked = diagnostic;
        CompletableFuture<Void> installed = keyed;
        return withdrawn
                .thenCompose(ignored -> installed)
                .thenCompose(ignored -> keys.retire(reply.retirements()))
                .thenCompose(ignored -> checked == null ? Async.done()
                        : diagnostics.configure(checked, reply.checkIn(), round.sentNanos, successMillis, successNanos)
                        .thenApply(configured -> {
                            if (!round.diagnosticsAdvertised) {
                                more[0] = true;
                            }
                            long remaining = Math.max(0, diagnostics.deadlineNanos() - runtime.clock.nanos());
                            long lead = Math.min(TimeUnit.SECONDS.toNanos(60), remaining / 5);
                            nextHeartbeat = Math.min(nextHeartbeat,
                                    Math.max(nextStatusUpdate, diagnostics.deadlineNanos() - lead));
                            return (Void) null;
                        }))
                .thenCompose(ignored -> {
                    Assistance.Authority authority = assistance.authority();
                    if (authority != null) {
                        long remaining = Math.max(0, authority.deadlineNanos() - runtime.clock.nanos());
                        long lead = Math.min(TimeUnit.SECONDS.toNanos(60), remaining / 5);
                        nextHeartbeat = Math.min(nextHeartbeat,
                                Math.max(runtime.clock.nanos(), authority.deadlineNanos() - lead));
                    }
                    return current == null ? Async.done() : reportConnectivity(current, reply);
                })
                .thenCompose(ignored -> {
                    if (current == null) {
                        return Async.done();
                    }
                    // A host policy transition republishes promptly, even at the same candidate revision
                    current.requireCurrent();
                    if (!runtime.config.assistedJoins()) {
                        return Async.done();
                    }
                    return Async.within(runtime.transport.captureHostProfile(), Duration.ofSeconds(10),
                            runtime.executor, "Host profile capture").thenApply(latest -> {
                        latest.requireCurrent();
                        if (assistance.needed(latest) != assistance.mode()) {
                            more[0] = true;
                        }
                        return (Void) null;
                    });
                })
                .thenApply(ignored -> {
                    if (!more[0]) {
                        runtime.log.recovered(ProviderLog.Operation.STATUS);
                    }
                    return more[0];
                });
    }

    /** Optional feedback never undoes a successful heartbeat or changes whether the host serves. */
    private CompletableFuture<Void> reportConnectivity(HostProfileSnapshot snapshot, Wire.HeartbeatReply reply) {
        if (snapshot.candidateRevision() < 1) {
            return Async.done();
        }
        return Async.attempt(() -> {
            var feedback = WireCodec.connectivityFeedback(reply.extensions());
            if (feedback.isEmpty() || feedback.get().candidateRevision() != snapshot.candidateRevision()) {
                return Async.done();
            }
            long now = runtime.clock.millis();
            List<ConnectivityCheck> checks = feedback.get().checks().stream()
                    .filter(check -> check.checkedAt() <= now && check.expiresAt() > now)
                    .toList();
            snapshot.requireCurrent();
            return Async.within(runtime.transport.reportConnectivityChecks(snapshot.candidateRevision(), checks),
                    Duration.ofSeconds(10), runtime.executor, "Connectivity feedback").thenApply(ignored -> {
                connectivity.checks(snapshot, checks);
                runtime.log.recovered(ProviderLog.Operation.CONNECTIVITY);
                return (Void) null;
            });
        }).exceptionally(unavailable -> {
            runtime.log.failed(ProviderLog.Operation.CONNECTIVITY, ProviderFailures.describe(unavailable));
            return null;
        });
    }

    /** Withdraws the published profile and the authority and diagnostics it carried. */
    CompletableFuture<Void> invalidatePublished() {
        publishedSnapshot = null;
        lastProfile = null;
        profileRevision = null;
        published = null;
        assistance.revoke();
        return diagnostics.disable();
    }

    /** Neither the schedule nor a status change sends another heartbeat until the backoff has passed. */
    void failed(Throwable failure) {
        if (runtime.fenced) {
            // Stopping, and why is already logged
            return;
        }
        failures++;
        nextHeartbeat = runtime.clock.nanos() + ProviderFailures.retryDelayNanos(failure, failures);
        nextStatusUpdate = nextHeartbeat;
        runtime.log.failed(ProviderLog.Operation.STATUS, ProviderFailures.describe(failure));
    }

    /** Whether a heartbeat is due: by schedule, for changed candidates, or for a changed listing or count. */
    CompletableFuture<Boolean> due() {
        if (runtime.clock.nanos() - nextHeartbeat >= 0) {
            return CompletableFuture.completedFuture(true);
        }
        return candidatesChanged().thenApply(changed -> changed || statusChanged());
    }

    /** Samples the cheap local counter; replaced material bypasses freshness-only coalescing. */
    private CompletableFuture<Boolean> candidatesChanged() {
        if (runtime.clock.nanos() - nextStatusUpdate < 0 || runtime.draining) {
            return CompletableFuture.completedFuture(false);
        }
        long version = runtime.transport.candidatePublicationVersion();
        if (version == 0 || version == publishedVersion) {
            return CompletableFuture.completedFuture(false);
        }
        CompletableFuture<Void> observed = Async.done();
        if (version != observedVersion) {
            observed = Async.within(runtime.transport.captureHostProfile(), Duration.ofSeconds(10), runtime.executor,
                    "Host profile capture").thenApply(snapshot -> {
                snapshot.requireCurrent();
                observedVersion = snapshot.publicationVersion();
                observedRevision = snapshot.candidateRevision();
                return null;
            });
        }
        return observed.thenApply(ignored -> {
            if (publishedSnapshot != null) {
                try {
                    publishedSnapshot.requireCurrent();
                } catch (RuntimeException changed) {
                    return true;
                }
            }
            return observedRevision != publishedRevision || runtime.clock.nanos() - nextCandidateRefresh >= 0;
        });
    }

    /** Whether the listing or the count changed enough, and long enough ago, to report before the schedule. */
    boolean statusChanged() {
        long now = runtime.clock.nanos();
        if (now - nextStatusUpdate < 0) {
            return false;
        }
        if (!scheduledCheckIns) {
            return now - nextHeartbeat >= 0;
        }
        ServerStatus status;
        try {
            status = statusSupplier == null ? null : statusSupplier.get();
        } catch (RuntimeException unreadable) {
            // The heartbeat itself says what failed
            return true;
        }
        Health health = healthSupplier.get();
        return !Objects.equals(status, lastStatus) || lastHealth == null
                || health.acceptingPlayers() != lastHealth.acceptingPlayers()
                || health.capacity() != lastHealth.capacity()
                || health.connectedPlayers().count() != lastHealth.connectedPlayers().count()
                || !Objects.equals(health.software(), lastHealth.software());
    }

    private long intervalMillis() {
        return runtime.discovery == null ? 10000 : runtime.discovery.heartbeatIntervalMs();
    }

    private void save(ProviderState state) {
        try {
            runtime.keeper.save(state);
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }
}

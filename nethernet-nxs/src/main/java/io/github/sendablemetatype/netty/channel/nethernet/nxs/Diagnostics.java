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

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfileSnapshot;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Diagnostic admission, a local opt-in. A successful heartbeat installs the trusted policy (context, the installed
 * admission keys and the current probe targets) for at most five minutes, bounded by the check-in lease and the key
 * deadlines. A failed heartbeat never renews it, and any change of profile, keys or serving state withdraws it.
 */
final class Diagnostics {
    static final long VALIDITY_MILLIS = 300000;

    private final ClientRuntime runtime;
    private KeyManager keys;
    private HostProfileSnapshot installed;
    private long deadlineNanos, expiresAt;

    Diagnostics(ClientRuntime runtime) {
        this.runtime = runtime;
    }

    void keys(KeyManager keys) {
        this.keys = keys;
    }

    boolean enabled() {
        return runtime.config.diagnosticAdmission();
    }

    long deadlineNanos() {
        return deadlineNanos;
    }

    boolean installed() {
        return installed != null;
    }

    /**
     * Whether diagnostics can serve this profile: a revisioned capture with at most 32 host or reflexive probe
     * targets, or an assisted family when assistance is on.
     */
    boolean eligible(HostProfileSnapshot snapshot) {
        if (snapshot.candidateRevision() < 1) {
            return false;
        }
        List<HostCandidate> probes = snapshot.probeCandidates();
        if (probes.isEmpty() && (!runtime.config.assistedJoins() || snapshot.assistedFamilies().isEmpty())) {
            return false;
        }
        return probes.stream().allMatch(candidate -> candidate.type() != CandidateType.RELAY);
    }

    /** Whether the installed policy is live and still describes this profile. */
    boolean current(HostProfileSnapshot snapshot) {
        if (installed == null || runtime.closing || runtime.closed
                || deadlineNanos - runtime.clock.nanos() <= 0
                || runtime.clock.millis() >= expiresAt
                || installed.candidateRevision() != snapshot.candidateRevision()) {
            return false;
        }
        try {
            installed.requireCurrent();
            snapshot.requireCurrent();
            return installed.profile().incarnation().equals(snapshot.profile().incarnation());
        } catch (RuntimeException replaced) {
            return false;
        }
    }

    CompletableFuture<Void> disable() {
        installed = null;
        expiresAt = 0;
        deadlineNanos = 0;
        if (enabled() && runtime.transport.supportsDiagnosticAdmission()) {
            return Async.within(runtime.transport.disableDiagnostics(), Duration.ofSeconds(10), runtime.executor,
                    "Diagnostic withdrawal");
        }
        return Async.done();
    }

    /**
     * Installs the policy a successful heartbeat for this snapshot allows.
     *
     * @param sentNanos    When the heartbeat was sent, which the lease counts from
     * @param successNanos When its reply arrived, monotonic
     * @param successMillis When its reply arrived, wall clock
     */
    CompletableFuture<Void> configure(HostProfileSnapshot snapshot, CheckInSchedule checkIn, long sentNanos,
                                      long successMillis, long successNanos) {
        return Async.attempt(() -> {
            if (runtime.closing || runtime.closed || runtime.draining) {
                throw new IOException("Diagnostic host no longer serving");
            }
            snapshot.requireCurrent();
            long until = successMillis + VALIDITY_MILLIS;
            if (checkIn != null) {
                long sent = successMillis - TimeUnit.NANOSECONDS.toMillis(successNanos - sentNanos);
                until = Math.min(until, sent + checkIn.leaseMillis());
            }
            long lifetime = until - successMillis;
            long deadline = successNanos + TimeUnit.MILLISECONDS.toNanos(Math.max(0, lifetime));
            if (lifetime <= 0 || deadline - runtime.clock.nanos() <= 0) {
                throw new IOException("Diagnostic heartbeat lease expired");
            }
            var context = new DiagnosticAdmissionCodec.Context(runtime.origin, runtime.registration().instanceId(),
                    snapshot.profile().incarnation(), runtime.keeper.state().generation());
            var policyKeys = keys.installed().stream()
                    .map(key -> new DiagnosticAdmissionCodec.Key(key.keyId(), key.secret(), 0,
                            Math.min(key.retireAfter(), 9007199254740991L)))
                    .toList();
            Map<DiagnosticHostPolicy.Endpoint, Long> expiries = new HashMap<>();
            for (HostCandidate candidate : snapshot.probeCandidates()) {
                AddressFamily family = candidate.family();
                var endpoint = new DiagnosticHostPolicy.Endpoint(family.number(),
                        DiagnosticAdmissionCodec.address(family.number(),
                                candidate.endpoint().getAddress().getHostAddress()),
                        candidate.endpoint().getPort(), snapshot.candidateRevision());
                expiries.merge(endpoint, until, Math::min);
            }
            if (runtime.config.assistedJoins()) {
                for (AddressFamily family : snapshot.assistedFamilies()) {
                    expiries.put(DiagnosticHostPolicy.Endpoint.assisted(family.number(), snapshot.candidateRevision()),
                            until);
                }
            }
            snapshot.requireCurrent();
            long policyExpiry = until;
            var policy = new DiagnosticHostPolicy(context, policyKeys, expiries.keySet(), policyExpiry, expiries);
            return Async.within(runtime.transport.configureDiagnostics(policy, deadline), Duration.ofSeconds(10),
                            runtime.executor, "Diagnostic installation")
                    .thenApply(ignored -> {
                        snapshot.requireCurrent();
                        if (runtime.closing || runtime.closed || deadline - runtime.clock.nanos() <= 0) {
                            throw new IllegalStateException("Diagnostic installation expired");
                        }
                        installed = snapshot;
                        deadlineNanos = deadline;
                        expiresAt = policyExpiry;
                        return (Void) null;
                    });
        }).exceptionallyCompose(failure -> disable().thenCompose(ignored -> CompletableFuture.failedFuture(failure)));
    }
}

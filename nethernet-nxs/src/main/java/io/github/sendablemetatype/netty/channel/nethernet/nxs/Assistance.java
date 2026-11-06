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

import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfile;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfileSnapshot;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Assisted joins. A successful heartbeat over the WebSocket grants local authority for at most five minutes, for the
 * profile it published; a reconnect or a ping never extends it. A join is served only while that authority, its
 * profile and the join's own deadline all hold, and the provider's deadline is read on the provider's clock, through
 * the offset its replies measured.
 */
final class Assistance implements ProviderWebSocket.AssistedJoins {
    static final long AUTHORITY_MILLIS = 300000;

    /**
     * @param deadlineNanos     Five minutes after the heartbeat was sent, on the monotonic clock
     * @param expiresAt         The same on this host's clock
     * @param providerExpiresAt The same on the provider's clock, which a join's deadline must not pass
     */
    record Authority(String instanceId, long generation, HostProfileSnapshot snapshot, long deadlineNanos,
                     long expiresAt, long providerExpiresAt) {
    }

    private final ClientRuntime runtime;
    private final ControlChannel control;
    /** Read by guards on other threads. */
    private volatile boolean mode;
    private volatile Authority authority;

    Assistance(ClientRuntime runtime, ControlChannel control) {
        this.runtime = runtime;
        this.control = control;
    }

    /** Whether the host currently offers assisted joins, which the published profile and the upgrade declare. */
    boolean mode() {
        return mode;
    }

    Authority authority() {
        return authority;
    }

    /** Whether the configuration, the carrier and the endpoint's families call for assistance. */
    boolean needed(HostProfileSnapshot snapshot) {
        if (!runtime.config.assistedJoins() || !control.available() || !runtime.transport.supportsAssistedJoins()) {
            return false;
        }
        snapshot.requireCurrent();
        return !snapshot.assistedFamilies().isEmpty();
    }

    /**
     * Follows the endpoint: when assistance starts or stops, the authority ends and the WebSocket reconnects, with or
     * without the assisted routing hint.
     *
     * @return Whether the mode changed, so the profile must be published again
     */
    boolean update(HostProfileSnapshot snapshot) throws IOException {
        boolean needed = needed(snapshot);
        if (needed == mode) {
            return false;
        }
        mode = needed;
        authority = null;
        control.configure();
        return true;
    }

    void grant(Authority granted) {
        authority = granted;
    }

    void revoke() {
        authority = null;
    }

    @Override
    public CompletionStage<AssistedAnswer> answer(AssistedJoin join, long deadlineNanos) {
        OwnershipGuard guard = authority(join);
        guard.requireCurrent();
        if (!join.diagnostic()) {
            // The host serves only offers it can answer and keys it can bind; anything else refuses this join only
            join.parseOffer();
            join.canonicalCpk();
        }
        long remaining = deadlineNanos - runtime.clock.nanos();
        return Async.within(runtime.transport.assistedJoin(join, deadlineNanos, guard),
                        Duration.ofNanos(Math.max(1, remaining)), runtime.executor, "Assisted join")
                .thenApply(answer -> {
                    guard.requireCurrent();
                    if (answer.asserted() == join.diagnostic()) {
                        // Clients since 26.40 refuse an answer without the server's identity; diagnostics forbid one
                        throw new IllegalStateException(join.diagnostic()
                                ? "A diagnostic answer must not carry an identity assertion"
                                : "An assisted answer must carry the server identity");
                    }
                    return answer;
                });
    }

    @Override
    public OwnershipGuard authority(AssistedJoin join) {
        Authority captured = authority;
        if (captured == null) {
            return () -> {
                throw new IllegalStateException("Assisted authority unavailable");
            };
        }
        return () -> {
            Authority current = authority;
            if (!mode || runtime.closing || current == null
                    || !captured.instanceId().equals(current.instanceId())
                    || captured.generation() != current.generation()
                    || !captured.instanceId().equals(join.instanceId())
                    || captured.generation() != join.generation()
                    || runtime.clock.nanos() - captured.deadlineNanos() >= 0
                    || runtime.clock.millis() >= captured.expiresAt()) {
                throw new IllegalStateException("Assisted authority expired");
            }
            captured.snapshot().requireCurrent();
            HostProfile profile = captured.snapshot().profile();
            if (!profile.incarnation().equals(join.incarnation())
                    || !profile.dtlsFingerprint().equals(join.hostFingerprint())
                    || !profile.admissionKeyId().equals(join.keyId())
                    || join.expiresAt() > captured.providerExpiresAt()) {
                throw new IllegalStateException("Assisted binding mismatch");
            }
        };
    }

    @Override
    public void failed(AssistedJoin join) {
        runtime.log.assistedJoinFailed(join);
    }

    /** The authority a heartbeat sent at {@code sentMillis}, monotonic {@code sentNanos}, grants. */
    Authority authorityFor(HostProfileSnapshot snapshot, long sentMillis, long sentNanos) {
        Wire.SavedRegistration registration = runtime.registration();
        return new Authority(registration.instanceId(), runtime.keeper.state().generation(), snapshot,
                sentNanos + TimeUnit.MILLISECONDS.toNanos(AUTHORITY_MILLIS), sentMillis + AUTHORITY_MILLIS,
                sentMillis + runtime.clockOffsetMillis() + AUTHORITY_MILLIS);
    }
}

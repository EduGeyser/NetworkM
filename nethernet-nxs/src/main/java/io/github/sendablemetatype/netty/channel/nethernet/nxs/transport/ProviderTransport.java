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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * What the provider client needs from the NetherNet endpoint that serves NXS players. The endpoint owns the
 * gameplay socket, the DTLS certificate, the admission keys' native mirror and every peer; the client owns the
 * conversation with the provider and never touches a packet.
 *
 * <p>Threading: the client calls every method from its own lifecycle thread, one call at a time, never from an
 * event loop and never while holding a lock. A method that returns a stage must not block: it does its work on the
 * endpoint's own threads and completes the stage from any thread. A method that returns a value must return
 * promptly without I/O. Monotonic deadlines are {@link System#nanoTime()} values.
 *
 * <p>Ownership: values passed in are immutable or copied by the callee. Guards passed in belong to the caller and
 * must be checked, not stored beyond the call's result.
 */
public interface ProviderTransport {
    /**
     * Captures the profile the provider should publish, with a guard over the endpoint material it describes.
     * Completes exceptionally while the endpoint has no installed key, is draining, or is closed.
     */
    CompletionStage<HostProfileSnapshot> captureHostProfile();

    /**
     * A cheap local counter that changes whenever {@link #captureHostProfile()} would return something else, so the
     * client can poll it every second. Zero when the endpoint does not track it.
     */
    default long candidatePublicationVersion() {
        return 0;
    }

    /**
     * Atomically replaces the installed admission key epochs, oldest first, and mirrors them into the native
     * prefilter. Completes exceptionally, installing nothing, when the endpoint refuses the snapshot; the client then
     * keeps the key out of its saved state.
     *
     * @param keys At most eight epochs with distinct IDs
     */
    CompletionStage<Void> installAdmissionKeys(List<AdmissionKey> keys);

    /**
     * Takes up to {@code maximum} observations of admitted tickets, oldest first. Called every second; the endpoint
     * keeps a bounded queue and drops what does not fit.
     */
    List<TransportOutcome> pollOutcomes(int maximum);

    /**
     * The STUN servers the provider's discovery lists, at most two, for an endpoint that discovers its public
     * address. Resolving names happens on the endpoint's threads. Configured endpoints ignore them.
     */
    default CompletionStage<Void> configureStunServers(List<StunServer> servers) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Regional check results from a heartbeat reply for the given candidate revision, at most eighteen. Feedback
     * only: it may withhold failed public endpoints from later offers but never changes whether the endpoint serves.
     * An endpoint whose revision moved on ignores them.
     */
    default CompletionStage<Void> reportConnectivityChecks(long candidateRevision, List<ConnectivityCheck> checks) {
        return CompletableFuture.completedFuture(null);
    }

    /** Whether the endpoint can create peers for assisted joins on its gameplay port. */
    default boolean supportsAssistedJoins() {
        return false;
    }

    /**
     * Serves one assisted join the provider pushed: checks the join against the endpoint's own incarnation,
     * fingerprint and keys, creates a peer on the gameplay port and gathers its answer. A player's answer must carry
     * the endpoint's server identity (see {@link AnswerSigner}); a diagnostic answer must carry none. The client has
     * already checked the join's frame, its owner and its deadline.
     *
     * @param deadlineNanos When the join's deadline passes, already corrected for the provider's clock offset
     * @param guard         Holds while the provider authority behind the join holds; check it before creating the
     *                      peer and again before completing, and let the peer's login binding consult it
     * @return The answer; completes exceptionally to refuse the join, which the client answers with
     * {@code accepted:false}
     */
    default CompletionStage<AssistedAnswer> assistedJoin(AssistedJoin join, long deadlineNanos, OwnershipGuard guard) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Assisted joins unavailable"));
    }

    /** Whether the endpoint can admit diagnostic tickets. A local opt-in, independent of serving players. */
    default boolean supportsDiagnosticAdmission() {
        return false;
    }

    /**
     * Installs the trusted diagnostic policy a successful heartbeat produced. Installation must not extend it.
     *
     * @param deadlineNanos The policy's monotonic deadline, taken from the heartbeat that produced it
     */
    default CompletionStage<Void> configureDiagnostics(DiagnosticHostPolicy policy, long deadlineNanos) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Diagnostic admission unavailable"));
    }

    /** Withdraws diagnostic admission, ending active attempts. Player admission is untouched. */
    default CompletionStage<Void> disableDiagnostics() {
        return CompletableFuture.completedFuture(null);
    }

    /** Stops admitting new players while keeping established sessions. */
    CompletionStage<Void> drain();

    /** Closes the endpoint's NXS admission. Completes once its native resources are released. */
    CompletionStage<Void> close();
}

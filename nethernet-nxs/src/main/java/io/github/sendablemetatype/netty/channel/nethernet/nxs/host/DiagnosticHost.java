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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.VerifiedAdmission;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Admission;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Claims;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Key;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticChannels;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticExchange;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticSdp;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.host.DiagnosticResult.Outcome;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SharedIcePort;
import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * NXS connectivity diagnostics on the endpoint's shared ICE port (diagnostic-v1), the host side of upstream's
 * {@code NativeDiagnosticHostGate}. A diagnostic ticket, network ID 0, arrives as a held check like a player's, or as
 * an assisted join over the provider's WebSocket. Its peer comes from {@link SharedIcePort#openPeer}, so it is never a
 * player: no child channel, no session, no admission event, and it holds one of the endpoint's pending handshakes
 * while it lives. The prober opens both NetherNet data channels and sends one PING, the host answers with its PONG, and
 * the peer closes at most a second later.
 *
 * <p>Only the trusted policy a successful heartbeat installed admits checks: its context, keys and endpoints, each
 * endpoint with its own deadline, and the heartbeat's monotonic deadline over all of it. Withdrawing or replacing the
 * policy stops every check it no longer covers. A direct permit (profile 1) is taken only from a held check and an
 * assisted one (profile 2) only from the WebSocket, so an assisted permit that arrives as a check creates no peer.
 * Bounds: {@value #MAX_ACTIVE} checks at once, the ledger's replay history, a handshake of at most
 * {@value DiagnosticAdmissionCodec#MAX_HANDSHAKE_MILLIS} ms within a permit of at most
 * {@value DiagnosticAdmissionCodec#MAX_ATTEMPT_MILLIS} ms, and {@value #MAX_RESULTS} queued results.
 *
 * <p>Destinations: an assisted prober's address must be one the host reaches by its rule for clients elsewhere, in
 * production a public one, as the specification asks.
 *
 * <p>Everything runs on the endpoint's event loop except the peer listener, which records what the engine reports and
 * hops to the loop, so no peer is closed inside an engine callback (ruling A-4). A check's ledger record is settled and
 * its pending handshake given back once its peer's close stage completed.
 */
final class DiagnosticHost {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(DiagnosticHost.class);

    static final int MAX_ACTIVE = 4;
    static final int MAX_RESULTS = 32;
    /** How long the prober may take to close after the PONG; it closes as soon as it checked the echo. */
    static final long LINGER_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long MAX_SAFE_INTEGER = 9007199254740991L;

    /** Finds the one candidate on the gameplay port an assisted answer names. Event loop. */
    @FunctionalInterface
    interface AnswerCandidates {
        /**
         * @return completes with the candidate, or exceptionally when none is found by the deadline; completing it
         *         from outside stops the search
         */
        CompletableFuture<HostCandidate> find(AddressFamily family, long deadlineNanos);
    }

    private final SharedIcePort port;
    private final DtlsIdentity identity;
    private final DtlsFingerprint fingerprint;
    private final TicketLedger ledger;
    private final EventExecutor loop;
    private final DiagnosticAdmissionCodec.Clock clock;
    private final AnswerCandidates answerCandidates;
    private final Predicate<InetAddress> reachable;

    // Event loop only.
    private final Map<String, Check> checks = new LinkedHashMap<>();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private DiagnosticHostPolicy policy;
    private long policyDeadlineNanos;
    private boolean closed;
    private long anchorMillis, anchorNanos, lastNanos;
    private boolean anchored, clockFailed;

    private final ArrayBlockingQueue<DiagnosticResult> results = new ArrayBlockingQueue<>(MAX_RESULTS);

    /**
     * @param reachable Whether clients elsewhere reach an address, the host's rule for what it may name or check
     */
    DiagnosticHost(SharedIcePort port, DtlsIdentity identity, DtlsFingerprint fingerprint, TicketLedger ledger,
                   EventExecutor loop, DiagnosticAdmissionCodec.Clock clock, AnswerCandidates answerCandidates,
                   Predicate<InetAddress> reachable) {
        this.port = port;
        this.identity = identity;
        this.fingerprint = fingerprint;
        this.ledger = ledger;
        this.loop = loop;
        this.clock = clock;
        this.answerCandidates = answerCandidates;
        this.reachable = reachable;
    }

    // ---- Policy ----

    /**
     * Installs the policy, replacing any earlier one. Checks the new policy does not cover stop at once; the ones it
     * covers keep their original deadlines.
     *
     * @param deadlineNanos The heartbeat's monotonic deadline, after which the policy admits nothing
     */
    void install(DiagnosticHostPolicy replacement, long deadlineNanos) {
        if (closed) {
            throw new IllegalStateException("Diagnostics are closed");
        }
        policy = replacement;
        policyDeadlineNanos = deadlineNanos;
        long now = observe();
        for (Check check : List.copyOf(checks.values())) {
            if (!authorized(check, now)) {
                stop(check, Outcome.WITHDRAWN);
            }
        }
    }

    /** Withdraws the policy and stops every check. The replay history stays. */
    void disable() {
        policy = null;
        for (Check check : List.copyOf(checks.values())) {
            stop(check, Outcome.WITHDRAWN);
        }
    }

    /** The installed policy, null when none is. */
    DiagnosticHostPolicy policy() {
        return policy;
    }

    // ---- Direct checks ----

    /**
     * Takes a held check whose ticket the validator opened with network ID 0, and releases the ticket's login
     * binding, since a diagnostic never logs in.
     *
     * @return Whether a peer now answers the check; false to reject it
     */
    boolean admit(IceEndpoint.HeldCheck held, VerifiedAdmission verified) {
        long now = observe();
        DiagnosticHostPolicy current = policy;
        Key key = current == null ? null : current.key(verified.keyId());
        if (!serving(current, now) || key == null) {
            verified.loginBinding().release();
            return false;
        }
        Admission admission = DiagnosticAdmissionCodec.verified(current.context(), key, verified, current.expiresAt(),
                clock);
        if (admission == null) {
            return false;
        }
        Claims claims = admission.claims();
        InetSocketAddress remote = numeric(held.remoteAddress());
        long remaining = claims.expiresAt() - now;
        if (remote == null
                || claims.profile() != DiagnosticAdmissionCodec.PROFILE
                || AddressFamily.of(remote.getAddress()).number() != claims.family()
                || claims.expiresAt() > current.endpointExpiry(DiagnosticHostPolicy.Endpoint.from(claims))
                || occupied(remote)
                || remaining < 1 || remaining > DiagnosticAdmissionCodec.MAX_ATTEMPT_MILLIS) {
            return false;
        }
        // The pinned offer the permit stands for, with the check's source as its one candidate
        String offer = VerifiedAdmission.remoteDescription(admission.remoteUfrag(), claims.clientIcePwd(),
                DtlsFingerprint.of(HexFormat.of().parseHex(claims.clientFingerprintHex())),
                DiagnosticAdmissionCodec.SCTP_PORT,
                DiagnosticAdmissionCodec.MAX_MESSAGE_SIZE)
                + "a=candidate:1 1 UDP 2130706431 " + remote.getAddress().getHostAddress() + " " + remote.getPort()
                + " typ host\r\na=end-of-candidates\r\n";
        return start(admission, key, remote, null, offer, remaining) != null;
    }

    // ---- Assisted checks ----

    /**
     * Serves an assisted diagnostic the provider pushed over its authenticated WebSocket: checks it against the
     * policy, creates its peer before any packet arrives, and answers with one candidate on the gameplay port.
     *
     * @param deadlineNanos When the join's deadline passes, corrected for the provider's clock
     * @param guard         Holds while the provider authority behind the join holds; the check stops once it lapses
     * @return The answer, which carries no identity assertion
     * @throws IllegalStateException    If the host serves no diagnostic now
     * @throws IllegalArgumentException If the policy does not admit the join
     */
    CompletionStage<AssistedAnswer> assist(AssistedJoin join, long deadlineNanos, OwnershipGuard guard) {
        guard.requireCurrent();
        long now = observe();
        DiagnosticHostPolicy current = policy;
        if (!serving(current, now)) {
            throw new IllegalStateException("This host serves no diagnostic check now");
        }
        Key key = current.key(join.keyId());
        if (key == null
                || !join.hostFingerprint().equals(fingerprint)
                || !join.instanceId().equals(current.context().hostId())
                || join.generation() != current.context().generation()
                || !join.incarnation().equals(current.context().incarnation())) {
            throw notAdmitted();
        }
        String remoteUfrag = join.parseOffer().ufrag();
        Admission admission = DiagnosticAdmissionCodec.open(current.context(), key, join.localUfrag(), remoteUfrag,
                current.expiresAt(), clock);
        if (admission == null) {
            throw notAdmitted();
        }
        Claims claims = admission.claims();
        InetSocketAddress remote = DiagnosticSdp.offer(join.offer().getBytes(StandardCharsets.UTF_8), claims,
                remoteUfrag).endpoint(claims.family());
        // An IPv4 prober behind NAT may offer its private host address and learn the host's public destination from
        // the answer. A private address is no destination, so the host checks nothing there and waits for the
        // prober's authenticated check to show its public source.
        boolean privateHost = claims.family() == 4
                && EndpointAddress.scope(remote.getAddress()) == EndpointAddress.Scope.PRIVATE
                && candidateType(join.offer()).equals("host");
        long remaining = claims.expiresAt() - now;
        if (claims.profile() != DiagnosticAdmissionCodec.ASSISTED_PROFILE
                || !claims.attemptIdHex().equals(join.id())
                || claims.expiresAt() != join.expiresAt()
                || !admission.credentials().icePwd().equals(join.localPassword())
                || claims.expiresAt() > current.endpointExpiry(DiagnosticHostPolicy.Endpoint.from(claims))
                || occupied(remote)
                || !(reachable.test(remote.getAddress()) || privateHost)
                || remaining < 1 || remaining > DiagnosticAdmissionCodec.MAX_ATTEMPT_MILLIS) {
            throw notAdmitted();
        }
        String offer = privateHost && !reachable.test(remote.getAddress()) ? withoutCandidates(join.offer())
                : join.offer();
        Check check = start(admission, key, remote, guard, offer, remaining);
        if (check == null) {
            throw new IllegalStateException("The diagnostic was replayed, or the host has no room for it");
        }
        long gathering = Math.min(check.handshakeDeadlineNanos, deadlineNanos);
        check.candidate = answerCandidates.find(AddressFamily.of(claims.family()), gathering);
        check.candidate.whenComplete((candidate, failure) -> onLoop(() -> {
            if (failure != null) {
                stop(check, Outcome.ANSWER_FAILED);
            } else {
                check.answerCandidate = candidate;
                answer(check);
            }
        }));
        return check.answer.minimalCompletionStage();
    }

    /** Whether an assisted check's own peer answers checks for this ufrag, so a held one must be left to it. */
    boolean awaits(String localUfrag) {
        return checks.values().stream().anyMatch(check -> check.guard != null && !check.closing
                && check.admission.credentials().localUfrag().equals(localUfrag));
    }

    /** Hands the assisted answer back once the peer answered and the candidate is known. Event loop. */
    private void answer(Check check) {
        if (check.closing || check.answer.isDone() || check.localDescription == null || check.answerCandidate == null) {
            return;
        }
        try {
            if (!authorized(check, observe())) {
                throw new IllegalStateException("The diagnostic's authority lapsed before its answer");
            }
            AssistedAnswer answer = AssistedAnswer.build(check.localDescription, check.answerCandidate, null);
            // The closed profile the prober checks the answer against
            DiagnosticSdp.answer(answer.sdp().getBytes(StandardCharsets.UTF_8), check.admission.claims(),
                    fingerprint.hex());
            check.answer.complete(answer);
        } catch (GeneralSecurityException | RuntimeException e) {
            check.answer.completeExceptionally(e);
            stop(check, Outcome.ANSWER_FAILED);
        }
    }

    // ---- Checks ----

    private boolean serving(DiagnosticHostPolicy current, long now) {
        return !closed && !clockFailed && current != null && lastNanos - policyDeadlineNanos < 0
                && now < current.expiresAt() && checks.size() < MAX_ACTIVE;
    }

    /** Records the attempt and creates its peer, which answers the offer. Null when either is refused. */
    private Check start(Admission admission, Key key, InetSocketAddress remote, OwnershipGuard guard, String offer,
                        long remainingMillis) {
        Claims claims = admission.claims();
        TicketLedger.DiagnosticRecord record = ledger.recordDiagnostic(claims.attemptIdHex(), claims.expiresAt(),
                clock.wallMillis().getAsLong());
        if (record == null) {
            return null;
        }
        long nanos = lastNanos;
        Check check = new Check(admission, key, remote, guard, record, nanos + remainingMillis * 1_000_000L,
                nanos + Math.min(remainingMillis, DiagnosticAdmissionCodec.MAX_HANDSHAKE_MILLIS) * 1_000_000L);
        Optional<Peer> peer;
        try {
            peer = port.openPeer(new IceCredentials(admission.credentials().localUfrag(),
                    admission.credentials().icePwd()), identity, check);
        } catch (RuntimeException e) {
            log.debug("Could not create the peer of diagnostic {}: {}", claims.attemptIdHex(), e.toString());
            peer = Optional.empty();
        }
        if (peer.isEmpty()) {
            ledger.finishDiagnostic(record, false);
            return null;
        }
        check.peer = peer.get();
        checks.put(claims.attemptIdHex(), check);
        check.peer.answer(offer).whenComplete((local, failure) -> onLoop(() -> answered(check, local, failure)));
        return check;
    }

    private void answered(Check check, String local, Throwable failure) {
        if (check.closing) {
            return;
        }
        // The client's view of the host is the published fingerprint and the permit's ufrag; a peer that presents
        // anything else can only fail DTLS or ICE
        if (failure != null || !local.contains("a=ice-ufrag:" + check.admission.credentials().localUfrag() + "\r\n")
                || local.lines().filter(line -> line.startsWith("a=fingerprint:"))
                .noneMatch(line -> fingerprint.equals(fingerprintOf(line)))) {
            stop(check, Outcome.NEGOTIATION_FAILED);
            return;
        }
        check.localDescription = local;
        if (check.guard != null) {
            answer(check);
        }
        advance(check);
    }

    /** Advances every check; ends the ones out of time. Event loop, from the host's maintenance. */
    void tick() {
        for (Check check : List.copyOf(checks.values())) {
            advance(check);
        }
    }

    /** The check's next step from what its peer reported. Event loop. */
    private void advance(Check check) {
        if (check.closing) {
            return;
        }
        long now = observe();
        DiagnosticChannels channels = check.channels;
        Outcome ended = !authorized(check, now) ? Outcome.WITHDRAWN
                : channels.protocolFailed() ? Outcome.INVALID_PROTOCOL
                : channels.failedTransport() ? check.complete ? Outcome.PONG_SENT : Outcome.TRANSPORT_FAILED
                : !check.complete && lastNanos - check.handshakeDeadlineNanos >= 0 ? Outcome.HANDSHAKE_TIMEOUT
                : null;
        if (ended != null) {
            stop(check, ended);
            return;
        }
        // Only a peer whose own answer was checked may speak for the host
        if (check.exchange == null && check.localDescription != null && channels.ready() && !selectPath(check)) {
            stop(check, Outcome.INVALID_PATH);
            return;
        }
        // The PING may come before the host sees the connection up, so it is read as soon as the exchange starts
        byte[] frame = check.exchange == null || check.complete ? null : channels.poll();
        if (frame != null) {
            try {
                check.exchange.receive(0, frame);
            } catch (IllegalArgumentException invalid) {
                stop(check, Outcome.INVALID_PING);
                return;
            }
            check.complete = true;
            check.completeNanos = lastNanos;
        }
        if (check.complete && lastNanos - check.completeNanos >= LINGER_NANOS) {
            stop(check, Outcome.PONG_SENT);
        }
    }

    /**
     * Checks the selected pair before any PONG and starts the exchange. The local side is the port's own host
     * address, or the endpoint the prober was told to check: libwebrtc reports the address a remote peer saw as a
     * local peer-reflexive candidate, which for a host behind NAT is the published or answered mapping.
     */
    private boolean selectPath(Check check) {
        TransportPath path = check.path;
        InetSocketAddress local = path == null ? null : numeric(path.local());
        InetSocketAddress remote = path == null ? null : numeric(path.remote());
        int family = check.admission.claims().family();
        if (local == null || remote == null
                || AddressFamily.of(local.getAddress()).number() != family
                || AddressFamily.of(remote.getAddress()).number() != family) {
            return false;
        }
        InetSocketAddress bound = port.localAddress();
        boolean onPort = local.getPort() == bound.getPort()
                && (bound.getAddress().isAnyLocalAddress() || bound.getAddress().equals(local.getAddress()));
        boolean announced = local.equals(check.announced());
        boolean remoteAllowed = check.guard == null ? remote.equals(check.remote) : reachable.test(remote.getAddress());
        if (!remoteAllowed || !(onPort || announced)) {
            return false;
        }
        check.selectedLocal = local;
        check.selectedRemote = remote;
        DataLink reliable = check.reliable;
        check.exchange = new DiagnosticExchange(check.admission.claims().attemptIdHex(), true,
                (channel, frame) -> reliable.send(ByteBuffer.wrap(frame), failure -> {
                    if (failure != null) {
                        check.channels.failed();
                        check.wake();
                    }
                }));
        check.exchange.start();
        return true;
    }

    private void stop(Check check, Outcome outcome) {
        if (check.closing) {
            return;
        }
        check.closing = true;
        check.outcome = outcome;
        if (outcome != Outcome.PONG_SENT) {
            check.complete = false;
        }
        if (check.candidate != null) {
            check.candidate.cancel(false);
        }
        check.answer.completeExceptionally(new IllegalStateException("The diagnostic ended: " + outcome));
        check.peer.close().whenComplete((ignored, failure) -> onLoop(() -> finished(check, failure),
                () -> finished(check, failure)));
    }

    /** The check's peer is gone: its record is settled, its pending handshake back, its result queued. */
    private void finished(Check check, Throwable failure) {
        Claims claims = check.admission.claims();
        checks.remove(claims.attemptIdHex(), check);
        ledger.finishDiagnostic(check.record, true);
        long now = observe();
        Outcome outcome = failure != null ? Outcome.CLEANUP_FAILED
                : check.complete && !authorized(check, now) ? Outcome.OBSERVATION_INVALIDATED
                : check.outcome;
        DiagnosticExchange exchange = check.exchange;
        DiagnosticResult result = new DiagnosticResult(claims.attemptIdHex(), check.key.keyId(),
                DiagnosticHostPolicy.Endpoint.from(claims), claims.offerDigestHex(), outcome, check.selectedLocal,
                check.selectedRemote, exchange == null ? 0 : exchange.sentFrames(),
                exchange == null ? 0 : exchange.sentBytes(), check.channels.receivedFrames(),
                check.channels.receivedBytes(), now);
        log.debug("NXS diagnostic {} ended: {}", claims.attemptIdHex(), outcome);
        if (!results.offer(result)) {
            log.debug("Dropping the result of NXS diagnostic {}: nobody takes the results", claims.attemptIdHex());
        }
        if (closed && checks.isEmpty()) {
            terminated.complete(null);
        }
    }

    private boolean authorized(Check check, long now) {
        if (check.guard != null) {
            try {
                check.guard.requireCurrent();
            } catch (RuntimeException lapsed) {
                return false;
            }
        }
        DiagnosticHostPolicy current = policy;
        Claims claims = check.admission.claims();
        // Admission capped the permit by the policy and endpoint deadlines it found; a later policy for the same
        // endpoint does not shorten it, but withdrawing the context, the key or the endpoint ends it at once
        return !closed && !clockFailed && current != null
                && lastNanos - policyDeadlineNanos < 0
                && check.admission.context().equals(current.context())
                && now < claims.expiresAt()
                && lastNanos - check.deadlineNanos < 0
                && check.key.equals(current.key(check.key.keyId()))
                && now >= check.key.notBefore()
                && now < check.key.retireAfter()
                && current.endpoints().contains(DiagnosticHostPolicy.Endpoint.from(claims));
    }

    private boolean occupied(InetSocketAddress remote) {
        return checks.values().stream().anyMatch(check -> check.remote.equals(remote));
    }

    /**
     * The wall clock as diagnostics read it: anchored to the monotonic clock, so it never runs backwards or slower
     * than real time, which a host clock set back would otherwise use to stretch a permit. A clock that cannot be
     * read sanely stops every check.
     */
    private long observe() {
        long wall = clock.wallMillis().getAsLong(), nanos = clock.nanoTime().getAsLong();
        if (clockFailed || wall < 0 || wall > MAX_SAFE_INTEGER || anchored && nanos - lastNanos < 0) {
            clockFailed = true;
            return MAX_SAFE_INTEGER;
        }
        if (!anchored) {
            anchored = true;
            anchorMillis = wall;
            anchorNanos = nanos;
        }
        lastNanos = nanos;
        long effective = anchorMillis + (nanos - anchorNanos) / 1_000_000L;
        if (wall > effective) {
            anchorMillis = wall;
            anchorNanos = nanos;
            effective = wall;
        }
        return effective;
    }

    // ---- Results and close ----

    /** Takes up to {@code maximum} results, oldest first. Any thread. */
    List<DiagnosticResult> poll(int maximum) {
        List<DiagnosticResult> polled = new ArrayList<>(Math.min(maximum, MAX_RESULTS));
        results.drainTo(polled, maximum);
        return polled;
    }

    /** Stops every check. Completes once every check's peer is gone. Event loop. */
    CompletionStage<Void> close() {
        if (!closed) {
            closed = true;
            policy = null;
            for (Check check : List.copyOf(checks.values())) {
                stop(check, Outcome.HOST_CLOSED);
            }
            if (checks.isEmpty()) {
                terminated.complete(null);
            }
        }
        return terminated.minimalCompletionStage();
    }

    private void onLoop(Runnable task) {
        onLoop(task, () -> { });
    }

    private void onLoop(Runnable task, Runnable rejectedTask) {
        if (loop.inEventLoop()) {
            task.run();
            return;
        }
        try {
            loop.execute(task);
        } catch (RejectedExecutionException shutDown) {
            rejectedTask.run();
        }
    }

    private static IllegalArgumentException notAdmitted() {
        return new IllegalArgumentException("The diagnostic policy does not admit this check");
    }

    private static InetSocketAddress numeric(InetSocketAddress address) {
        if (address == null || address.isUnresolved() || address.getPort() < 1) {
            return null;
        }
        try {
            // Drops any scope ID and host name, so equal endpoints compare equal
            return new InetSocketAddress(InetAddress.getByAddress(address.getAddress().getAddress()),
                    address.getPort());
        } catch (UnknownHostException impossible) {
            return null;
        }
    }

    private static DtlsFingerprint fingerprintOf(String line) {
        try {
            return DtlsFingerprint.parse(line.substring("a=fingerprint:".length()).strip());
        } catch (IllegalArgumentException otherDigest) {
            return null;
        }
    }

    /** The type of the offer's one candidate, which {@link DiagnosticSdp#offer} has checked. */
    private static String candidateType(String offer) {
        return offer.lines().filter(line -> line.startsWith("a=candidate:")).findFirst().orElseThrow()
                .split(" ")[7];
    }

    private static String withoutCandidates(String offer) {
        return String.join("\r\n", offer.lines()
                .filter(line -> !line.startsWith("a=candidate:") && !line.startsWith("a=remote-candidates:")
                        && !line.equals("a=end-of-candidates"))
                .toList()) + "\r\n";
    }

    /** One admitted check, from its peer's creation until its peer is gone. */
    private final class Check implements PeerListener {
        final Admission admission;
        final Key key;
        /** The direct check's source, or the assisted offer's candidate. */
        final InetSocketAddress remote;
        /** The provider authority behind an assisted check; null for a direct one. */
        final OwnershipGuard guard;
        final TicketLedger.DiagnosticRecord record;
        final long deadlineNanos, handshakeDeadlineNanos;
        final DiagnosticChannels channels = new DiagnosticChannels();
        final CompletableFuture<AssistedAnswer> answer = new CompletableFuture<>();
        volatile DataLink reliable;
        volatile TransportPath path;

        // Event loop only.
        Peer peer;
        String localDescription;
        CompletableFuture<HostCandidate> candidate;
        HostCandidate answerCandidate;
        DiagnosticExchange exchange;
        InetSocketAddress selectedLocal, selectedRemote;
        boolean complete, closing;
        long completeNanos;
        Outcome outcome;

        Check(Admission admission, Key key, InetSocketAddress remote, OwnershipGuard guard,
              TicketLedger.DiagnosticRecord record, long deadlineNanos, long handshakeDeadlineNanos) {
            this.admission = admission;
            this.key = key;
            this.remote = remote;
            this.guard = guard;
            this.record = record;
            this.deadlineNanos = deadlineNanos;
            this.handshakeDeadlineNanos = handshakeDeadlineNanos;
        }

        /** The endpoint the prober was told to check: the direct target, or the assisted answer's candidate. */
        InetSocketAddress announced() {
            if (guard != null) {
                return answerCandidate == null ? null : answerCandidate.endpoint();
            }
            Claims claims = admission.claims();
            byte[] address = HexFormat.of().parseHex(claims.targetAddressHex());
            try {
                return new InetSocketAddress(InetAddress.getByAddress(claims.family() == 4
                        ? Arrays.copyOfRange(address, 12, 16) : address), claims.targetPort());
            } catch (UnknownHostException impossible) {
                return null;
            }
        }

        void wake() {
            onLoop(() -> advance(this));
        }

        // Engine threads from here on: record what the engine reports, then step on the event loop.

        @Override
        public boolean onRemoteChannel(DataLink link) {
            if (link.parameters().label().equals(DiagnosticChannels.RELIABLE.label()) && reliable == null) {
                reliable = link;
            }
            // Checked once open, where a wrong channel fails the attempt
            return true;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            try {
                channels.opened(link.parameters());
            } catch (IllegalArgumentException wrongChannel) {
                // The channels record the violation
            }
            wake();
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            channels.received(link == reliable, message);
            wake();
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
            channels.failed();
            wake();
        }

        @Override
        public void onPathSelected(TransportPath selected) {
            path = selected;
        }

        @Override
        public void onStateChange(PeerState state) {
            switch (state) {
                case CONNECTED -> channels.connected();
                case FAILED, CLOSED -> channels.failed();
                default -> {
                    return;
                }
            }
            wake();
        }

        @Override
        public void onClosed(CloseCause cause) {
            channels.failed();
            wake();
        }

        @Override
        public void onLocalCandidate(String candidate, String mid) {
        }

        @Override
        public void onGatheringComplete() {
        }

        @Override
        public void onQueueLow(DataLink link) {
        }

        @Override
        public void onIceStateChange(IceState state) {
        }
    }
}

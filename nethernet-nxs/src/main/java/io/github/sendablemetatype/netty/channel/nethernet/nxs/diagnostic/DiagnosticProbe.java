/* Copyright 2026 CloudburstMC. Licensed under the Apache License, Version 2.0. */
package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EndpointConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Claims;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.Context;
import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.*;

/**
 * The prober's side of one connectivity diagnostic (diagnostic-v1), upstream's {@code NativeDiagnosticProbeAttempt}
 * on the engine interface. A client peer on a fixed local endpoint offers both NetherNet data channels, the provider's
 * signaling returns the host's answer for that exact offer, and the probe sends one PING, checks that the host behind
 * the pinned certificate echoed it on the selected path, and closes. The caller authorizes the attempt; the probe
 * neither retries nor speaks the game protocol.
 *
 * <p>Only public endpoints are destinations, as the specification asks of production probers. A loopback endpoint also
 * counts while the probe binds a loopback address, and private and loopback endpoints count in
 * {@linkplain Builder#localDevelopment local development}.
 *
 * <p>Every step runs on the executor; engine events and signaling results hop to it. Nothing blocks a thread. The
 * attempt has its job's deadline, at most a minute, and a handshake of at most
 * {@value DiagnosticAdmissionCodec#MAX_HANDSHAKE_MILLIS} ms within it.
 */
public final class DiagnosticProbe {
    /** How often the probe rereads its authority and the STUN mapping while it waits. */
    private static final long TICK_MILLIS = 50;

    /**
     * What the caller authorized: the host, the attempt, its target and its deadline. Trusted local input, not
     * evidence that the target belongs to the host. An attempt ID and deadline are never issued twice.
     *
     * @param hostFingerprintHex The SHA-256 fingerprint of the host's DTLS certificate, 64 lowercase hex digits
     * @param expiresAt          Epoch milliseconds, a whole second
     */
    public record Job(Context context, String attemptIdHex, DiagnosticHostPolicy.Endpoint target,
                      String hostFingerprintHex, long expiresAt) {
        public Job {
            Objects.requireNonNull(context);
            Objects.requireNonNull(target);
            unhex(attemptIdHex, 16);
            unhex(hostFingerprintHex, 32);
            integer(expiresAt, 1000, 0xffffffffL * 1000);
            if (expiresAt % 1000 != 0) {
                throw invalid();
            }
        }
    }

    /**
     * The exact gathered offer the provider issues the permit for. Its ICE credentials must not be logged or kept.
     *
     * @param ufrag The probe's ICE ufrag, which the permit is bound to
     */
    public record Request(Context context, Claims claims, String ufrag, byte[] offer) {
        public Request {
            Objects.requireNonNull(context);
            Objects.requireNonNull(claims);
            DiagnosticAdmissionCodec.ufrag(ufrag);
            if (offer.length < 1 || offer.length > DiagnosticSdp.MAX_BYTES) {
                throw invalid();
            }
            offer = offer.clone();
        }

        @Override
        public byte[] offer() {
            return offer.clone();
        }

        @Override
        public String toString() {
            return "DiagnosticProbe.Request[redacted]";
        }
    }

    /**
     * The provider's authenticated HTTPS exchange: sends the offer, returns the host's answer SDP. It must reject
     * redirects and bound the body to {@value DiagnosticSdp#MAX_BYTES} bytes, and return without blocking.
     */
    @FunctionalInterface
    public interface Signaling {
        CompletionStage<String> exchange(Request request);
    }

    /** Where an attempt ended. */
    public enum Reason {
        /** The host echoed the PING on an allowed path, and the probe cleaned up. */
        COMPLETE,
        CANCELLED,
        /** The job's deadline passed, or the handshake ran out of time before the answer was verified. */
        EXPIRED,
        /** The caller's authority behind the job lapsed. */
        WITHDRAWN,
        /** The probe could not gather an offer the profile allows on its endpoint. */
        GATHERING,
        /** The provider's signaling failed. */
        SIGNALING,
        /** The provider's answer is not one for this host, profile and target. */
        ANSWER,
        /** The connection did not come up in time, or failed, after a verified answer. */
        TRANSPORT,
        /** The host broke the diagnostic protocol. */
        PROTOCOL,
        /** The selected pair is not between the probe's endpoint and an allowed destination. */
        SELECTED_PATH,
        /** The probe's peer or endpoint did not close cleanly. */
        CLEANUP
    }

    /**
     * How an attempt ended. Success needs the original PING echoed by the pinned host on an allowed path, a clean
     * close, and the authority still holding at the end.
     *
     * @param answerVerified       Whether the provider's answer passed the profile and pinning checks
     * @param transportEstablished Whether both data channels opened
     * @param pingVerified         Whether the PONG echoed the PING
     * @param attemptedRemote      The destination the verified answer named, also when no pair was selected
     */
    public record Result(Job job, Reason reason, boolean answerVerified, boolean transportEstablished,
                         boolean pingVerified, boolean cleanupComplete, String offerDigestHex,
                         String clientFingerprintHex, InetSocketAddress selectedLocal,
                         InetSocketAddress selectedRemote, InetSocketAddress attemptedRemote, int sentFrames,
                         int sentBytes, int receivedFrames, int receivedBytes, long completedAt) {
        /** Whether the host answered the probe. */
        public boolean success() {
            return reason == Reason.COMPLETE;
        }
    }

    /** Sets up a probe. */
    public static final class Builder {
        private final NetherNetEngine engine;
        private final EventExecutor executor;
        private final Job job;
        private final InetSocketAddress bind;
        private final BooleanSupplier authorized;
        private InetSocketAddress stunServer;
        private boolean localDevelopment;

        private Builder(NetherNetEngine engine, EventExecutor executor, Job job, InetSocketAddress bind,
                        BooleanSupplier authorized) {
            this.engine = Objects.requireNonNull(engine, "engine");
            this.executor = Objects.requireNonNull(executor, "executor");
            this.job = Objects.requireNonNull(job, "job");
            this.bind = Objects.requireNonNull(bind, "bind");
            this.authorized = Objects.requireNonNull(authorized, "authorized");
        }

        /**
         * For an assisted target: a numeric STUN server through which the probe learns the public mapping of its
         * port, which its offer then names instead of the bound address. Without one, an IPv4 probe behind NAT offers
         * its private address and the host learns the public source from the probe's check.
         */
        public Builder stunServer(InetSocketAddress stunServer) {
            this.stunServer = Objects.requireNonNull(stunServer, "stunServer");
            return this;
        }

        /**
         * Whether private and loopback endpoints count as destinations, for a provider, probe and host on one network.
         * Off by default; production probes leave it off.
         */
        public Builder localDevelopment(boolean localDevelopment) {
            this.localDevelopment = localDevelopment;
            return this;
        }

        /**
         * @throws IllegalArgumentException If the target, the bound endpoint or the STUN server is no destination of
         *                                  the target's family, or the bound endpoint has no fixed port
         */
        public DiagnosticProbe build() {
            return new DiagnosticProbe(this);
        }
    }

    /**
     * @param engine     Creates the probe's endpoint and peer; the probe closes only what it created
     * @param executor   Runs every step of the attempt
     * @param job        What the caller authorized
     * @param bind       The local endpoint the probe gathers on and checks from: a concrete address of the target's
     *                   family, without a scope, and a fixed port
     * @param authorized Holds while the caller's authority behind the job holds; read on the executor, must not block
     */
    public static Builder builder(NetherNetEngine engine, EventExecutor executor, Job job, InetSocketAddress bind,
                                  BooleanSupplier authorized) {
        return new Builder(engine, executor, job, bind, authorized);
    }

    private final NetherNetEngine engine;
    private final EventExecutor executor;
    private final Job job;
    private final InetSocketAddress bind, target, stunServer;
    private final boolean localDevelopment;
    private final BooleanSupplier authorized;
    private final AtomicBoolean started = new AtomicBoolean();
    private final CompletableFuture<Result> result = new CompletableFuture<>();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final DiagnosticChannels channels = new DiagnosticChannels();
    private volatile boolean cancelled;

    // Executor only.
    private Reason reason = Reason.GATHERING;
    private boolean done, answerVerified, transportEstablished;
    private long anchorWall, anchorNanos, deadlineNanos, currentWall;
    private final List<ScheduledFuture<?>> timers = new ArrayList<>();
    private IceEndpoint endpoint;
    private Peer peer;
    private volatile DataLink reliable;
    private volatile TransportPath path;
    private IceEndpoint.StunMonitor monitor;
    private InetSocketAddress mapping, gatheredLocal, selectedLocal, selectedRemote, attemptedRemote;
    private String gatheredOffer, ufrag, password, offerDigestHex, clientFingerprintHex;
    private Claims claims;
    private Signaling signaling;
    private CompletableFuture<String> pending;
    private DiagnosticExchange exchange;

    private DiagnosticProbe(Builder builder) {
        this.engine = builder.engine;
        this.executor = builder.executor;
        this.job = builder.job;
        this.authorized = builder.authorized;
        this.localDevelopment = builder.localDevelopment;
        int family = job.target().family();
        InetSocketAddress bound = builder.bind;
        if (bound.isUnresolved() || bound.getPort() < 1 || bound.getAddress().isAnyLocalAddress()
                || bound.getAddress().isMulticastAddress() || family(bound.getAddress()) != family
                || bound.getAddress() instanceof Inet6Address v6 && v6.getScopeId() != 0) {
            throw invalid();
        }
        this.bind = numeric(bound.getAddress(), bound.getPort());
        if (job.target().assisted()) {
            this.target = null;
        } else {
            byte[] address = unhex(job.target().addressHex(), 16);
            this.target = numeric(family == 4 ? Arrays.copyOfRange(address, 12, 16) : address, job.target().port());
            if (!destination(target.getAddress())) {
                throw invalid();
            }
        }
        InetSocketAddress server = builder.stunServer;
        if (server != null && (!job.target().assisted() || server.isUnresolved() || server.getPort() < 1
                || family(server.getAddress()) != family || !destination(server.getAddress()))) {
            throw invalid();
        }
        this.stunServer = server;
    }

    /** Completes once the probe's peer and endpoint are gone. */
    public CompletionStage<Void> termination() {
        return termination.minimalCompletionStage();
    }

    /**
     * Runs the attempt. Once only.
     *
     * @return Completes with the result once the attempt ended and cleaned up; never exceptionally
     * @throws IllegalStateException If the probe ran before
     */
    public CompletionStage<Result> run(Signaling signaling) {
        Objects.requireNonNull(signaling, "signaling");
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("A diagnostic probe runs once");
        }
        onExecutor(() -> {
            this.signaling = signaling;
            start();
        });
        return result.minimalCompletionStage();
    }

    /** Ends the attempt as {@link Reason#CANCELLED}; its peer closes even while the signaling is still out. */
    public void cancel() {
        cancelled = true;
        if (!started.getAndSet(true)) {
            termination.complete(null);
        }
        onExecutor(this::proceed);
    }

    // ---- Steps, on the executor ----

    private void start() {
        anchorWall = System.currentTimeMillis();
        anchorNanos = System.nanoTime();
        currentWall = anchorWall;
        long remaining = job.expiresAt() - anchorWall;
        if (remaining < 1 || remaining > MAX_ATTEMPT_MILLIS) {
            finish(Reason.EXPIRED);
            return;
        }
        deadlineNanos = anchorNanos + TimeUnit.MILLISECONDS.toNanos(remaining);
        long handshake = TimeUnit.MILLISECONDS.toNanos(Math.min(remaining, MAX_HANDSHAKE_MILLIS));
        timers.add(executor.schedule(this::handshakeTimedOut, handshake, TimeUnit.NANOSECONDS));
        timers.add(executor.scheduleWithFixedDelay(this::tick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS));
        if (!proceed()) {
            return;
        }
        engine.openEndpoint(new EndpointConfig(bind.getPort(), List.of(bind.getAddress())))
                .whenComplete((opened, failure) -> onExecutor(() -> opened(opened, failure),
                        () -> closeQuietly(opened)));
    }

    private void opened(IceEndpoint opened, Throwable failure) {
        if (done) {
            closeQuietly(opened);
            return;
        }
        if (failure != null) {
            finish(Reason.GATHERING);
            return;
        }
        endpoint = opened;
        ufrag = random(18);
        password = random(18);
        try {
            peer = engine.createPeer(new PeerConfig(PeerConfig.Role.OFFERER, List.of(),
                    new CandidatePolicy(job.target().family() == 6, List.of(bind.getAddress()), null), endpoint, null,
                    new IceCredentials(ufrag, password)), new Events());
            reliable = peer.openChannel(DiagnosticChannels.RELIABLE);
            peer.openChannel(DiagnosticChannels.UNRELIABLE);
        } catch (RuntimeException e) {
            finish(Reason.GATHERING);
            return;
        }
        if (stunServer != null) {
            endpoint.monitorStun(stunServer).whenComplete((stun, refused) -> onExecutor(() -> {
                if (done || refused != null) {
                    closeQuietly(stun);
                    if (!done) {
                        finish(Reason.GATHERING);
                    }
                } else {
                    monitor = stun;
                }
            }, () -> closeQuietly(stun)));
        }
        peer.offer().thenCompose(ignored -> peer.gatheredLocalDescription())
                .whenComplete((description, gatheringFailed) -> onExecutor(() -> {
                    if (gatheringFailed != null) {
                        finish(Reason.GATHERING);
                    } else {
                        gatheredOffer = description;
                        offer();
                    }
                }));
    }

    /** Sends the offer once it is gathered and, with a STUN server, the mapping is known. */
    private void offer() {
        if (done || gatheredOffer == null || stunServer != null && mapping == null || claims != null
                || !proceed()) {
            return;
        }
        int family = job.target().family();
        try {
            String text = gatheredOffer;
            if (job.target().assisted()) {
                InetSocketAddress candidate = stunServer == null ? bind : mapping;
                // An IPv4 probe behind NAT offers its private host address; the host learns its public source from
                // the probe's check and needs no STUN mapping from it
                boolean privateHost = family == 4 && stunServer == null
                        && EndpointAddress.scope(candidate.getAddress()) == EndpointAddress.Scope.PRIVATE;
                if (family(candidate.getAddress()) != family || !(destination(candidate.getAddress()) || privateHost)) {
                    throw invalid();
                }
                gatheredLocal = candidate;
                text = text.replace("a=candidate:" + field(text, "a=candidate:"), "a=candidate:1 1 UDP 2130706431 "
                        + candidate.getAddress().getHostAddress() + " " + candidate.getPort() + " typ "
                        + (stunServer == null ? "host" : "srflx"));
            }
            byte[] offer = text.getBytes(StandardCharsets.UTF_8);
            if (offer.length > DiagnosticSdp.MAX_BYTES) {
                throw invalid();
            }
            String fingerprint = field(text, "a=fingerprint:");
            if (!fingerprint.startsWith("sha-256 ")) {
                throw invalid();
            }
            offerDigestHex = hex(digest(offer));
            clientFingerprintHex = fingerprint.substring(8).replace(":", "").toLowerCase(Locale.ROOT);
            Claims offered = new Claims(job.expiresAt(), clientFingerprintHex, password, job.attemptIdHex(),
                    offerDigestHex, job.target().candidateRevision(), family, job.target().addressHex(),
                    job.target().port(),
                    job.target().assisted() ? ASSISTED_PROFILE : PROFILE);
            DiagnosticSdp sdp = DiagnosticSdp.offer(offer, offered, ufrag);
            if (!job.target().assisted() && !sdp.endpoint(family).equals(bind)) {
                throw invalid();
            }
            claims = offered;
            reason = Reason.SIGNALING;
            pending = Objects.requireNonNull(signaling.exchange(new Request(job.context(), offered, ufrag, offer)))
                    .toCompletableFuture();
            pending.whenComplete((answer, failure) -> onExecutor(() -> answered(answer, failure)));
        } catch (RuntimeException e) {
            finish(reason == Reason.SIGNALING ? Reason.SIGNALING : Reason.GATHERING);
        }
    }

    private void answered(String answer, Throwable failure) {
        pending = null;
        if (done || !proceed()) {
            return;
        }
        if (failure != null) {
            finish(Reason.SIGNALING);
            return;
        }
        reason = Reason.ANSWER;
        InetSocketAddress destination;
        try {
            if (answer == null || answer.length() > DiagnosticSdp.MAX_BYTES) {
                throw invalid();
            }
            destination = DiagnosticSdp.answer(answer.getBytes(StandardCharsets.UTF_8), claims,
                    job.hostFingerprintHex()).endpoint(job.target().family());
            if (!destination(destination.getAddress())) {
                throw invalid();
            }
        } catch (RuntimeException invalid) {
            finish(Reason.ANSWER);
            return;
        }
        answerVerified = true;
        attemptedRemote = destination;
        reason = Reason.TRANSPORT;
        peer.applyAnswer(answer).whenComplete((ignored, refused) -> onExecutor(() -> {
            if (refused != null) {
                finish(Reason.TRANSPORT);
            } else {
                step();
            }
        }));
    }

    /** Starts the exchange once the channels are up, then waits for the PONG. */
    private void step() {
        if (done || !proceed()) {
            return;
        }
        if (channels.protocolFailed()) {
            finish(Reason.PROTOCOL);
            return;
        }
        if (channels.failedTransport()) {
            finish(Reason.TRANSPORT);
            return;
        }
        if (!answerVerified) {
            return;
        }
        if (exchange == null) {
            if (!channels.ready()) {
                return;
            }
            transportEstablished = true;
            reason = Reason.PROTOCOL;
            // No PING goes to a destination the job does not allow
            if (!selectPath()) {
                finish(Reason.SELECTED_PATH);
                return;
            }
            DataLink link = reliable;
            exchange = new DiagnosticExchange(job.attemptIdHex(), false,
                    (channel, frame) -> link.send(ByteBuffer.wrap(frame), refused -> {
                        if (refused != null) {
                            channels.failed();
                            onExecutor(this::step);
                        }
                    }));
            try {
                exchange.start();
            } catch (RuntimeException e) {
                finish(Reason.TRANSPORT);
                return;
            }
        }
        byte[] frame = channels.poll();
        if (frame != null) {
            try {
                exchange.receive(0, frame);
            } catch (IllegalArgumentException wrong) {
                finish(Reason.PROTOCOL);
                return;
            }
        }
        if (exchange.complete()) {
            // The path is checked again before success is reported
            finish(selectPath() ? Reason.COMPLETE : Reason.SELECTED_PATH);
        }
    }

    private boolean selectPath() {
        TransportPath current = path;
        InetSocketAddress local = current == null ? null : numeric(current.local());
        InetSocketAddress remote = current == null ? null : numeric(current.remote());
        int family = job.target().family();
        if (local == null || remote == null || family(local.getAddress()) != family
                || family(remote.getAddress()) != family) {
            return false;
        }
        // The peer gathers on the bound socket only, so every local candidate is that socket or a mapping of it. A
        // probe behind NAT learns the mapping the host saw from the host's check responses, which libwebrtc then
        // reports as a local peer-reflexive candidate: on a direct probe from a private address, and on an assisted
        // one whose NAT showed the host another mapping than the STUN server saw. A host elsewhere sees a public
        // mapping; a private one means probe and host share a network, which only local development allows, so the
        // mapping must be a destination.
        boolean mapped = current.localType() == TransportPath.CandidateType.PEER_REFLEXIVE
                && destination(local.getAddress());
        boolean localAllowed = local.equals(bind) || local.equals(gatheredLocal) || mapped;
        boolean remoteAllowed = target != null ? remote.equals(target) : destination(remote.getAddress());
        if (!localAllowed || !remoteAllowed) {
            return false;
        }
        selectedLocal = local;
        selectedRemote = remote;
        return true;
    }

    private void handshakeTimedOut() {
        if (!done && !transportEstablished) {
            finish(answerVerified ? Reason.TRANSPORT : Reason.EXPIRED);
        }
    }

    private void tick() {
        if (done || !proceed()) {
            return;
        }
        IceEndpoint.StunMonitor current = monitor;
        if (current != null && mapping == null) {
            current.read().whenComplete((reading, failure) -> onExecutor(() -> {
                if (done || mapping != null || failure != null || reading.state()
                        != IceEndpoint.StunReading.State.SUCCEEDED || reading.mapped() == null) {
                    return;
                }
                mapping = numeric(reading.mapped());
                if (mapping == null) {
                    finish(Reason.GATHERING);
                    return;
                }
                closeQuietly(monitor);
                monitor = null;
                offer();
            }));
        }
    }

    /**
     * Whether the attempt may go on: not cancelled, before its deadline on a sane clock, and authorized. Ends it
     * otherwise.
     */
    private boolean proceed() {
        if (done) {
            return false;
        }
        if (cancelled) {
            finish(Reason.CANCELLED);
            return false;
        }
        if (!clockHolds()) {
            // A job shorter than the handshake limit ends with it; a verified answer then means the transport failed
            finish(answerVerified && !transportEstablished ? Reason.TRANSPORT : Reason.EXPIRED);
            return false;
        }
        if (!authorized.getAsBoolean()) {
            finish(Reason.WITHDRAWN);
            return false;
        }
        return true;
    }

    /** Reads the wall clock anchored to the monotonic one, as the host does, and checks the deadline. */
    private boolean clockHolds() {
        long nanos = System.nanoTime(), wall = System.currentTimeMillis();
        if (wall < 0 || nanos - anchorNanos < 0) {
            return false;
        }
        currentWall = Math.max(wall, anchorWall + (nanos - anchorNanos) / 1_000_000L);
        anchorWall = currentWall;
        anchorNanos = nanos;
        return nanos - deadlineNanos < 0 && currentWall < job.expiresAt();
    }

    /** Ends the attempt: stops waiting, closes the peer and then the endpoint, and reports. */
    private void finish(Reason ending) {
        if (done) {
            return;
        }
        done = true;
        reason = ending;
        timers.forEach(timer -> timer.cancel(false));
        if (pending != null) {
            pending.cancel(false);
        }
        closeQuietly(monitor);
        boolean pinged = exchange != null && exchange.complete();
        CompletableFuture<Void> peerClosed = peer == null ? CompletableFuture.completedFuture(null)
                : peer.close().toCompletableFuture();
        peerClosed.thenCompose(ignored -> endpoint == null ? CompletableFuture.<Void>completedFuture(null)
                        : endpoint.close().toCompletableFuture())
                .whenComplete((ignored, failure) -> onExecutor(() -> report(pinged, failure == null),
                        () -> report(pinged, failure == null)));
    }

    private void report(boolean pinged, boolean clean) {
        Reason ending = clean ? reason : Reason.CLEANUP;
        // The authority must still hold once everything is gone, or the observation does not count
        if (ending == Reason.COMPLETE && (cancelled || !clockHolds() || !authorized.getAsBoolean())) {
            ending = cancelled ? Reason.CANCELLED : !clockHolds() ? Reason.EXPIRED : Reason.WITHDRAWN;
        }
        channels.clear();
        DiagnosticExchange current = exchange;
        result.complete(new Result(job, ending, answerVerified, transportEstablished, pinged, clean,
                offerDigestHex, clientFingerprintHex, selectedLocal, selectedRemote, attemptedRemote,
                current == null ? 0 : current.sentFrames(), current == null ? 0 : current.sentBytes(),
                channels.receivedFrames(), channels.receivedBytes(), currentWall));
        termination.complete(null);
    }

    private boolean destination(InetAddress address) {
        return switch (EndpointAddress.scope(address)) {
            case PUBLIC -> true;
            case PRIVATE -> localDevelopment;
            case LOOPBACK -> localDevelopment || bind.getAddress().isLoopbackAddress();
            case UNUSABLE -> false;
        };
    }

    private void onExecutor(Runnable task) {
        onExecutor(task, () -> { });
    }

    private void onExecutor(Runnable task, Runnable rejected) {
        if (executor.inEventLoop()) {
            task.run();
            return;
        }
        try {
            executor.execute(task);
        } catch (RejectedExecutionException shutDown) {
            rejected.run();
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            // Nothing left to report it to
        }
    }

    private static void closeQuietly(IceEndpoint endpoint) {
        if (endpoint != null) {
            endpoint.close();
        }
    }

    private static int family(InetAddress address) {
        return address instanceof Inet6Address ? 6 : 4;
    }

    private static InetSocketAddress numeric(InetSocketAddress address) {
        return address == null || address.isUnresolved() || address.getPort() < 1 ? null
                : numeric(address.getAddress(), address.getPort());
    }

    private static InetSocketAddress numeric(InetAddress address, int port) {
        return numeric(address.getAddress(), port);
    }

    private static InetSocketAddress numeric(byte[] address, int port) {
        try {
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        } catch (UnknownHostException malformed) {
            throw invalid();
        }
    }

    private static String random(int bytes) {
        byte[] value = new byte[bytes];
        new SecureRandom().nextBytes(value);
        return base64(value);
    }

    private static String field(String text, String prefix) {
        List<String> values = text.lines().filter(line -> line.startsWith(prefix)).toList();
        if (values.size() != 1) {
            throw invalid();
        }
        return values.getFirst().substring(prefix.length());
    }

    /** The probe's own peer: what the engine reports, then a step on the executor. */
    private final class Events implements PeerListener {
        @Override
        public void onChannelOpen(DataLink link) {
            try {
                channels.opened(link.parameters());
            } catch (IllegalArgumentException wrongChannel) {
                // The channels record the violation
            }
            onExecutor(DiagnosticProbe.this::step);
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            // The probe opens both channels itself; a host that opens one breaks the profile
            channels.violated();
            onExecutor(DiagnosticProbe.this::step);
            return false;
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            channels.received(link == reliable, message);
            onExecutor(DiagnosticProbe.this::step);
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
            channels.failed();
            onExecutor(DiagnosticProbe.this::step);
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
            onExecutor(DiagnosticProbe.this::step);
        }

        @Override
        public void onClosed(CloseCause cause) {
            channels.failed();
            onExecutor(DiagnosticProbe.this::step);
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

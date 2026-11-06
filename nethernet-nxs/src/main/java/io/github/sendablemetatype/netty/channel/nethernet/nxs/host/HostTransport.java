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

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.ice.StunObserver;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionContext;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionPrincipal;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionRequest;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AssistedAdmission;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionValidator;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.VerifiedAdmission;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.EndpointConnectivityController;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.EndpointSelection;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.MaintainedCandidatePublisher;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.ProviderEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticHostPolicy;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AssistedAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ConnectivityCheck;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfile;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfileSnapshot;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ProfileReplacedException;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ProviderTransport;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.StunServer;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSink;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.RemoteSignal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.ReplyPath;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SharedIcePort;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingHealth;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingState;
import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import io.github.sendablemetatype.netty.util.nethernet.DtlsCertificate;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The NXS host side on one NetherNet endpoint (rulings A-1, A-2, G-6 item 3 and H-1). It is the
 * {@link ProviderTransport} the provider client drives, and through {@link #source()} a {@link JoinSource} of the
 * endpoint: both NXS paths become joins of that endpoint, so they pass its one admission stage and capacity budget,
 * and their children are ordinary children of the endpoint.
 * <ul>
 * <li><b>Stateless joins.</b> It claims the checks the shared ICE port holds for unknown ufrags, with the admission
 * keys installed as the port's derived-password prefilter, so only checks signed by an installed key reach Java. Each
 * one's token is validated once, recorded in the {@link TicketLedger}, and delivered as an authenticated join whose
 * peer answers the offer the token implies with the token as its ufrag and the derived password.</li>
 * <li><b>Assisted joins.</b> A join the provider pushes is delivered the same way, with the provider's offer and
 * credentials; its answer goes back through the provider's control socket, cut to one candidate on the gameplay port
 * as upstream builds it, with {@code m=} and {@code c=} naming that candidate.</li>
 * <li><b>Diagnostics.</b> A ticket with network ID 0 is a connectivity check, never a player: it goes to the
 * {@link DiagnosticHost} before any player reservation, which answers its PING on a peer of its own from the shared
 * port, with no child (diagnostic-v1). The provider client installs the policy that admits checks after each
 * successful heartbeat; {@link #pollDiagnosticResults} reports how they ended.</li>
 * <li><b>Profile.</b> It publishes the shared port's candidates (configured, local, or maintained with STUN through
 * the provider's servers), the fingerprint of the one DTLS certificate every NXS peer presents, and the admission
 * parameters.</li>
 * </ul>
 * Its state lives on the endpoint's event loop. A login still undecided at its ticket's deadline closes the child:
 * the application must decide it with {@code TransportIdentityBinding.verifyLogin} (area 9, finding F1). Closing
 * this transport stops admitting, revokes pending logins and closes the children they belong to; players whose login
 * was accepted stay connected (ruling A-1).
 */
public final class HostTransport implements ProviderTransport {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(HostTransport.class);

    /** libwebrtc's SCTP port, which every NetherNet description carries. */
    static final int SCTP_PORT = 5000;
    /** How far ahead of this host's clock a ticket may expire. */
    static final long MAX_TICKET_MILLIS = 120_000;
    static final int MAX_OUTCOMES = 256;
    static final int MAX_ASSISTED = 32;
    /** How long an assisted join may wait for its answer candidate from STUN. */
    static final Duration GATHERING = Duration.ofSeconds(15);
    static final Duration MAINTENANCE = Duration.ofMillis(100);
    static final Duration REFRESH = Duration.ofSeconds(1);
    static final Duration STUN_MAX_AGE = Duration.ofMinutes(5);
    /** The longest a heartbeat's diagnostic policy lasts, as the provider client installs it. */
    static final Duration MAX_DIAGNOSTIC_POLICY = Duration.ofMinutes(5);

    /** How the host finds the candidates it publishes. */
    enum Publication {
        /** The bound addresses plus the configured endpoints, without STUN. */
        FIXED,
        /** Only the configured endpoints, which name the PROXY protocol relays players come through. */
        RELAYED,
        /** Configured or public local endpoints, kept current with STUN mappings and the provider's feedback. */
        MAINTAINED
    }

    /**
     * What the host configured.
     *
     * @param advertised       The configured endpoints, empty to derive them
     * @param localDevelopment Whether loopback endpoints may be published, for a provider on this machine
     * @param stunWarming      Whether maintained publication keeps STUN mappings warm in the background
     */
    record Settings(Publication publication, List<InetSocketAddress> advertised, boolean localDevelopment,
                    boolean assistedJoins, boolean stunWarming, TicketLedger.Limits limits) {
        Settings {
            advertised = List.copyOf(advertised);
            Objects.requireNonNull(publication, "publication");
            Objects.requireNonNull(limits, "limits");
            if (assistedJoins && publication != Publication.MAINTAINED) {
                throw new IllegalArgumentException("Assisted joins need maintained publication");
            }
        }
    }

    private record Epoch(String id, long retireAfter) {
    }

    private enum Kind {
        STATELESS, ASSISTED
    }

    /** One admitted ticket, from its reservation until its peer's teardown completed. Event loop. */
    private static final class Session {
        final TicketLedger.Reservation reservation;
        final AdmissionContext admission;
        final Kind kind;
        NetherNetChildChannel child;
        boolean reporting, reported;
        String failure;

        Session(TicketLedger.Reservation reservation, AdmissionContext admission, Kind kind) {
            this.reservation = reservation;
            this.admission = admission;
            this.kind = kind;
        }
    }

    /** An assisted join from delivery until its session ends. Event loop. */
    private final class Assisted {
        final AssistedJoin join;
        final Session session;
        final long deadlineNanos;
        final OwnershipGuard guard;
        final CompletableFuture<AssistedAnswer> result;
        final CompletableFuture<Void> abandoned = new CompletableFuture<>();
        CompletableFuture<HostCandidate> candidate;
        boolean done;

        Assisted(AssistedJoin join, Session session, long deadlineNanos, OwnershipGuard guard,
                 CompletableFuture<AssistedAnswer> result) {
            this.join = join;
            this.session = session;
            this.deadlineNanos = deadlineNanos;
            this.guard = guard;
            this.result = result;
        }

        /** Holds while the provider authority, this host's material and the join's deadline all hold. */
        void requireCurrent() {
            guard.requireCurrent();
            requireAssistable(join);
            if (nanos.getAsLong() - deadlineNanos >= 0) {
                throw new IllegalStateException("Assisted join deadline passed");
            }
        }

        /** Stops gathering and fails what has not completed. Event loop. */
        void end(Throwable failure) {
            done = true;
            if (candidate != null) {
                candidate.completeExceptionally(failure);
            }
            result.completeExceptionally(failure);
            abandoned.complete(null);
        }
    }

    /** The search for the STUN mapping an assisted answer names. Event loop. */
    private static final class Gathering {
        final CompletableFuture<HostCandidate> candidate = new CompletableFuture<>();
        final AddressFamily family;
        final long deadlineNanos;
        IceEndpoint.StunMonitor monitor;
        boolean reading;

        Gathering(AddressFamily family, long deadlineNanos) {
            this.family = family;
            this.deadlineNanos = deadlineNanos;
        }

        void closeMonitor() {
            IceEndpoint.StunMonitor current = monitor;
            monitor = null;
            if (current != null) {
                current.close();
            }
        }
    }

    private final Settings settings;
    private final DtlsCertificate certificate;
    /** The certificate's fingerprint, as the profile publishes it and assisted joins name it. */
    private final DtlsFingerprint fingerprint;
    private final EventExecutor loop;
    private final LongSupplier millis = System::currentTimeMillis;
    private final LongSupplier nanos = System::nanoTime;
    private final Source source = new Source();
    private final String incarnation;
    private final StatelessAdmissionValidator validator;
    private final TicketLedger ledger;
    private final ArrayBlockingQueue<TransportOutcome> outcomes = new ArrayBlockingQueue<>(MAX_OUTCOMES);
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final List<Consumer<SignalingHealth>> healthListeners = new CopyOnWriteArrayList<>();
    private volatile SignalingHealth health = new SignalingHealth(SignalingState.NEW, 0, null, null);
    private volatile List<Epoch> epochs = List.of();
    private volatile boolean draining;
    /**
     * Set once, under {@link #lifecycle}, which key installation holds too: an installation either finishes before
     * close wipes the keys or installs nothing.
     */
    private volatile boolean closing;
    private final Object lifecycle = new Object();
    /** Changes with the candidate material, read by guards on any thread. */
    private volatile long candidateRevision = 1;
    private volatile long publicationVersion;

    // Event loop only.
    private final Map<TicketLedger.Reservation, Session> sessions = new HashMap<>();
    /** Live assisted joins by their local ufrag, whose checks their own peer takes. */
    private final Map<String, Assisted> assisted = new HashMap<>();
    /** Answer candidates still being found. */
    private final Set<Gathering> gatherings = new HashSet<>();
    /** Connectivity checks, from the start on; read by {@link #pollDiagnosticResults} on any thread. */
    private volatile DiagnosticHost diagnostics;
    private final List<ScheduledFuture<?>> tasks = new ArrayList<>();
    private JoinSink sink;
    private SharedIcePort port;
    private SharedIcePort.HeldChecks heldChecks;
    private volatile MaintainedCandidatePublisher publisher;
    private MaintainedCandidatePublisher.Publication publication;
    private boolean withdrawn;
    private long mappingRevision;
    private Set<HostCandidate> fixedCandidates;
    /** The addresses the shared port listens on, gathered when NXS started. */
    private List<InetAddress> hostAddresses = List.of();

    /**
     * @param loop The endpoint's event loop, which runs the source's handlers
     */
    HostTransport(Settings settings, DtlsCertificate certificate, EventExecutor loop) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.certificate = Objects.requireNonNull(certificate, "certificate");
        this.fingerprint = DtlsFingerprint.of(certificate.sha256());
        this.loop = Objects.requireNonNull(loop, "loop");
        byte[] nonce = new byte[16];
        new SecureRandom().nextBytes(nonce);
        this.incarnation = HexFormat.of().formatHex(nonce);
        this.validator = new StatelessAdmissionValidator(StatelessAdmissionValidator.audience(incarnation),
                MAX_TICKET_MILLIS);
        this.ledger = new TicketLedger(settings.limits());
    }

    /** The join source to add to the endpoint, which starts it once its shared ICE port is open. */
    public JoinSource source() {
        return source;
    }

    /** Completes once the endpoint started this source and it holds the shared port's held checks. */
    public CompletionStage<Void> started() {
        return started.minimalCompletionStage();
    }

    /** Completes once this transport closed. */
    CompletionStage<Void> whenClosed() {
        return closed.minimalCompletionStage();
    }

    /** The incarnation tokens are issued for, fresh for every transport. */
    String incarnation() {
        return incarnation;
    }

    TicketLedger.Stats ledgerStats() {
        return ledger.stats();
    }

    /** The IDs of the admission keys the validator holds. */
    Set<String> installedKeyIds() {
        return validator.keyIds();
    }

    // ---- Join source ----

    private void startOnLoop(JoinSink sink) {
        if (closing) {
            started.completeExceptionally(new IllegalStateException("The NXS host transport is closed"));
            return;
        }
        try {
            this.sink = sink;
            this.port = sink.sharedIcePort().orElseThrow(() -> new IllegalStateException(
                    "NXS needs the endpoint's shared ICE port: the provider publishes one port for every player"));
            this.diagnostics = new DiagnosticHost(port, certificate.dtls(), fingerprint, ledger, loop,
                    new DiagnosticAdmissionCodec.Clock(millis, nanos),
                    (family, deadline) -> answerCandidate(List.of(family), deadline), this::reachable);
            this.heldChecks = port.claimHeldChecks(this::held);
            // Mirrors the installed keys now and after every change; none installed passes no check on.
            validator.mirrorKeys(heldChecks::admit);
        } catch (RuntimeException e) {
            failStart(e);
            return;
        }
        // Binds the port on every address it publishes before any client can be told about one
        port.gatherHostAddresses().whenComplete((addresses, failure) -> onLoop(() -> {
            if (failure != null) {
                log.warn("NXS could not gather on the shared ICE port, so it publishes only configured endpoints: {}",
                        failure.toString());
            }
            finishStart(addresses == null ? List.of() : addresses);
        }));
    }

    private void finishStart(List<InetSocketAddress> gathered) {
        if (closing) {
            return;
        }
        try {
            hostAddresses = gathered.stream().map(InetSocketAddress::getAddress).toList();
            if (settings.publication() == Publication.MAINTAINED) {
                startMaintained(gathered);
            }
        } catch (IOException | RuntimeException e) {
            failStart(e);
            return;
        }
        schedule(this::maintain, MAINTENANCE);
        schedule(() -> validator.retireKeys(millis.getAsLong()), REFRESH);
        health(SignalingState.CONNECTED, null);
        started.complete(null);
    }

    private void failStart(Exception e) {
        log.error("NXS could not start on the NetherNet endpoint: {}", e.getMessage());
        health(SignalingState.CLOSED, e);
        started.completeExceptionally(e);
        close();
    }

    private void startMaintained(List<InetSocketAddress> gathered) throws IOException {
        InetSocketAddress bind = port.localAddress();
        // The addresses the endpoint gathered on are the ones its port listens on, unlike every interface's
        List<EndpointSelection.Candidate> hosts = gathered.stream()
                .map(address -> new EndpointSelection.Candidate(address, EndpointSelection.Provenance.NATIVE_HOST))
                .toList();
        EndpointSelection selection = settings.advertised().isEmpty()
                ? EndpointSelection.select(bind, List.of(), hosts)
                : EndpointSelection.select(bind, settings.advertised(), List.of());
        // Assistance gathers a mapping per join instead of keeping one warm, as upstream does
        EndpointConnectivityController controller = selection.configured() || settings.assistedJoins()
                || !settings.stunWarming() ? null
                : new EndpointConnectivityController(selection, Map.of(), STUN_MAX_AGE,
                        StunObserver.sharedPort(port::monitorStun,
                                task -> Thread.ofVirtual().name("nxs-stun-lookup").start(task)));
        publisher = new MaintainedCandidatePublisher(selection, controller, settings.assistedJoins());
        refreshMaintained();
        schedule(this::refreshMaintained, REFRESH);
    }

    private void schedule(Runnable task, Duration period) {
        long nanosPeriod = period.toNanos();
        tasks.add(loop.scheduleWithFixedDelay(() -> {
            try {
                task.run();
            } catch (RuntimeException e) {
                log.warn("An NXS host task failed: {}", e.toString());
            }
        }, nanosPeriod, nanosPeriod, TimeUnit.NANOSECONDS));
    }

    private void health(SignalingState state, Throwable failure) {
        SignalingHealth next = new SignalingHealth(state, failure == null ? 0 : 1, failure, null);
        health = next;
        for (Consumer<SignalingHealth> listener : healthListeners) {
            try {
                listener.accept(next);
            } catch (RuntimeException e) {
                log.debug("An NXS health listener failed: {}", e.toString());
            }
        }
    }

    /** This transport as one of the endpoint's join sources. */
    private final class Source implements JoinSource {
        @Override
        public void start(JoinSink sink) {
            Objects.requireNonNull(sink, "sink");
            onLoop(() -> startOnLoop(sink));
        }

        @Override
        public SignalingHealth health() {
            return health;
        }

        @Override
        public void addHealthListener(Consumer<SignalingHealth> listener) {
            healthListeners.add(Objects.requireNonNull(listener, "listener"));
        }

        @Override
        public void removeHealthListener(Consumer<SignalingHealth> listener) {
            healthListeners.remove(listener);
        }

        /** NXS peers need no STUN or TURN servers: the provider handed the client this host's candidates. */
        @Override
        public List<IceServer> iceServers() {
            return List.of();
        }

        /** Closes the transport, as {@link HostTransport#close()} does. */
        @Override
        public void close() {
            HostTransport.this.close();
        }
    }

    // ---- Stateless joins ----

    /** A check no peer has taken, whose password a key the prefilter holds derived. Event loop. */
    private void held(IceEndpoint.HeldCheck check) {
        if (closing || draining || !ledger.isServing()) {
            check.reject();
            return;
        }
        if (assisted.containsKey(check.localUfrag()) || diagnostics.awaits(check.localUfrag())) {
            // The assisted join's own peer takes it once its port is ready
            return;
        }
        AdmissionRequest request;
        try {
            request = new AdmissionRequest(check.localUfrag(), check.remoteUfrag(), check.remoteAddress());
        } catch (IllegalArgumentException e) {
            ledger.invalidNativeRequest();
            check.reject();
            return;
        }
        long now = millis.getAsLong();
        VerifiedAdmission verified = validator.validate(request, now);
        if (verified == null) {
            ledger.invalidNativeRequest();
            check.reject();
            return;
        }
        if (verified.diagnostic()) {
            // Only the authenticated token says what a check is for, so network ID 0 never reaches a player
            // reservation or the endpoint
            if (!diagnostics.admit(check, verified)) {
                check.reject();
            }
            return;
        }
        TicketLedger.Reservation reservation = ledger.reserve(verified, request.address(), verified.expiresAt(), now,
                nanos.getAsLong());
        AdmissionContext admission = reservation == null ? null : ledger.admission(reservation);
        if (admission == null) {
            check.reject();
            return;
        }
        Session session = new Session(reservation, admission, Kind.STATELESS);
        deliver(session, new SourcedAddress(check.remoteAddress(), AddressSource.NXS_ADMISSION, null),
                new StatelessReply(check, session));
    }

    /** Hands an admitted ticket to the endpoint, which applies its throttle and budget and creates the child. */
    private void deliver(Session session, SourcedAddress source, ReplyPath reply) {
        AdmissionContext admission = session.admission;
        Join join = new Join(connectionId(admission.ticketId()), admission.networkId(), admission.remoteDescription(),
                source, reply, null, new Join.Authenticated(
                new IceCredentials(admission.localUfrag(), admission.localPassword()), certificate.dtls(),
                TransportIdentityBinding.of(ledger.verifier(session.reservation)), child -> created(session, child)));
        sink.join(join);
    }

    /** The NetherNet connection ID of a ticket: the first eight bytes of its ID, so logs can tie them together. */
    private static long connectionId(TicketId ticket) {
        return Long.parseUnsignedLong(ticket.value().substring(0, 16), 16);
    }

    /** The endpoint created the child of an admitted ticket, before exposing it. Event loop. */
    private void created(Session session, NetherNetChildChannel child) {
        if (closing || !ledger.ready(session.reservation)) {
            throw new IllegalStateException("The NXS admission expired or was withdrawn");
        }
        session.child = child;
        sessions.put(session.reservation, session);
        child.attr(AdmissionPrincipal.KEY).set(AdmissionPrincipal.of(session.admission));
        child.termination().whenComplete((ignored, failure) -> onLoop(() -> terminated(session)));
        if (session.kind == Kind.STATELESS) {
            // The prefilter verified the check's integrity with the derived password before it held it
            emit(session, OutcomeStage.ICE_SEEN, "token_and_stun_validated", null);
        }
    }

    /** The child's teardown completed: the ticket is done with. Event loop. */
    private void terminated(Session session) {
        sessions.remove(session.reservation);
        ledger.finish(session.reservation);
        if (session.kind == Kind.ASSISTED) {
            assisted.remove(session.admission.localUfrag());
        }
        if (!session.reported) {
            emit(session, OutcomeStage.FAILED, session.failure == null ? "closed" : session.failure, null);
        }
    }

    /** No peer was created for the ticket: the endpoint refused it, or the child failed before the source saw it. */
    private void withoutChild(Session session) {
        if (session.child == null) {
            ledger.finish(session.reservation);
        }
    }

    /**
     * The way back for a stateless join: none. The client built the host's answer itself from the published
     * profile, so the answer goes nowhere, and a refusal rejects the held check.
     */
    private final class StatelessReply implements ReplyPath {
        private final IceEndpoint.HeldCheck check;
        private final Session session;

        StatelessReply(IceEndpoint.HeldCheck check, Session session) {
            this.check = check;
            this.session = session;
        }

        @Override
        public boolean trickles() {
            return false;
        }

        @Override
        public CompletionStage<Void> answer(String sdp) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> candidate(String candidate) {
            throw new UnsupportedOperationException("A stateless NXS join does not trickle");
        }

        @Override
        public CompletionStage<Void> refuse(JoinRefusal refusal) {
            check.reject();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onRemoteSignal(Consumer<RemoteSignal> handler) {
            // A stateless client signals nothing after its first check
        }

        @Override
        public CompletionStage<Void> abandoned() {
            // The ledger's handshake window and the endpoint's handshake timeout end a join that never connects
            return new CompletableFuture<>();
        }

        @Override
        public void finished() {
            onLoop(() -> withoutChild(session));
        }
    }

    // ---- Assisted joins ----

    @Override
    public boolean supportsAssistedJoins() {
        return settings.assistedJoins();
    }

    @Override
    public CompletionStage<AssistedAnswer> assistedJoin(AssistedJoin join, long deadlineNanos, OwnershipGuard guard) {
        Objects.requireNonNull(join, "join");
        Objects.requireNonNull(guard, "guard");
        CompletableFuture<AssistedAnswer> result = new CompletableFuture<>();
        if (!settings.assistedJoins()) {
            result.completeExceptionally(new UnsupportedOperationException("Assisted joins are off"));
            return result;
        }
        onLoop(() -> {
            try {
                serveAssisted(join, deadlineNanos, guard, result);
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }, () -> result.completeExceptionally(new IllegalStateException("The NetherNet endpoint is closed")));
        return result;
    }

    /** Checks the join against this host, records it and delivers it to the endpoint. Event loop. */
    private void serveAssisted(AssistedJoin join, long deadlineNanos, OwnershipGuard guard,
                               CompletableFuture<AssistedAnswer> result) {
        guard.requireCurrent();
        requireAssistable(join);
        if (join.diagnostic()) {
            diagnostics.assist(join, deadlineNanos, guard).whenComplete((answer, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(failure);
                } else {
                    result.complete(answer);
                }
            });
            return;
        }
        long remaining = deadlineNanos - nanos.getAsLong();
        if (remaining <= 0) {
            throw new IllegalStateException("Assisted join deadline passed");
        }
        if (assisted.size() >= MAX_ASSISTED || assisted.containsKey(join.localUfrag())) {
            throw new IllegalStateException("Too many assisted joins, or a duplicate one");
        }
        long expiresAt = millis.getAsLong() + TimeUnit.NANOSECONDS.toMillis(remaining);
        AssistedAdmission admission = AssistedAdmission.of(join, expiresAt, () -> {
            try {
                guard.requireCurrent();
                return nanos.getAsLong() - deadlineNanos < 0;
            } catch (RuntimeException lapsed) {
                return false;
            }
        });
        // A provider may hand over a ufrag the stateless path admits, valid for longer than the join
        long tokenExpiresAt = validator.expiry(join.localUfrag(), admission.remoteUfrag());
        TicketLedger.Reservation reservation = ledger.reserve(admission, null, tokenExpiresAt, millis.getAsLong(),
                nanos.getAsLong());
        if (reservation == null) {
            throw new IllegalStateException("The assisted join was refused: replayed, or no room for its record");
        }
        Session session = new Session(reservation, admission, Kind.ASSISTED);
        Assisted state = new Assisted(join, session, deadlineNanos, guard, result);
        Set<AddressFamily> families = new LinkedHashSet<>();
        admission.offer().candidates().forEach(candidate -> families.add(AddressFamily.of(candidate.getAddress())));
        try {
            state.candidate = answerCandidate(families, nanos.getAsLong() + Math.min(GATHERING.toNanos(), remaining));
        } catch (RuntimeException e) {
            ledger.finish(reservation);
            throw e;
        }
        assisted.put(join.localUfrag(), state);
        state.candidate.whenComplete((candidate, failure) -> {
            if (failure != null) {
                onLoop(() -> {
                    if (!state.done) {
                        state.end(failure);
                    }
                });
            }
        });
        loop.schedule(() -> {
            if (!state.done && !result.isDone()) {
                state.end(new IllegalStateException("Assisted join deadline passed"));
            }
        }, remaining, TimeUnit.NANOSECONDS);
        // The client claims its candidates, so its address stays unknown until ICE settles
        deliver(session, SourcedAddress.unknown(), new AssistedReply(state));
    }

    /** Whether this host can serve the join: its incarnation, fingerprint and an installed key. Any thread. */
    private void requireAssistable(AssistedJoin join) {
        long now = millis.getAsLong();
        if (closing || draining || port == null || !ledger.isServing()
                || !incarnation.equals(join.incarnation())
                || !fingerprint.equals(join.hostFingerprint())
                || !validator.keyIds().contains(join.keyId())
                || epochs.stream().noneMatch(epoch -> epoch.id().equals(join.keyId())
                && epoch.retireAfter() > now && epoch.retireAfter() >= join.expiresAt())) {
            throw new IllegalStateException("This host cannot serve the assisted join");
        }
    }

    /**
     * Finds the one candidate an assisted answer names, for an assisted join or check: a {@linkplain #reachable
     * reachable} host candidate of the first family the client offered that has one, else a fresh STUN mapping of the
     * gameplay port through the provider's server for such a family. Completing the stage from outside stops the
     * search. Event loop.
     *
     * @throws IllegalStateException If no family has a host candidate or a server
     */
    private CompletableFuture<HostCandidate> answerCandidate(Collection<AddressFamily> families, long deadlineNanos) {
        Map<AddressFamily, HostCandidate> hosts = new EnumMap<>(AddressFamily.class);
        if (publication != null) {
            // With assistance on, the offered candidates hold every probe target too, failed ones included
            for (HostCandidate candidate : publication.candidates()) {
                if (candidate.type() == CandidateType.HOST && reachable(candidate.endpoint().getAddress())) {
                    hosts.putIfAbsent(candidate.family(), candidate);
                }
            }
        }
        for (AddressFamily family : families) {
            HostCandidate host = hosts.get(family);
            if (host != null) {
                return CompletableFuture.completedFuture(host);
            }
        }
        MaintainedCandidatePublisher current = publisher;
        Map<AddressFamily, InetSocketAddress> servers = current == null ? Map.of() : current.assistedStunServers();
        AddressFamily family = families.stream().filter(servers::containsKey).findFirst()
                .orElseThrow(() -> new IllegalStateException("No eligible assisted discovery endpoint"));
        Gathering gathering = new Gathering(family, deadlineNanos);
        gatherings.add(gathering);
        gathering.candidate.whenComplete((candidate, failure) -> onLoop(() -> {
            gatherings.remove(gathering);
            gathering.closeMonitor();
        }));
        port.monitorStun(servers.get(family)).whenComplete((monitor, failure) -> onLoop(() -> {
            if (failure != null) {
                gathering.candidate.completeExceptionally(failure);
            } else if (gathering.candidate.isDone() || closing) {
                monitor.close();
            } else {
                gathering.monitor = monitor;
            }
        }, monitor == null ? () -> { } : monitor::close));
        return gathering.candidate;
    }

    /**
     * Whether a client elsewhere reaches this address, for an assisted answer to name it or a diagnostic to use it: a
     * public one, as the specification asks of production hosts; a loopback one while the port is bound to loopback;
     * and a private or loopback one in local development, whose provider runs on this machine and whose clients on
     * the same network.
     */
    private boolean reachable(InetAddress address) {
        return switch (EndpointAddress.scope(address)) {
            case PUBLIC -> true;
            case PRIVATE -> settings.localDevelopment();
            case LOOPBACK -> settings.localDevelopment() || port.localAddress().getAddress().isLoopbackAddress();
            case UNUSABLE -> false;
        };
    }

    /** Reads the answer candidates' monitors and fails the searches out of time. Event loop, from maintenance. */
    private void gather() {
        long now = nanos.getAsLong();
        for (Gathering gathering : List.copyOf(gatherings)) {
            if (gathering.candidate.isDone()) {
                continue;
            }
            if (now - gathering.deadlineNanos >= 0) {
                gathering.candidate.completeExceptionally(
                        new IllegalStateException("No STUN mapping for the assisted answer in time"));
                continue;
            }
            if (gathering.monitor == null || gathering.reading) {
                continue;
            }
            gathering.reading = true;
            gathering.monitor.read().whenComplete((reading, failure) -> onLoop(() -> {
                gathering.reading = false;
                if (gathering.candidate.isDone()) {
                    return;
                }
                if (failure != null) {
                    gathering.candidate.completeExceptionally(failure);
                    return;
                }
                if (reading.state() != IceEndpoint.StunReading.State.SUCCEEDED || reading.mapped() == null) {
                    return;
                }
                InetSocketAddress mapped = reading.mapped();
                if (mapped.isUnresolved() || mapped.getPort() < 1
                        || AddressFamily.of(mapped.getAddress()) != gathering.family
                        || !reachable(mapped.getAddress())) {
                    gathering.candidate.completeExceptionally(
                            new IllegalStateException("The STUN mapping is no endpoint players can reach"));
                    return;
                }
                gathering.candidate.complete(new HostCandidate(mapped, CandidateType.SERVER_REFLEXIVE));
            }));
        }
    }

    /**
     * The way back for an assisted join: the provider's control socket, through the stage
     * {@link #assistedJoin} returned. The endpoint gathers and signs a full answer; this cuts it to the one
     * candidate the client should check.
     */
    private final class AssistedReply implements ReplyPath {
        private final Assisted state;

        AssistedReply(Assisted state) {
            this.state = state;
        }

        @Override
        public boolean trickles() {
            return false;
        }

        @Override
        public CompletionStage<Void> answer(String sdp) {
            // The endpoint signed it with its server identity (gap G1), over the fingerprints only, so the
            // candidates may still change.
            return state.candidate.thenCompose(candidate -> {
                CompletableFuture<Void> handedOn = new CompletableFuture<>();
                onLoop(() -> {
                    try {
                        state.requireCurrent();
                        AssistedAnswer answer = AssistedAnswer.build(sdp, candidate, null);
                        state.result.complete(answer);
                        handedOn.complete(null);
                    } catch (Exception e) {
                        state.end(e);
                        handedOn.completeExceptionally(e);
                    }
                }, () -> handedOn.completeExceptionally(new IllegalStateException("The NetherNet endpoint is closed")));
                return handedOn;
            });
        }

        @Override
        public CompletionStage<Void> candidate(String candidate) {
            throw new UnsupportedOperationException("An assisted NXS answer carries its one candidate");
        }

        @Override
        public CompletionStage<Void> refuse(JoinRefusal refusal) {
            onLoop(() -> state.end(new IllegalStateException("The endpoint refused the assisted join: " + refusal)));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onRemoteSignal(Consumer<RemoteSignal> handler) {
            // The provider relays nothing after the offer
        }

        @Override
        public CompletionStage<Void> abandoned() {
            return state.abandoned;
        }

        @Override
        public void finished() {
            onLoop(() -> {
                if (!state.result.isDone()) {
                    state.end(new IllegalStateException("The assisted join ended before its answer"));
                }
                if (state.session.child == null) {
                    assisted.remove(state.join.localUfrag());
                    withoutChild(state.session);
                }
            });
        }
    }

    // ---- Maintenance and outcomes ----

    /** Activation, logins and the ledger's sweep. Event loop, every 100 ms. */
    private void maintain() {
        if (closing) {
            return;
        }
        for (Session session : List.copyOf(sessions.values())) {
            NetherNetChildChannel child = session.child;
            if (!session.reporting && child.isActive()) {
                session.reporting = true;
                // Before the sweep below, which would take a child connected just in time for a stalled handshake;
                // only the outcome waits for the address
                ledger.connected(session.reservation);
                try {
                    // The address tracker belongs to the child's own loop
                    child.eventLoop().execute(() -> {
                        SourcedAddress reported = child.reportedAddress();
                        onLoop(() -> activated(session, reported));
                    });
                } catch (RejectedExecutionException closing) {
                    // A child whose loop is gone is closing too
                }
            }
        }
        for (TicketLedger.Reservation reservation : ledger.sweep(millis.getAsLong(), nanos.getAsLong())) {
            Session session = sessions.get(reservation);
            if (session != null) {
                session.failure = "timeout";
                log.debug("Closing NXS ticket {}: its handshake or login ran out of time", reservation.ticketId());
                session.child.close();
            }
        }
        gather();
        diagnostics.tick();
    }

    private void activated(Session session, SourcedAddress reported) {
        if (session.reported || !sessions.containsKey(session.reservation)) {
            return;
        }
        session.reported = true;
        if (session.kind == Kind.ASSISTED) {
            emit(session, OutcomeStage.ICE_SEEN, "assisted_ice_dtls_established", null);
        }
        InetSocketAddress remote = switch (reported == null ? AddressSource.UNKNOWN : reported.source()) {
            case ICE, ICE_PROXY_PROTOCOL, TURN -> reported.address();
            default -> null;
        };
        emit(session, OutcomeStage.DATA_CHANNELS_OPEN, "both_channels_open",
                remote == null || remote.isUnresolved() || remote.getPort() < 1 ? null : remote);
    }

    private void emit(Session session, OutcomeStage stage, String reason, InetSocketAddress remote) {
        TransportOutcome outcome = new TransportOutcome(session.admission.ticketId(), stage, millis.getAsLong(), reason,
                remote);
        if (!outcomes.offer(outcome)) {
            // The client polls every second; a full queue means it stopped, so the outcome is only telemetry lost
            log.debug("Dropping NXS outcome {} of ticket {}: the queue is full", stage, outcome.ticketId());
        }
    }

    @Override
    public List<TransportOutcome> pollOutcomes(int maximum) {
        List<TransportOutcome> polled = new ArrayList<>(Math.min(maximum, MAX_OUTCOMES));
        outcomes.drainTo(polled, maximum);
        return polled;
    }

    // ---- Keys ----

    @Override
    public CompletionStage<Void> installAdmissionKeys(List<AdmissionKey> keys) {
        List<AdmissionKey> snapshot;
        try {
            // Outside the guard: copying runs the caller's list code
            snapshot = List.copyOf(keys);
        } catch (RuntimeException invalid) {
            return CompletableFuture.failedFuture(invalid);
        }
        synchronized (lifecycle) {
            if (closing) {
                return CompletableFuture.failedFuture(new IllegalStateException("The NXS host transport is closed"));
            }
            try {
                validator.installKeys(snapshot);
                epochs = snapshot.stream().map(key -> new Epoch(key.keyId(), key.retireAfter())).toList();
                validator.retireKeys(millis.getAsLong());
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException invalid) {
                return CompletableFuture.failedFuture(invalid);
            }
        }
    }

    // ---- Diagnostics ----

    @Override
    public boolean supportsDiagnosticAdmission() {
        return true;
    }

    /**
     * Installs the policy after checking that it describes this host as it stands: its incarnation, its current
     * candidate revision, and only endpoints it publishes, probe targets or the families assisted checks serve. The
     * policy is cut to the heartbeat's deadline, which installing never extends, and replaces the earlier one.
     */
    @Override
    public CompletionStage<Void> configureDiagnostics(DiagnosticHostPolicy policy, long deadlineNanos) {
        Objects.requireNonNull(policy, "policy");
        long remaining = deadlineNanos - nanos.getAsLong();
        if (remaining <= 0 || remaining > MAX_DIAGNOSTIC_POLICY.toNanos()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Diagnostic deadline"));
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        onLoop(() -> {
            try {
                installDiagnostics(policy, deadlineNanos);
                result.complete(null);
            } catch (IOException | RuntimeException e) {
                result.completeExceptionally(e);
            }
        }, () -> result.completeExceptionally(new IllegalStateException("The NetherNet endpoint is closed")));
        return result;
    }

    private void installDiagnostics(DiagnosticHostPolicy policy, long deadlineNanos) throws IOException {
        DiagnosticHost current = diagnostics;
        if (closing || draining || current == null) {
            throw new IllegalStateException("The NXS host transport is not serving");
        }
        long now = millis.getAsLong();
        if (now >= policy.expiresAt() || policy.endpointExpiries().values().stream().anyMatch(expiry -> now >= expiry)) {
            throw new IllegalArgumentException("Expired diagnostic policy");
        }
        List<HostCandidate> probes;
        Set<AddressFamily> families;
        if (publisher == null) {
            probes = fixedCandidates();
            families = Set.of();
        } else if (withdrawn || publication == null) {
            probes = List.of();
            families = Set.of();
        } else {
            publication.guard().requireCurrent();
            probes = publication.probeCandidates();
            families = settings.assistedJoins() ? publication.assistedFamilies() : Set.of();
        }
        long revision = candidateRevision;
        boolean described = incarnation.equals(policy.context().incarnation())
                && policy.endpoints().stream().allMatch(endpoint -> endpoint.candidateRevision() == revision
                && (endpoint.assisted() ? families.contains(AddressFamily.of(endpoint.family()))
                : probes.stream().anyMatch(candidate -> names(candidate, endpoint))));
        if (!described) {
            throw new IllegalArgumentException("The diagnostic policy describes another state of this host");
        }
        long expiry = Math.min(policy.expiresAt(), now + TimeUnit.NANOSECONDS.toMillis(deadlineNanos
                - nanos.getAsLong()));
        Map<DiagnosticHostPolicy.Endpoint, Long> expiries = new HashMap<>();
        policy.endpointExpiries().forEach((endpoint, limit) -> expiries.put(endpoint, Math.min(limit, expiry)));
        current.install(new DiagnosticHostPolicy(policy.context(), policy.keys(), policy.endpoints(), expiry, expiries),
                deadlineNanos);
    }

    private static boolean names(HostCandidate candidate, DiagnosticHostPolicy.Endpoint endpoint) {
        int family = candidate.family().number();
        return family == endpoint.family() && candidate.endpoint().getPort() == endpoint.port()
                && DiagnosticAdmissionCodec.address(family, candidate.endpoint().getAddress().getHostAddress())
                .equals(endpoint.addressHex());
    }

    /** Withdraws the policy and ends every check; the replay history stays. Player admission is untouched. */
    @Override
    public CompletionStage<Void> disableDiagnostics() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        onLoop(() -> {
            DiagnosticHost current = diagnostics;
            if (current != null) {
                current.disable();
            }
            result.complete(null);
        }, () -> result.complete(null));
        return result;
    }

    /**
     * Takes up to {@code maximum} results of the checks this host admitted, oldest first. The host keeps the latest
     * {@value DiagnosticHost#MAX_RESULTS} and drops newer ones while nobody takes them. Any thread.
     */
    public List<DiagnosticResult> pollDiagnosticResults(int maximum) {
        if (maximum < 0) {
            throw new IllegalArgumentException("Negative result count");
        }
        DiagnosticHost current = diagnostics;
        return current == null ? List.of() : current.poll(maximum);
    }

    // ---- Profile ----

    @Override
    public long candidatePublicationVersion() {
        return publicationVersion;
    }

    @Override
    public CompletionStage<HostProfileSnapshot> captureHostProfile() {
        CompletableFuture<HostProfileSnapshot> result = new CompletableFuture<>();
        onLoop(() -> {
            try {
                result.complete(capture());
            } catch (IOException | RuntimeException e) {
                result.completeExceptionally(e);
            }
        }, () -> result.completeExceptionally(new IllegalStateException("The NetherNet endpoint is closed")));
        return result;
    }

    private HostProfileSnapshot capture() throws IOException {
        if (closing || draining || port == null) {
            throw new IllegalStateException("The NXS host transport is not serving");
        }
        long now = millis.getAsLong();
        Set<String> installed = validator.keyIds();
        String keyId = null;
        // The provider sends its epochs oldest first; the newest one still live is the one to issue under
        for (Epoch epoch : epochs) {
            if (epoch.retireAfter() > now && installed.contains(epoch.id())) {
                keyId = epoch.id();
            }
        }
        if (keyId == null) {
            throw new IllegalStateException("No active admission key");
        }
        List<HostCandidate> offered;
        List<HostCandidate> probes;
        Set<AddressFamily> families;
        OwnershipGuard maintained = OwnershipGuard.ALWAYS;
        if (publisher != null) {
            if (withdrawn || publication == null) {
                offered = List.of();
                probes = List.of();
                families = Set.of();
            } else {
                offered = publication.candidates();
                probes = publication.probeCandidates();
                families = publication.assistedFamilies();
                maintained = publication.guard();
            }
        } else {
            offered = fixedCandidates();
            probes = offered;
            families = Set.of();
        }
        HostProfile profile = new HostProfile(offered, fingerprint, keyId, SCTP_PORT,
                StatelessAdmissionCodec.MAX_MESSAGE_SIZE, incarnation);
        long revision = candidateRevision;
        long version = publicationVersion;
        OwnershipGuard publicationGuard = maintained;
        OwnershipGuard guard = () -> {
            if (closing || draining) {
                throw new IllegalStateException("The NXS host transport stopped serving");
            }
            if (candidateRevision != revision || publicationVersion != version) {
                throw new ProfileReplacedException();
            }
            publicationGuard.requireCurrent();
        };
        guard.requireCurrent();
        return new HostProfileSnapshot(profile, probes, families, revision, version, guard);
    }

    /** The addresses the port listens on and the configured endpoints. Event loop. */
    private List<HostCandidate> fixedCandidates() throws IOException {
        InetSocketAddress bind = port.localAddress();
        ProviderEndpoint endpoint = settings.publication() == Publication.RELAYED
                ? ProviderEndpoint.relayed(bind, settings.advertised(), settings.localDevelopment(), hostAddresses)
                : ProviderEndpoint.resolve(bind, settings.advertised(), settings.localDevelopment(), hostAddresses);
        List<HostCandidate> candidates = endpoint.advertised().stream().map(HostCandidate::host).toList();
        Set<HostCandidate> material = Set.copyOf(candidates);
        if (fixedCandidates != null && !fixedCandidates.equals(material)) {
            candidateRevision++;
        }
        fixedCandidates = material;
        return candidates;
    }

    /** Takes a fresh publication; a failed refresh withdraws every candidate rather than keep an old one. */
    private void refreshMaintained() {
        MaintainedCandidatePublisher current = publisher;
        if (closing || draining || current == null) {
            return;
        }
        try {
            MaintainedCandidatePublisher.Publication next = current.refresh();
            boolean changed = withdrawn || publication == null
                    || !Set.copyOf(publication.probeCandidates()).equals(Set.copyOf(next.probeCandidates()))
                    || next.mappingRevision() != mappingRevision;
            if (changed && publication != null) {
                candidateRevision++;
            }
            if (changed || !publication.candidates().equals(next.candidates())) {
                publicationVersion++;
            }
            withdrawn = false;
            mappingRevision = next.mappingRevision();
            publication = next;
        } catch (RuntimeException unavailable) {
            log.warn("NXS could not refresh its candidates, withdrawing them: {}", unavailable.toString());
            if (!withdrawn) {
                withdrawn = true;
                candidateRevision++;
                publicationVersion++;
            }
        }
    }

    @Override
    public CompletionStage<Void> configureStunServers(List<StunServer> servers) {
        if (servers.size() > 2) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("At most two provider STUN servers"));
        }
        MaintainedCandidatePublisher current = publisher;
        if (closing || current == null || !current.needsStunServers()) {
            return CompletableFuture.completedFuture(null);
        }
        List<StunServer> owned = List.copyOf(servers);
        CompletableFuture<Void> result = new CompletableFuture<>();
        // Names are looked up off the client's thread and the event loop
        Thread.ofVirtual().name("nxs-stun-lookup").start(() -> {
            Map<AddressFamily, InetSocketAddress> numeric = numericServers(owned);
            onLoop(() -> {
                try {
                    if (closing || draining || publisher == null) {
                        throw new IllegalStateException("The NXS host transport is not serving");
                    }
                    publisher.configureStunServers(numeric);
                    refreshMaintained();
                    result.complete(null);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            }, () -> result.completeExceptionally(new IllegalStateException("The NetherNet endpoint is closed")));
        });
        return result;
    }

    /** The first usable address of each family the servers resolve to; one that does not resolve stays unknown. */
    private static Map<AddressFamily, InetSocketAddress> numericServers(List<StunServer> servers) {
        Map<AddressFamily, InetSocketAddress> numeric = new EnumMap<>(AddressFamily.class);
        for (StunServer server : servers) {
            try {
                for (InetAddress address : InetAddress.getAllByName(server.host())) {
                    if (!address.isAnyLocalAddress() && !address.isMulticastAddress()) {
                        numeric.putIfAbsent(AddressFamily.of(address), new InetSocketAddress(address, server.port()));
                    }
                }
            } catch (UnknownHostException unavailable) {
                log.debug("Provider STUN server {} does not resolve", server.host());
            }
        }
        return numeric;
    }

    @Override
    public CompletionStage<Void> reportConnectivityChecks(long revision, List<ConnectivityCheck> checks) {
        if (checks.size() > 18) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("At most eighteen connectivity checks"));
        }
        List<ConnectivityCheck> owned = List.copyOf(checks);
        long reportedAt = millis.getAsLong();
        CompletableFuture<Void> result = new CompletableFuture<>();
        onLoop(() -> {
            try {
                MaintainedCandidatePublisher current = publisher;
                // Feedback for an older revision describes candidates no longer published
                if (!closing && !draining && current != null && candidateRevision == revision) {
                    long now = millis.getAsLong();
                    List<ConnectivityCheck> fresh = owned.stream()
                            .filter(check -> check.checkedAt() <= reportedAt && check.expiresAt() > now)
                            .toList();
                    current.reportDirectChecks(fresh, now);
                    refreshMaintained();
                }
                result.complete(null);
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }, () -> result.completeExceptionally(new IllegalStateException("The NetherNet endpoint is closed")));
        return result;
    }

    // ---- Drain and close ----

    @Override
    public CompletionStage<Void> drain() {
        draining = true;
        ledger.drain();
        CompletableFuture<Void> result = new CompletableFuture<>();
        onLoop(() -> {
            MaintainedCandidatePublisher current = publisher;
            if (current != null) {
                current.close();
            }
            result.complete(null);
        }, () -> result.complete(null));
        return result;
    }

    /**
     * Stops admitting, revokes the admission keys and every pending login, gives the held checks back, and closes
     * every child whose login was not accepted, since no login deadline closes it once this transport is gone.
     * Players whose login was accepted stay connected. Idempotent; the endpoint calls it when it closes or removes
     * this source.
     */
    @Override
    public CompletionStage<Void> close() {
        synchronized (lifecycle) {
            if (closing) {
                return closed.minimalCompletionStage();
            }
            closing = true;
            draining = true;
            validator.clear();
            epochs = List.of();
        }
        onLoop(this::closeOnLoop, () -> closed.complete(null));
        return closed.minimalCompletionStage();
    }

    private void closeOnLoop() {
        tasks.forEach(task -> task.cancel(false));
        tasks.clear();
        if (heldChecks != null) {
            heldChecks.close();
        }
        MaintainedCandidatePublisher current = publisher;
        if (current != null) {
            try {
                current.close();
            } catch (RuntimeException e) {
                log.debug("Closing the NXS candidate publisher failed: {}", e.toString());
            }
        }
        for (Assisted state : List.copyOf(assisted.values())) {
            if (!state.done) {
                state.end(new IllegalStateException("The NXS host transport closed"));
            }
        }
        for (Gathering gathering : List.copyOf(gatherings)) {
            gathering.candidate.completeExceptionally(new IllegalStateException("The NXS host transport closed"));
        }
        for (TicketLedger.Reservation reservation : ledger.close()) {
            Session session = sessions.get(reservation);
            if (session != null) {
                log.debug("Closing NXS ticket {}: its login was not accepted before NXS closed",
                        reservation.ticketId());
                session.child.close();
            }
        }
        started.completeExceptionally(new IllegalStateException("The NXS host transport is closed"));
        health(SignalingState.CLOSED, null);
        // The checks' peers belong to this transport, so it is closed once they are gone
        (diagnostics == null ? CompletableFuture.<Void>completedFuture(null) : diagnostics.close())
                .whenComplete((ignored, failure) -> closed.complete(null));
    }

    private void onLoop(Runnable task) {
        onLoop(task, () -> log.debug("Dropping an NXS host task: the endpoint's event loop is shutting down"));
    }

    private void onLoop(Runnable task, Runnable rejected) {
        if (loop.inEventLoop()) {
            task.run();
            return;
        }
        try {
            loop.execute(task);
        } catch (RejectedExecutionException e) {
            rejected.run();
        }
    }

    @Override
    public String toString() {
        return "HostTransport[incarnation=" + incarnation + ", " + certificate + "]";
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig.Role;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.RemoteDescriptionRejectedException;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.webrtc.AddIceCandidateObserver;
import io.github.sendablemetatype.webrtc.CreateSessionDescriptionObserver;
import io.github.sendablemetatype.webrtc.PeerConnectionObserver;
import io.github.sendablemetatype.webrtc.RTCCandidatePairChangeEvent;
import io.github.sendablemetatype.webrtc.RTCDataChannel;
import io.github.sendablemetatype.webrtc.RTCErrorDetailType;
import io.github.sendablemetatype.webrtc.RTCErrorType;
import io.github.sendablemetatype.webrtc.RTCIceCandidate;
import io.github.sendablemetatype.webrtc.RTCIceConnectionState;
import io.github.sendablemetatype.webrtc.RTCIceGatheringState;
import io.github.sendablemetatype.webrtc.RTCPeerConnectionState;
import io.github.sendablemetatype.webrtc.RTCSdpType;
import io.github.sendablemetatype.webrtc.RTCSessionDescription;
import io.github.sendablemetatype.webrtc.RTCSctpMetrics;
import io.github.sendablemetatype.webrtc.RTCTransportMetrics;
import io.github.sendablemetatype.webrtc.SetSessionDescriptionObserver;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A peer on a webrtc-java peer connection.
 *
 * <p>Threads: every call that may wait for libwebrtc (creating the
 * connection, description steps, candidates, metrics, teardown) runs on the
 * control thread of the peer's factory. libwebrtc delivers the connection's
 * events and the results of description steps on the factory's signaling
 * thread, and the data channels' events on its network thread, where their
 * observers ask to be called ({@link WebRtcLink}); send results arrive on
 * webrtc-java's callback thread. So a link's events are not ordered with the
 * connection's: a link can open before the connection reports that it is
 * connected.
 */
final class WebRtcPeer implements Peer {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(WebRtcPeer.class);

    /** SCTP's "User Initiated Abort" (RFC 9260, 3.3.10.12): dcSCTP reports a peer's ABORT with it. */
    private static final int SCTP_USER_INITIATED_ABORT = 12;

    /**
     * How long teardown waits for send results before it fails the sends
     * still without one and releases the data channels. libwebrtc settles
     * every send when the connection closes, so this only bounds a lost result.
     */
    private static final Duration SEND_SETTLE_TIMEOUT = Duration.ofSeconds(10);

    /**
     * How long a data channel may stay CLOSING before it counts as closed by
     * the remote peer. An abrupt close passes CLOSING and CLOSED in one call
     * ({@code SctpDataChannel::CloseAbruptlyWithError}). When the remote peer
     * resets its stream, libwebrtc resets ours and reports CLOSED once the
     * peer acknowledged that: one round trip, and dcSCTP sends the reset again
     * every RTO, at least 400 ms ({@code rto_min}), with no limit in WebRTC. A
     * peer that denies the reset leaves the channel CLOSING for good: dcSCTP
     * reports the denial ({@code StreamResetHandler::HandleResponse}) and
     * WebRTC only logs it ({@code DcSctpTransport::OnStreamsResetFailed}).
     * Five seconds give a healthy close several resends, and end a stuck one,
     * whose remote peer is leaving, soon after.
     */
    static final Duration CLOSING_TIMEOUT = Duration.ofSeconds(5);

    private final Role role;
    private final PeerListener listener;
    private final Executor control;
    private final IceCredentials localCredentials;
    private final Duration sendSettleTimeout;
    private final Duration closingTimeout;

    private final AtomicBoolean closeRequested = new AtomicBoolean();
    private volatile boolean listening = true;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private final CompletionStage<Void> closeStage = terminated.minimalCompletionStage();
    private final CompletableFuture<String> gathered = new CompletableFuture<>();

    // Written once on the control thread, read there and by teardown.
    private volatile NativePeer connection;
    private volatile Throwable failure;
    private final AtomicBoolean closeReported = new AtomicBoolean();
    private volatile IceState iceState = IceState.NEW;
    private volatile CloseCause peerFailure;

    private final Object lock = new Object();
    // Guarded by lock.
    private boolean descriptionStarted;
    private boolean answerStarted;
    private boolean remoteDescriptionIssued;
    private List<RemoteCandidate> heldRemote = new ArrayList<>();
    private boolean localDescriptionReturned;
    private List<LocalEvent> heldLocal = new ArrayList<>();
    private final List<CompletableFuture<?>> openStages = new ArrayList<>();
    private final List<WebRtcLink> links = new ArrayList<>();

    WebRtcPeer(Role role, PeerListener listener, Executor control, IceCredentials localCredentials) {
        this(role, listener, control, localCredentials, SEND_SETTLE_TIMEOUT, CLOSING_TIMEOUT);
    }

    WebRtcPeer(Role role, PeerListener listener, Executor control, IceCredentials localCredentials,
               Duration sendSettleTimeout, Duration closingTimeout) {
        this.role = Objects.requireNonNull(role, "role");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.control = Objects.requireNonNull(control, "control");
        this.localCredentials = localCredentials;
        this.sendSettleTimeout = Objects.requireNonNull(sendSettleTimeout, "sendSettleTimeout");
        this.closingTimeout = Objects.requireNonNull(closingTimeout, "closingTimeout");
    }

    /**
     * Creates the native connection on the control thread.
     *
     * @param creator creates it with the observer that feeds this peer
     */
    void start(Function<PeerConnectionObserver, NativePeer> creator) {
        execute(() -> {
            if (closeRequested.get()) {
                return;
            }
            try {
                connection = creator.apply(new Events());
            } catch (RuntimeException | Error e) {
                // A LinkageError means webrtc-java has no natives here.
                if (!(e instanceof LinkageError) && !NativeErrors.isCallFailure(e)) {
                    throw e;
                }
                log.warn("Creating a WebRTC peer connection failed", e);
                negotiationFailed("CreatePeerConnection", e.toString());
            }
        });
    }

    @Override
    public CompletionStage<String> answer(String offer) {
        Objects.requireNonNull(offer, "offer");
        requireRole(Role.ANSWERER, "answer");
        CompletableFuture<String> stage = openStage(true);
        String stripped = SdpEdits.withoutIdentity(offer);
        begin(stage, () -> {
            if (proceed(stage) && step("SetRemoteDescription", () -> connection.setRemoteDescription(
                    new RTCSessionDescription(RTCSdpType.OFFER, stripped),
                    new SetObserver("SetRemoteDescription", () -> execute(() -> createAnswer(stage)))))) {
                remoteDescriptionIssued();
            }
        });
        return stage.minimalCompletionStage();
    }

    private void createAnswer(CompletableFuture<String> stage) {
        if (proceed(stage)) {
            step("CreateAnswer", () -> connection.createAnswer(
                    new DescriptionObserver("CreateAnswer", description -> execute(() -> applyLocal(description, stage)))));
        }
    }

    @Override
    public CompletionStage<String> offer() {
        requireRole(Role.OFFERER, "offer");
        CompletableFuture<String> stage = openStage(true);
        begin(stage, () -> {
            if (proceed(stage)) {
                step("CreateOffer", () -> connection.createOffer(
                        new DescriptionObserver("CreateOffer", description -> execute(() -> applyLocal(description, stage)))));
            }
        });
        return stage.minimalCompletionStage();
    }

    private void applyLocal(RTCSessionDescription created, CompletableFuture<String> stage) {
        if (!proceed(stage)) {
            return;
        }
        String sdp = localCredentials == null ? created.sdp : SdpEdits.withCredentials(created.sdp, localCredentials);
        step("SetLocalDescription", () -> connection.setLocalDescription(new RTCSessionDescription(created.sdpType, sdp),
                new SetObserver("SetLocalDescription", () -> localDescriptionApplied(sdp, stage))));
    }

    /** On the signaling thread, which also delivers the local candidates held meanwhile. */
    private void localDescriptionApplied(String sdp, CompletableFuture<String> stage) {
        if (!listening) {
            return;
        }
        // Dependents attached to the stage run here, before any held candidate.
        stage.complete(sdp);
        List<LocalEvent> held;
        synchronized (lock) {
            localDescriptionReturned = true;
            held = heldLocal;
            heldLocal = List.of();
        }
        held.forEach(this::deliverLocal);
    }

    @Override
    public CompletionStage<Void> applyAnswer(String answer) {
        Objects.requireNonNull(answer, "answer");
        requireRole(Role.OFFERER, "applyAnswer");
        CompletableFuture<Void> stage;
        synchronized (lock) {
            if (!descriptionStarted) {
                throw new IllegalStateException("offer() comes before applyAnswer()");
            }
            if (answerStarted) {
                throw new IllegalStateException("applyAnswer() was called before");
            }
            answerStarted = true;
            stage = openStage(false);
        }
        String stripped = SdpEdits.withoutIdentity(answer);
        begin(stage, () -> {
            if (proceed(stage) && step("SetRemoteDescription", () -> connection.setRemoteDescription(
                    new RTCSessionDescription(RTCSdpType.ANSWER, stripped),
                    new SetObserver("SetRemoteDescription", () -> stage.complete(null))))) {
                remoteDescriptionIssued();
            }
        });
        return stage.minimalCompletionStage();
    }

    @Override
    public void addRemoteCandidate(String candidate, String mid) {
        RemoteCandidate remote = new RemoteCandidate(SdpEdits.candidateAttribute(candidate), Objects.requireNonNull(mid, "mid"));
        synchronized (lock) {
            if (closeRequested.get()) {
                return;
            }
            if (!remoteDescriptionIssued) {
                heldRemote.add(remote);
                return;
            }
        }
        execute(() -> applyRemote(remote));
    }

    /**
     * On the control thread, right after the remote description was handed to
     * libwebrtc. Candidates are chained behind it there, so they need not wait
     * for its result; later candidates queue behind this task.
     */
    private void remoteDescriptionIssued() {
        List<RemoteCandidate> held;
        synchronized (lock) {
            remoteDescriptionIssued = true;
            held = heldRemote;
            heldRemote = List.of();
        }
        held.forEach(this::applyRemote);
    }

    private void applyRemote(RemoteCandidate remote) {
        NativePeer current = connection;
        if (current == null || closeRequested.get() || failure != null) {
            return;
        }
        try {
            current.addIceCandidate(new RTCIceCandidate(remote.mid(), 0, remote.attribute()), new AddIceCandidateObserver() {
                @Override
                public void onSuccess() {
                }

                @Override
                public void onFailure(String error) {
                    log.debug("Remote candidate refused ({}): {}", error, remote.attribute());
                }
            });
        } catch (RuntimeException | Error e) {
            // An unparsable candidate is a plain Error; it must not cost the
            // candidates held behind it.
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            log.debug("Remote candidate refused ({}): {}", e.toString(), remote.attribute());
        }
    }

    @Override
    public CompletionStage<String> gatheredLocalDescription() {
        return gathered.minimalCompletionStage();
    }

    @Override
    public DataLink openChannel(ChannelParameters parameters) {
        Objects.requireNonNull(parameters, "parameters");
        requireRole(Role.OFFERER, "openChannel");
        WebRtcLink link = new WebRtcLink(this, parameters, null);
        synchronized (lock) {
            if (descriptionStarted) {
                throw new IllegalStateException("Data links are opened before offer()");
            }
            if (!closeRequested.get()) {
                links.add(link);
            }
        }
        execute(() -> {
            NativePeer current = connection;
            if (current == null || closeRequested.get() || failure != null) {
                return;
            }
            try {
                link.bind(current.createDataChannel(parameters));
            } catch (RuntimeException | Error e) {
                if (!NativeErrors.isCallFailure(e)) {
                    throw e;
                }
                negotiationFailed("CreateDataChannel", e.toString());
            }
        });
        return link;
    }

    @Override
    public CompletionStage<TransportMetrics> metrics() {
        CompletableFuture<TransportMetrics> result = new CompletableFuture<>();
        if (closeRequested.get() || !execute(() -> result.complete(readMetrics()))) {
            result.complete(TransportMetrics.NONE);
        }
        return result.minimalCompletionStage();
    }

    private TransportMetrics readMetrics() {
        NativePeer current = connection;
        if (current == null || closeRequested.get()) {
            return TransportMetrics.NONE;
        }
        try {
            return metricsOf(current.transportMetrics());
        } catch (RuntimeException | Error e) {
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            log.debug("Reading transport metrics failed: {}", e.toString());
            return TransportMetrics.NONE;
        }
    }

    static TransportMetrics metricsOf(RTCTransportMetrics metrics) {
        if (metrics == null) {
            return TransportMetrics.NONE;
        }
        RTCSctpMetrics sctp = metrics.sctp();
        return new TransportMetrics(Optional.ofNullable(metrics.iceRoundTripTime()), Optional.ofNullable(sctp)
                .map(m -> new TransportMetrics.Sctp(m.smoothedRoundTripTime(), m.congestionWindowBytes(),
                        m.unackedDataCount(), m.peerReceiveWindowBytes(), m.packetsSent(), m.messagesSent(),
                        m.packetsRetransmitted(), m.bytesRetransmitted(), m.packetsReceived(), m.messagesReceived())));
    }

    @Override
    public CompletionStage<Void> close() {
        if (closeRequested.compareAndSet(false, true)) {
            listening = false;
            // The control thread lives until every peer of the engine has
            // terminated, so a refusal means there are no natives left to
            // release; only the stages wait.
            if (!execute(this::terminate)) {
                failStages();
                terminated.complete(null);
            }
        }
        return closeStage;
    }

    /**
     * The termination, on the control thread and never inside an engine
     * callback: observers off, data channels and connection closed, send
     * results settled, data channels released.
     */
    private void terminate() {
        List<WebRtcLink> current;
        synchronized (lock) {
            current = List.copyOf(links);
        }
        failStages();

        List<CompletableFuture<Void>> settled = new ArrayList<>(current.size());
        for (WebRtcLink link : current) {
            settled.add(link.stopSending());
            NativeLink channel = link.channel();
            if (channel != null) {
                quietly("unregistering a data channel observer", channel::unregisterObserver);
                quietly("closing a data channel", channel::close);
            }
        }
        NativePeer peer = connection;
        if (peer != null) {
            quietly("closing a peer connection", peer::close);
        }
        // libwebrtc settles every pending send once the connection is closed.
        // A result still missing at the timeout fails its send, and arrives
        // to no one if it ever comes; the data channels are released after
        // that, so no send completion follows the close stage.
        CompletableFuture.allOf(settled.toArray(CompletableFuture[]::new))
                .completeOnTimeout(null, sendSettleTimeout.toNanos(), TimeUnit.NANOSECONDS)
                .whenComplete((ignored, error) -> onControl(() -> abandonSends(current)));
    }

    /** Fails the open stages and drops the held candidates, once the close was requested. */
    private void failStages() {
        List<CompletableFuture<?>> stages;
        synchronized (lock) {
            stages = List.copyOf(openStages);
            heldRemote = List.of();
            heldLocal = List.of();
        }
        IllegalStateException closed = new IllegalStateException("Peer closed");
        stages.forEach(stage -> stage.completeExceptionally(closed));
        gathered.completeExceptionally(closed);
    }

    private void abandonSends(List<WebRtcLink> current) {
        CompletableFuture.allOf(current.stream().map(WebRtcLink::abandonSends).toArray(CompletableFuture[]::new))
                .whenComplete((ignored, error) -> onControl(() -> release(current)));
    }

    private void release(List<WebRtcLink> current) {
        for (WebRtcLink link : current) {
            NativeLink channel = link.channel();
            if (channel != null) {
                quietly("releasing a data channel", channel::dispose);
            }
        }
        terminated.complete(null);
    }

    /** Completes after the peer's termination; the same stage as {@link #close()} returns. */
    CompletionStage<Void> closeStage() {
        return closeStage;
    }

    void dispatch(Consumer<PeerListener> event) {
        if (!listening) {
            return;
        }
        try {
            event.accept(listener);
        } catch (RuntimeException e) {
            log.warn("A NetherNet peer listener failed", e);
        }
    }

    /**
     * The message path, kept free of allocations.
     *
     * @param binary whether libwebrtc marks the message binary rather than text
     */
    void deliver(WebRtcLink link, ByteBuffer message, boolean binary) {
        if (!listening) {
            return;
        }
        try {
            if (binary) {
                listener.onMessage(link, message);
            } else {
                listener.onTextMessage(link, message);
            }
        } catch (RuntimeException e) {
            log.warn("A NetherNet peer listener failed", e);
        }
    }

    /**
     * Runs a task on the control thread once the closing timeout ({@link #CLOSING_TIMEOUT}) passed.
     * The JDK's delay thread, which teardown's settle timeout uses too, hands it over. Dropped if
     * the engine stopped the control thread meanwhile.
     */
    void afterClosingTimeout(Runnable task) {
        CompletableFuture.delayedExecutor(closingTimeout.toNanos(), TimeUnit.NANOSECONDS, this::execute).execute(task);
    }

    /** Why a data channel that libwebrtc reports CLOSED ended; a local close never reaches here. */
    CloseCause linkCloseCause(NativeLink.LinkError error) {
        if (error == null || error.type() == RTCErrorType.NONE || isRefusedSend(error)) {
            return CloseCause.REMOTE_CLOSED;
        }
        log.debug("Data channel closed with {}", error);
        if (error.detail() == RTCErrorDetailType.SCTP_FAILURE
                && Objects.equals(error.sctpCauseCode(), SCTP_USER_INITIATED_ABORT)) {
            return CloseCause.REMOTE_ABORTED;
        }
        CloseCause failed = peerFailure;
        if (failed != null) {
            return failed;
        }
        return iceState == IceState.FAILED ? CloseCause.ICE_FAILED : CloseCause.TRANSPORT_FAILED;
    }

    /**
     * Whether a closed channel's error is only the refusal of a send made while the remote peer
     * closed it in an orderly way. libwebrtc keeps the result of the channel's last send attempt
     * as its error. While the channel is closing it refuses a send with a bare
     * {@code INVALID_STATE} ({@code SctpDataChannel::SendImpl}), which the end of the closing
     * procedure leaves in place ({@code OnClosingProcedureComplete}). Once the channel closed it
     * refuses sends without touching the error ({@code SendAsync}), so an abrupt close keeps its
     * own. The one close that sets {@code INVALID_STATE} itself, a failed open handshake message,
     * carries a message ({@code SendControlMessage}).
     */
    private static boolean isRefusedSend(NativeLink.LinkError error) {
        return error.type() == RTCErrorType.INVALID_STATE
                && (error.detail() == null || error.detail() == RTCErrorDetailType.NONE)
                && (error.message() == null || error.message().isEmpty());
    }

    void onLocalCandidate(String candidate, String mid) {
        holdOrDeliver(new LocalEvent.Candidate(candidate, mid));
    }

    void onGatheringState(RTCIceGatheringState state) {
        if (state == RTCIceGatheringState.COMPLETE) {
            holdOrDeliver(new LocalEvent.GatheringComplete());
        }
    }

    /**
     * Local events wait until the description they belong to has been
     * returned. libwebrtc may gather a candidate before it reports that the
     * local description applied; both arrive on the signaling thread, so
     * holding them here keeps their order.
     */
    private void holdOrDeliver(LocalEvent event) {
        synchronized (lock) {
            if (closeRequested.get()) {
                return;
            }
            if (!localDescriptionReturned) {
                heldLocal.add(event);
                return;
            }
        }
        deliverLocal(event);
    }

    private void deliverLocal(LocalEvent event) {
        switch (event) {
            case LocalEvent.Candidate candidate ->
                    dispatch(l -> l.onLocalCandidate(candidate.candidate(), candidate.mid()));
            case LocalEvent.GatheringComplete ignored -> {
                dispatch(PeerListener::onGatheringComplete);
                execute(this::completeGathered);
            }
        }
    }

    private void completeGathered() {
        NativePeer current = connection;
        if (current == null || closeRequested.get() || failure != null) {
            return;
        }
        String sdp;
        try {
            sdp = current.localDescription();
        } catch (RuntimeException | Error e) {
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            negotiationFailed("GetLocalDescription", e.toString());
            return;
        }
        if (sdp == null) {
            negotiationFailed("GetLocalDescription", "no local description after gathering completed");
            return;
        }
        gathered.complete(SdpEdits.withEndOfCandidates(sdp));
    }

    void onIceConnectionState(RTCIceConnectionState state) {
        IceState mapped = switch (state) {
            case NEW -> IceState.NEW;
            case CHECKING -> IceState.CHECKING;
            case CONNECTED -> IceState.CONNECTED;
            case COMPLETED -> IceState.COMPLETED;
            case DISCONNECTED -> IceState.DISCONNECTED;
            case FAILED -> IceState.FAILED;
            case CLOSED -> IceState.CLOSED;
        };
        iceState = mapped;
        dispatch(l -> l.onIceStateChange(mapped));
    }

    /**
     * libwebrtc reports the ICE state before the connection state it feeds
     * (JsepTransportController updates them in that order), so a FAILED
     * connection already knows whether ICE failed.
     */
    void onConnectionState(RTCPeerConnectionState state) {
        PeerState mapped = switch (state) {
            case NEW -> PeerState.NEW;
            case CONNECTING -> PeerState.CONNECTING;
            case CONNECTED -> PeerState.CONNECTED;
            case DISCONNECTED -> PeerState.DISCONNECTED;
            case FAILED -> PeerState.FAILED;
            case CLOSED -> PeerState.CLOSED;
        };
        dispatch(l -> l.onStateChange(mapped));
        switch (state) {
            case FAILED -> {
                CloseCause cause = iceState == IceState.FAILED ? CloseCause.ICE_FAILED : CloseCause.TRANSPORT_FAILED;
                peerFailure = cause;
                reportClosed(cause);
            }
            // Only a local close closes a connection, and that stops the events first.
            case CLOSED -> reportClosed(CloseCause.UNKNOWN);
            default -> {
            }
        }
    }

    void onSelectedPair(String localCandidate, String remoteCandidate) {
        dispatch(l -> l.onPathSelected(CandidateLines.path(localCandidate, remoteCandidate)));
    }

    /** A data channel the remote peer opened, on the signaling thread. */
    void onRemoteChannel(NativeLink channel) {
        if (!listening) {
            execute(() -> discard(channel));
            return;
        }
        WebRtcLink link;
        try {
            link = new WebRtcLink(this, channel.parameters(), channel);
        } catch (RuntimeException e) {
            log.debug("Reading a remote data channel failed: {}", e.toString());
            execute(() -> discard(channel));
            return;
        }
        boolean keep;
        try {
            keep = listener.onRemoteChannel(link);
        } catch (RuntimeException e) {
            log.warn("A NetherNet peer listener failed", e);
            keep = false;
        }
        synchronized (lock) {
            keep &= !closeRequested.get();
            if (keep) {
                links.add(link);
            }
        }
        if (keep) {
            link.attachRemote();
        } else {
            execute(() -> discard(channel));
        }
    }

    private static void discard(NativeLink channel) {
        quietly("closing a refused data channel", channel::close);
        quietly("releasing a refused data channel", channel::dispose);
    }

    private void negotiationFailed(String operation, String error) {
        if (closeRequested.get()) {
            // Teardown fails the open stages; libwebrtc discards pending steps then.
            return;
        }
        // A remote description is the remote side's input; the other steps are ours.
        boolean remote = operation.equals("SetRemoteDescription");
        Exception cause = remote ? new RemoteDescriptionRejectedException(operation + " failed: " + error)
                : new IllegalStateException(operation + " failed: " + error);
        List<CompletableFuture<?>> stages;
        synchronized (lock) {
            if (failure != null) {
                return;
            }
            failure = cause;
            stages = List.copyOf(openStages);
        }
        if (remote) {
            log.debug("{} failed: {}", operation, error);
        } else {
            log.warn("{} failed: {}", operation, error);
        }
        stages.forEach(stage -> stage.completeExceptionally(cause));
        gathered.completeExceptionally(cause);
        reportClosed(remote ? CloseCause.REMOTE_DESCRIPTION_REJECTED : CloseCause.NEGOTIATION_FAILED);
    }

    private void reportClosed(CloseCause cause) {
        if (listening && closeReported.compareAndSet(false, true)) {
            dispatch(l -> l.onClosed(cause));
        }
    }

    /**
     * A negotiation's stage. Teardown fails the stages open when it starts;
     * a stage opened after the close request fails at once, since teardown
     * may have passed already.
     */
    private <T> CompletableFuture<T> openStage(boolean description) {
        CompletableFuture<T> stage = new CompletableFuture<>();
        synchronized (lock) {
            if (description) {
                if (descriptionStarted) {
                    throw new IllegalStateException(role == Role.ANSWERER ? "answer() was called before" : "offer() was called before");
                }
                descriptionStarted = true;
            }
            if (closeRequested.get()) {
                stage.completeExceptionally(new IllegalStateException("Peer closed"));
                return stage;
            }
            openStages.add(stage);
        }
        stage.whenComplete((value, error) -> {
            synchronized (lock) {
                openStages.remove(stage);
            }
        });
        return stage;
    }

    /** Starts a negotiation on the control thread, unless its stage failed already or the engine stopped that thread. */
    private void begin(CompletableFuture<?> stage, Runnable firstStep) {
        if (!stage.isDone() && !execute(firstStep)) {
            stage.completeExceptionally(new IllegalStateException("The WebRTC engine is closed"));
        }
    }

    /** Whether the next step of a negotiation may run; fails the stage if not. */
    private boolean proceed(CompletableFuture<?> stage) {
        if (closeRequested.get()) {
            stage.completeExceptionally(new IllegalStateException("Peer closed"));
            return false;
        }
        Throwable failed = failure;
        if (failed != null) {
            stage.completeExceptionally(failed);
            return false;
        }
        return true;
    }

    /** Makes a description call; a description webrtc-java cannot parse fails it with a plain Error. */
    private boolean step(String operation, Runnable call) {
        try {
            call.run();
            return true;
        } catch (RuntimeException | Error e) {
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            negotiationFailed(operation, e.toString());
            return false;
        }
    }

    private void requireRole(Role required, String method) {
        if (role != required) {
            throw new IllegalStateException(method + "() is for an " + required.name().toLowerCase() + " peer");
        }
    }

    /**
     * Runs a teardown step on the control thread, or here if the engine has
     * stopped it: the control thread lives until every peer of the engine has
     * terminated, so then nothing else runs there.
     */
    private void onControl(Runnable task) {
        if (!execute(task)) {
            task.run();
        }
    }

    /** Runs on the control thread; false if the engine has stopped it. */
    private boolean execute(Runnable task) {
        try {
            control.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            log.debug("The WebRTC engine is closed; dropping a peer task");
            return false;
        }
    }

    private static void quietly(String what, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            log.debug("Error {}: {}", what, e.toString());
        } catch (Error e) {
            // webrtc-java reports a native reference left behind with a plain
            // Error; the object is freed once its last holder lets go.
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            log.warn("Error {}: {}", what, e.getMessage());
        }
    }

    private record RemoteCandidate(String attribute, String mid) {
    }

    private sealed interface LocalEvent {
        record Candidate(String candidate, String mid) implements LocalEvent {
        }

        record GatheringComplete() implements LocalEvent {
        }
    }

    private final class SetObserver implements SetSessionDescriptionObserver {
        private final String operation;
        private final Runnable next;

        SetObserver(String operation, Runnable next) {
            this.operation = operation;
            this.next = next;
        }

        @Override
        public void onSuccess() {
            next.run();
        }

        @Override
        public void onFailure(String error) {
            negotiationFailed(operation, error);
        }
    }

    private final class DescriptionObserver implements CreateSessionDescriptionObserver {
        private final String operation;
        private final Consumer<RTCSessionDescription> next;

        DescriptionObserver(String operation, Consumer<RTCSessionDescription> next) {
            this.operation = operation;
            this.next = next;
        }

        @Override
        public void onSuccess(RTCSessionDescription description) {
            next.accept(description);
        }

        @Override
        public void onFailure(String error) {
            negotiationFailed(operation, error);
        }
    }

    /** Turns webrtc-java's observer calls into this peer's events. */
    private final class Events implements PeerConnectionObserver {

        @Override
        public void onIceCandidate(RTCIceCandidate candidate) {
            onLocalCandidate(candidate.sdp, candidate.sdpMid);
        }

        @Override
        public void onIceGatheringChange(RTCIceGatheringState state) {
            onGatheringState(state);
        }

        @Override
        public void onIceConnectionChange(RTCIceConnectionState state) {
            onIceConnectionState(state);
        }

        @Override
        public void onConnectionChange(RTCPeerConnectionState state) {
            onConnectionState(state);
        }

        @Override
        public void onSelectedCandidatePairChanged(RTCCandidatePairChangeEvent event) {
            onSelectedPair(event.getLocal().sdp, event.getRemote().sdp);
        }

        @Override
        public void onDataChannel(RTCDataChannel dataChannel) {
            onRemoteChannel(new NativeLink.RtcLink(dataChannel));
        }
    }
}

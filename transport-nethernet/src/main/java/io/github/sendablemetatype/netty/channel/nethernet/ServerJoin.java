package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.JoinFailedMessage.JoinProgress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.RemoteDescriptionRejectedException;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AnswerCandidates;
import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateAnnouncement;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidateIntake;
import io.github.sendablemetatype.netty.channel.nethernet.ice.TrickledAnswerCandidates;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.RemoteSignal;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.channel.EventLoop;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The server side of one admitted join: answers through the join's reply path, trickles local
 * candidates on paths that trickle, applies the client's later signals, and warns once, saying
 * how far it got, when the join never carries traffic.
 *
 * <p>Remote signals arrive on the endpoint's event loop; the answer and local candidates go out
 * from the child's, so local candidates always follow the answer.
 */
final class ServerJoin implements NetherNetChildChannel.Negotiation {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(ServerJoin.class);
    private static final String END_OF_CANDIDATES = "a=end-of-candidates";

    /**
     * What every join of one endpoint answers with.
     *
     * @param identity signs every answer
     * @param inferCandidates whether to check guessed candidates at the client's signaled address
     * @param icePort the shared ICE port, 0 when each session takes its own
     * @param relayed whether PROXY protocol relays front the ICE port
     */
    record Settings(ServerIdentity identity, boolean inferCandidates, int icePort, boolean relayed) {
    }

    private final Join join;
    private final String id;
    private final Settings settings;
    private final RemoteCandidateIntake intake;
    /** The offer as the engine gets it, run through the intake before any trickled candidate. */
    private final String remoteDescription;
    private final CandidateAnnouncement announcement;
    private final TrickledAnswerCandidates trickled;
    private final Runnable handshakeDone;
    private final Consumer<JoinRefusal> refused;
    private volatile NetherNetChildChannel child;
    private volatile JoinProgress progress = JoinProgress.NONE;
    private volatile boolean answerHandedOn;
    private final AtomicBoolean refused = new AtomicBoolean();
    private final AtomicBoolean warned = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();

    // Child event loop only.
    private boolean answerSent;
    private boolean gatheringCompleteHeld;
    private final List<String> pendingCandidates = new ArrayList<>();

    /**
     * @param handshakeDone gives the join's pending handshake back, once it went active
     * @param refused told about the refusal this join sends, if it sends one
     */
    ServerJoin(Join join, Settings settings, RemoteCandidateIntake intake, CandidateAnnouncement announcement,
               Runnable handshakeDone, Consumer<JoinRefusal> refused) {
        this.join = join;
        this.id = Long.toUnsignedString(join.connectionId());
        this.settings = settings;
        this.intake = intake;
        // Now, on the endpoint's loop: the client's trickled candidates take the offer's mid, and
        // they can arrive before the child's own loop starts negotiating.
        this.remoteDescription = intake.remoteDescription(join.offer());
        this.announcement = announcement;
        this.trickled = join.reply().trickles() ? AnswerCandidates.trickled(announcement) : null;
        this.handshakeDone = handshakeDone;
        this.refused = refused;
    }

    String id() {
        return id;
    }

    /**
     * The child exists and owns its peer: the client's later signals go to it from now on, on the
     * endpoint's event loop. A client that never got the answer closes it, also when the answer
     * was handed on: the source could not deliver it, or its deadline passed while it was on its
     * way. A child that went active stays, whatever the signaling says.
     */
    void attach(NetherNetChildChannel child, EventLoop endpointLoop) {
        this.child = child;
        join.reply().onRemoteSignal(signal -> onLoop(endpointLoop, () -> remoteSignal(signal)));
        join.reply().abandoned().whenComplete((ignored, failure) -> onLoop(endpointLoop, () -> {
            if (child.isOpen() && !child.isActive()) {
                log.debug("Join {} was abandoned before the client got its answer", id);
                child.close();
            }
        }));
    }

    private void remoteSignal(RemoteSignal signal) {
        NetherNetChildChannel current = child;
        if (current == null || !current.isOpen()) {
            return;
        }
        switch (signal) {
            case RemoteSignal.Candidate candidate -> {
                Peer peer = current.peer();
                if (peer != null) {
                    // The engine holds candidates until the offer applied.
                    intake.trickled(candidate.candidate())
                            .ifPresent(taken -> peer.addRemoteCandidate(taken.sdp(), taken.sdpMid()));
                }
            }
            case RemoteSignal.Error error -> {
                log.debug("Join {} received CONNECTERROR {}", id, error.code());
                current.connectErrorReceived();
            }
        }
    }

    /** Child event loop, from the child's initializer. */
    void negotiate() {
        NetherNetChildChannel current = child;
        Peer peer = current.peer();
        if (peer == null) {
            return;
        }
        CompletionStage<String> answer = peer.answer(remoteDescription);
        CompletionStage<String> gathered = answer.thenCompose(ignored -> peer.gatheredLocalDescription());
        if (trickled != null) {
            answer.whenComplete((sdp, failure) -> onChildLoop(() -> answered(sdp, failure)));
            gathered.whenComplete((sdp, failure) -> onChildLoop(() -> {
                if (failure == null) {
                    gatheredWhileTrickling(sdp);
                }
            }));
        } else {
            // Request/response signaling carries every candidate in the answer.
            gathered.whenComplete((sdp, failure) -> onChildLoop(() -> fullAnswer(sdp, failure)));
        }
    }

    private void answered(String sdp, Throwable failure) {
        if (!child.isOpen() || refused.get()) {
            return;
        }
        if (failure != null) {
            negotiationFailed(failure);
            return;
        }
        if (!handOn(sdp)) {
            return;
        }
        answerSent = true;
        for (String candidate : pendingCandidates) {
            sendCandidates(trickled.local(candidate));
        }
        pendingCandidates.clear();
        if (gatheringCompleteHeld) {
            sendCandidates(trickled.gatheringComplete());
        }
    }

    private void gatheredWhileTrickling(String localDescription) {
        if (!child.isOpen()) {
            return;
        }
        if (answerSent) {
            sendCandidates(trickled.gatheringComplete());
        } else {
            gatheringCompleteHeld = true;
        }
        inferCandidates(localDescription);
    }

    private void fullAnswer(String gathered, Throwable failure) {
        if (!child.isOpen() || refused.get()) {
            return;
        }
        if (failure != null) {
            negotiationFailed(failure);
            return;
        }
        inferCandidates(gathered);
        // Candidates the announcement adds go ahead of the terminator, which the engine appended.
        String announced = AnswerCandidates.fullAnswer(withoutEndOfCandidates(gathered), announcement);
        handOn(announced + END_OF_CANDIDATES + lineEnding(announced));
    }

    /** Signs the answer and hands it to the source. False when it could not be signed. */
    private boolean handOn(String sdp) {
        String signed;
        try {
            signed = settings.identity().augmentAnswer(sdp);
        } catch (Exception e) {
            // Clients refuse an answer without an identity only after parsing it; failing now is
            // the faster failure.
            log.warn("Could not sign the answer for join {}; failing it: {}", id, e.getMessage());
            refuse(JoinRefusal.INTERNAL);
            child.close();
            return false;
        }
        answerHandedOn = true;
        sent(join.reply().answer(signed), "answer");
        return true;
    }

    @Override
    public void localCandidate(String candidate, String mid) {
        NetherNetChildChannel current = child;
        if (trickled == null || current == null) {
            return;
        }
        onChildLoop(() -> {
            if (!current.isOpen()) {
                return;
            }
            if (answerSent) {
                sendCandidates(trickled.local(candidate));
            } else {
                pendingCandidates.add(candidate);
            }
        });
    }

    private void sendCandidates(List<String> candidates) {
        for (String candidate : candidates) {
            sent(join.reply().candidate(candidate), "candidate");
        }
    }

    /**
     * Adds the guessed candidates at the client's signaled address, once this side has gathered
     * and found nothing public of its own.
     */
    private void inferCandidates(String localDescription) {
        Peer peer = child.peer();
        if (!settings.inferCandidates() || peer == null) {
            return;
        }
        List<RemoteCandidate> inferred = intake.inferredCandidates(localDescription, join.source().address());
        if (!inferred.isEmpty()) {
            log.debug("Inferred candidates for join {}: {}", id,
                    inferred.stream().map(RemoteCandidate::sdp).collect(Collectors.joining(", ")));
        }
        for (RemoteCandidate candidate : inferred) {
            peer.addRemoteCandidate(candidate.sdp(), candidate.sdpMid());
        }
    }

    private void negotiationFailed(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
        log.debug("Negotiation for join {} failed: {}", id, cause.toString());
        child.connectionFailed(NetherConnectionFailure.NEGOTIATION_FAILED);
        refuse(cause instanceof RemoteDescriptionRejectedException ? JoinRefusal.BAD_OFFER : JoinRefusal.INTERNAL);
        child.close();
    }

    /**
     * Tells the client the join failed, once. A path that does not trickle has nothing to say
     * after its answer went out. Any thread.
     */
    void refuse(JoinRefusal refusal) {
        if (!refused.compareAndSet(false, true) || (trickled == null && answerHandedOn)) {
            return;
        }
        refused.accept(refusal);
        try {
            sent(join.reply().refuse(refusal), "refusal");
        } catch (RuntimeException e) {
            log.debug("Could not refuse join {}: {}", id, e.toString());
        }
    }

    @Override
    public void progressed(JoinProgress reached) {
        if (reached.compareTo(progress) > 0) {
            progress = reached;
        }
    }

    @Override
    public void activated() {
        handshakeDone.run();
    }

    @Override
    public void failed(CloseCause cause) {
        switch (cause) {
            case NEGOTIATION_FAILED -> refuse(JoinRefusal.INTERNAL);
            case REMOTE_DESCRIPTION_REJECTED -> {
                log.debug("Join {} sent an offer the engine refused", id);
                refuse(JoinRefusal.BAD_OFFER);
            }
            // The client left; nothing a host could fix.
            case REMOTE_CLOSED, REMOTE_ABORTED -> log.debug("Join {} ended by the client before it went active", id);
            case ICE_FAILED, TRANSPORT_FAILED, UNKNOWN -> warn("the peer connection failed: " + cause);
        }
    }

    /** The handshake timeout closed the child. */
    void timedOut(int seconds) {
        warn("timed out after " + seconds + "s");
    }

    /** The child closed: the source may forget the join. Once, from the child's close future. */
    void closed() {
        if (finished.compareAndSet(false, true)) {
            join.reply().finished();
        }
    }

    /** Warns that a player could not connect, once per join. */
    private void warn(String reason) {
        if (!warned.compareAndSet(false, true)) {
            return;
        }
        NetherNetChildChannel current = child;
        OfferCheckResult check = current == null ? null : current.getOfferCheck();
        String player = check == null ? null : check.displayName().orElse(null);
        JoinProgress reached = progress;
        log.warn(JoinFailedMessage.of(player, reached, settings.icePort(), settings.relayed()));
        log.debug("Join {} from {} failed: {}{}", id, join.source(), reason,
                reached == JoinProgress.PEER_CONNECTED ? ", after the peer connected"
                        : reached == JoinProgress.ICE_CONNECTED ? ", after ICE connected" : "");
    }

    private void sent(CompletionStage<Void> send, String what) {
        send.whenComplete((ignored, failure) -> {
            if (failure != null) {
                // Only this join is affected; the handshake timeout reaps it if it cannot complete.
                log.debug("Could not send the {} for join {}: {}", what, id, failure.toString());
            }
        });
    }

    private void onChildLoop(Runnable task) {
        onLoop(child.eventLoop(), task);
    }

    private void onLoop(EventLoop loop, Runnable task) {
        try {
            loop.execute(task);
        } catch (RejectedExecutionException shuttingDown) {
            log.debug("Dropping a negotiation step for join {}: its event loop is shutting down", id);
        }
    }

    private static String withoutEndOfCandidates(String sdp) {
        String eol = lineEnding(sdp);
        StringBuilder out = new StringBuilder(sdp.length());
        for (String line : sdp.split("\r\n|\n")) {
            if (!line.isEmpty() && !line.equals(END_OF_CANDIDATES)) {
                out.append(line).append(eol);
            }
        }
        return out.toString();
    }

    private static String lineEnding(String sdp) {
        return sdp.contains("\r\n") || !sdp.contains("\n") ? "\r\n" : "\n";
    }
}

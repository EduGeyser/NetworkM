package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The reply path of a join over message based signaling (Xbox RPC, LAN): the answer and each
 * candidate go to the joining client as {@code CONNECTRESPONSE} and {@code CANDIDATEADD}
 * messages, a refusal as {@code CONNECTERROR} with the refusal's error code. The client's later
 * messages for the join arrive through {@link #received}.
 */
abstract class MessageReplyPath implements ReplyPath {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(MessageReplyPath.class);
    /** Far more signals than one join sends before the endpoint listens, so only a flood reaches it. */
    static final int MAX_EARLY_SIGNALS = 64;

    private final String connectionId;
    private final CompletableFuture<Void> abandoned = new CompletableFuture<>();
    private final AtomicBoolean finished = new AtomicBoolean();
    // Guarded by this.
    private final List<RemoteSignal> early = new ArrayList<>();
    private Consumer<RemoteSignal> handler;

    MessageReplyPath(String connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * Sends one signaling message to the joining client.
     *
     * @return completes when the source handed it on
     */
    abstract CompletionStage<Void> send(String message);

    /** Drops what the source keeps for this join. Called once. */
    abstract void release();

    @Override
    public boolean trickles() {
        return true;
    }

    @Override
    public CompletionStage<Void> answer(String sdp) {
        return send(new Signal(SignalType.CONNECT_RESPONSE, connectionId, sdp).wire());
    }

    @Override
    public CompletionStage<Void> candidate(String candidate) {
        return send(new Signal(SignalType.CANDIDATE_ADD, connectionId, candidate).wire());
    }

    @Override
    public CompletionStage<Void> refuse(JoinRefusal refusal) {
        return send(new Signal(SignalType.CONNECT_ERROR, connectionId, Integer.toString(refusal.errorCode())).wire());
    }

    @Override
    public void onRemoteSignal(Consumer<RemoteSignal> handler) {
        synchronized (this) {
            this.handler = handler;
            early.forEach(handler);
            early.clear();
        }
    }

    @Override
    public CompletionStage<Void> abandoned() {
        // A client reached by messages can always be sent to; only the handshake timeout ends it.
        return abandoned.minimalCompletionStage();
    }

    @Override
    public void finished() {
        if (finished.compareAndSet(false, true)) {
            release();
        }
    }

    /** A signal for this join from the client. Signals that do not concern a join in progress are dropped. */
    void received(Signal message) {
        String payload = message.payload();
        RemoteSignal signal = switch (message.type()) {
            case CANDIDATE_ADD -> payload.isEmpty() ? null : new RemoteSignal.Candidate(payload);
            case CONNECT_ERROR -> new RemoteSignal.Error(errorCode(payload));
            case CONNECT_REQUEST, CONNECT_RESPONSE -> null;
        };
        if (signal == null) {
            log.trace("Ignoring a {} signal for join {}", message.type().wireName(), connectionId);
            return;
        }
        synchronized (this) {
            if (handler != null) {
                handler.accept(signal);
            } else if (early.size() < MAX_EARLY_SIGNALS) {
                early.add(signal);
            } else {
                log.debug("Dropping a signal for join {}: {} are already waiting",
                        connectionId, MAX_EARLY_SIGNALS);
            }
        }
    }

    /** Vanilla sends a decimal error code; anything else counts as code 0. */
    private static int errorCode(String payload) {
        try {
            return Integer.parseInt(payload.strip());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}

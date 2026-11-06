package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
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
        return send(NetherNetConstants.buildSignalConnectResponse(connectionId, sdp));
    }

    @Override
    public CompletionStage<Void> candidate(String candidate) {
        return send(NetherNetConstants.buildSignalCandidateAdd(connectionId, candidate));
    }

    @Override
    public CompletionStage<Void> refuse(JoinRefusal refusal) {
        return send(NetherNetConstants.buildSignalConnectError(connectionId, refusal.errorCode()));
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

    /**
     * A message for this join from the client, in its wire form {@code <TYPE> <id> <payload>}.
     * Signals that do not concern a join in progress are dropped.
     */
    void received(String message) {
        String[] parts = message.split(" ", 3);
        String payload = parts.length > 2 ? parts[2] : "";
        RemoteSignal signal = switch (parts[0]) {
            case NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD -> payload.isEmpty() ? null
                    : new RemoteSignal.Candidate(payload);
            case NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR -> new RemoteSignal.Error(errorCode(payload));
            default -> null;
        };
        if (signal == null) {
            log.trace("Ignoring signal {} for join {}", parts[0], connectionId);
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

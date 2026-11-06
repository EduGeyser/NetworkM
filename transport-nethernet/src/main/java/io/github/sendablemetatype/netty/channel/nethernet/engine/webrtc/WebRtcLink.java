package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLinkClosedException;
import io.github.sendablemetatype.webrtc.RTCDataChannelBuffer;
import io.github.sendablemetatype.webrtc.RTCDataChannelObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelSendObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A data link on a webrtc-java data channel.
 *
 * <p>Queued bytes are counted in Java: up on each send, down by the decreases
 * libwebrtc reports through {@code onBufferedAmountChange}. Reading libwebrtc's
 * own buffered amount would wait for the network thread on every read.
 *
 * <p>libwebrtc counts a message in its buffered amount as soon as it tries to
 * send it, also when it then refuses it, and reports the refused bytes with a
 * later decrease ({@code SctpDataChannel::SendImpl}). So a refused send stays
 * counted until that decrease, or for good if the channel closes first; only
 * a send libwebrtc discarded without trying it, or one that never reached it,
 * leaves the count with its failure.
 *
 * <p>libwebrtc calls the channel's observer on the factory's network thread,
 * rather than posting each event to the signaling thread first. There the
 * observer only counts, reads the channel's state and error, which libwebrtc
 * answers on that thread at once, and hands the events to the listener, which
 * must not block either. It never calls a peer connection method: that would
 * wait for the signaling thread, which may be waiting for the network thread.
 *
 * <p>A send libwebrtc refuses because the channel is not open fails with a
 * {@link DataLinkClosedException} naming why the channel closed, and only
 * after the listener heard of that close. Send results arrive on webrtc-java's
 * callback thread, the close on the network thread, or on the signaling
 * thread with natives that make libwebrtc post data channel events there, so
 * the refusal of a send made just after a client quit can arrive before the
 * close: on those natives, 13 of 40 aborts during a burst of sends had a
 * refusal arrive first (measured 2026-11-06). Such a refusal waits for the
 * close.
 */
final class WebRtcLink implements DataLink {

    /**
     * The start of webrtc-java's failure for every send libwebrtc refused
     * because the channel was not open. While the remote peer closes the
     * channel in an orderly way, for a round trip, libwebrtc tries the send
     * and refuses it ({@code SctpDataChannel::SendImpl}); once the channel
     * closed, for any cause, such as a client's quit, it refuses the send
     * without trying it ({@code SctpDataChannel::SendAsync}). Other refusals
     * carry other error types.
     */
    static final String NOT_OPEN = "[INVALID_STATE] ";

    /**
     * webrtc-java's failure for a send whose result libwebrtc dropped without
     * calling it ({@code RTCDataChannelSendObserver}'s destructor): its
     * network thread stopped before it ran the send, so its buffered amount
     * never counted it.
     */
    static final String DISCARDED = NOT_OPEN + "Send operation was discarded before completion";

    private final WebRtcPeer peer;
    private final ChannelParameters parameters;
    private final RTCDataChannelObserver observer = new Observer();
    private volatile NativeLink channel;

    private final AtomicLong queued = new AtomicLong();
    private volatile long lowWatermark;

    private final Object sends = new Object();
    // Guarded by sends: the sends handed to libwebrtc whose completion has not
    // finished, and whether sending stopped. Sends register only before the
    // stop, so after it the set only shrinks.
    private final Set<PendingSend> pending = new HashSet<>();
    private boolean stopped;
    private final CompletableFuture<Void> settled = new CompletableFuture<>();
    // Guarded by sends: why the channel closed, once the listener heard of it,
    // and the sends refused as not open that wait for that. They stay pending.
    private CloseCause closedBecause;
    private final List<PendingSend> refusedBeforeClose = new ArrayList<>();

    /**
     * Held for each event handed to the listener, so the events of this link
     * reach it one at a time: the network thread delivers them, and for a
     * remote link the thread that attached it reports the state it found.
     * Never held while waiting for one of libwebrtc's threads.
     */
    private final Object events = new Object();
    // Written under events; read outside it only to skip reading the error again.
    private volatile Reported reported = Reported.NOTHING;

    WebRtcLink(WebRtcPeer peer, ChannelParameters parameters, NativeLink channel) {
        this.peer = peer;
        this.parameters = parameters;
        this.channel = channel;
    }

    @Override
    public ChannelParameters parameters() {
        return parameters;
    }

    @Override
    public void send(ByteBuffer message, Consumer<Throwable> completion) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(completion, "completion");
        NativeLink target = channel;
        PendingSend send = new PendingSend(message.remaining(), completion);
        String refused;
        synchronized (sends) {
            refused = stopped ? "Data link is closed" : target == null ? "Data link is not open" : null;
            if (refused == null) {
                pending.add(send);
            }
        }
        if (refused != null) {
            completion.accept(new IllegalStateException(refused));
            return;
        }
        queued.addAndGet(send.size);
        try {
            target.sendAsync(message, send);
        } catch (RuntimeException | Error e) {
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            // No result follows a send that threw; libwebrtc never saw it.
            send.settle(e, true);
        }
    }

    @Override
    public long queuedBytes() {
        return queued.get();
    }

    @Override
    public void lowWatermark(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Negative low watermark: " + bytes);
        }
        lowWatermark = bytes;
    }

    /** Gives a link opened locally its native channel, once it exists. */
    void bind(NativeLink created) {
        channel = created;
        created.registerObserver(observer);
    }

    /**
     * Starts delivering events of a remote channel the listener kept.
     * libwebrtc announces a remote channel before it opens it, and opens it
     * right after the announcement returns
     * ({@code DataChannelController::OnDataChannelOpenMessage}), but a ready
     * transport can open it earlier ({@code OnReadyToSend}). A change before
     * the observer is registered fires no event, so the state is read once
     * the observer is: the messages that registration delivers report the
     * open first ({@link Observer#onMessage}).
     */
    void attachRemote() {
        NativeLink current = channel;
        current.registerObserver(observer);
        observer.onStateChange();
    }

    /**
     * Refuses further sends.
     *
     * @return completes once the completion of every send already handed to
     *         libwebrtc has run
     */
    CompletableFuture<Void> stopSending() {
        boolean idle;
        List<PendingSend> waiting;
        synchronized (sends) {
            stopped = true;
            idle = pending.isEmpty();
            waiting = List.copyOf(refusedBeforeClose);
            refusedBeforeClose.clear();
        }
        // No close event follows the teardown, and their results are in.
        waiting.forEach(PendingSend::settleRefusal);
        if (idle) {
            settled.complete(null);
        }
        return settled;
    }

    /**
     * The listener heard that the channel closed: refusals that waited for it
     * fail now, later ones at once.
     */
    private void closed(CloseCause cause) {
        List<PendingSend> waiting;
        synchronized (sends) {
            closedBecause = cause;
            waiting = List.copyOf(refusedBeforeClose);
            refusedBeforeClose.clear();
        }
        waiting.forEach(send -> send.settle(new DataLinkClosedException(cause), send.uncountedRefusal));
    }

    /**
     * Fails every send whose result has not arrived. A result that arrives
     * later finds its send settled and is dropped. Call after
     * {@link #stopSending()}.
     *
     * @return the stage {@link #stopSending()} returned
     */
    CompletableFuture<Void> abandonSends() {
        List<PendingSend> left;
        synchronized (sends) {
            left = List.copyOf(pending);
        }
        IOException cause = new IOException("The data link closed before the engine reported the send's result");
        left.forEach(send -> send.settle(cause, false));
        return settled;
    }

    NativeLink channel() {
        return channel;
    }

    /**
     * A send handed to libwebrtc. Its completion runs once: with the result
     * webrtc-java reports, or with a failure when teardown stops waiting.
     */
    private final class PendingSend implements RTCDataChannelSendObserver {
        private final int size;
        private final Consumer<Throwable> completion;
        // Guarded by sends.
        private boolean claimed;
        // Set under sends before the send waits in refusedBeforeClose, read after it left.
        private String refusal;
        private boolean uncountedRefusal;

        PendingSend(int size, Consumer<Throwable> completion) {
            this.size = size;
            this.completion = completion;
        }

        @Override
        public void onSuccess() {
            settle(null, false);
        }

        @Override
        public void onFailure(String error) {
            boolean uncounted = DISCARDED.equals(error);
            if (!error.startsWith(NOT_OPEN)) {
                settle(new IOException("Native send failed: " + error), uncounted);
                return;
            }
            CloseCause cause;
            synchronized (sends) {
                cause = closedBecause;
                if (cause == null && !stopped) {
                    refusal = error;
                    uncountedRefusal = uncounted;
                    refusedBeforeClose.add(this);
                    return;
                }
            }
            settle(cause != null ? new DataLinkClosedException(cause)
                    : new IOException("Native send failed: " + error), uncounted);
        }

        /** Fails a refusal that waited for a close the listener will not hear of. */
        void settleRefusal() {
            settle(new IOException("Native send failed: " + refusal), uncountedRefusal);
        }

        /**
         * Runs the completion unless the send is settled already.
         *
         * @param cause the failure, or null for success
         * @param uncounted whether libwebrtc never counted the message, so it
         *        leaves the queued bytes here
         */
        void settle(Throwable cause, boolean uncounted) {
            synchronized (sends) {
                if (claimed) {
                    return;
                }
                claimed = true;
            }
            try {
                if (uncounted) {
                    queued.addAndGet(-size);
                }
                completion.accept(cause);
            } finally {
                boolean last;
                synchronized (sends) {
                    pending.remove(this);
                    last = stopped && pending.isEmpty();
                }
                if (last) {
                    settled.complete(null);
                }
            }
        }
    }

    /** What the listener heard of the link's state; it never hears of an open after the close. */
    private enum Reported {
        NOTHING, OPEN, CLOSED
    }

    private final class Observer implements RTCDataChannelObserver {

        @Override
        public boolean isOkToCallOnTheNetworkThread() {
            return true;
        }

        @Override
        public void onBufferedAmountChange(long sentDataSize) {
            long after = queued.addAndGet(-sentDataSize);
            long level = lowWatermark;
            if (after <= level && after + sentDataSize > level) {
                synchronized (events) {
                    peer.dispatch(listener -> listener.onQueueLow(WebRtcLink.this));
                }
            }
        }

        /**
         * Also called by {@link WebRtcLink#attachRemote()}, on the thread that
         * attaches the link, where reading the state waits for the network
         * thread. The state is read before taking the lock, so the network
         * thread never waits for a thread that waits for it. That thread's
         * read can be older than one the network thread makes meanwhile,
         * which is why an open is not reported after the close.
         */
        @Override
        public void onStateChange() {
            NativeLink current = channel;
            if (current == null) {
                return;
            }
            RTCDataChannelState state = current.state();
            CloseCause cause = state == RTCDataChannelState.CLOSED && reported != Reported.CLOSED
                    ? peer.linkCloseCause(current.error()) : null;
            boolean closedNow = false;
            synchronized (events) {
                if (state == RTCDataChannelState.OPEN && reported == Reported.NOTHING) {
                    reported = Reported.OPEN;
                    peer.dispatch(listener -> listener.onChannelOpen(WebRtcLink.this));
                } else if (cause != null && reported != Reported.CLOSED) {
                    reported = Reported.CLOSED;
                    peer.dispatch(listener -> listener.onChannelClosed(WebRtcLink.this, cause));
                    closedNow = true;
                }
            }
            if (closedNow) {
                closed(cause);
            }
        }

        /**
         * libwebrtc delivers messages only while the channel is open, so a
         * message the listener gets before it heard of the open reports the
         * open first. That happens for a remote channel that opened before
         * its observer was registered: the registration delivers the
         * messages that arrived meanwhile, before the state is read.
         */
        @Override
        public void onMessage(RTCDataChannelBuffer buffer) {
            synchronized (events) {
                if (reported == Reported.NOTHING) {
                    reported = Reported.OPEN;
                    peer.dispatch(listener -> listener.onChannelOpen(WebRtcLink.this));
                }
                peer.deliver(WebRtcLink.this, buffer.data, buffer.binary);
            }
        }
    }
}

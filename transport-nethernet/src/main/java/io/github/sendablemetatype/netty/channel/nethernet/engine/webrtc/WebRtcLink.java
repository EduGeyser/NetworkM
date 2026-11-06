package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.webrtc.RTCDataChannelBuffer;
import io.github.sendablemetatype.webrtc.RTCDataChannelObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelSendObserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
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
 */
final class WebRtcLink implements DataLink {

    /**
     * webrtc-java's failure for a send whose result libwebrtc dropped without
     * calling it ({@code RTCDataChannelSendObserver}'s destructor): libwebrtc
     * found no transport, or its network thread stopped, before it tried the
     * send, so its buffered amount never counted it.
     */
    static final String DISCARDED = "[INVALID_STATE] Send operation was discarded before completion";

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

    private final AtomicBoolean openReported = new AtomicBoolean();
    private final AtomicBoolean closeReported = new AtomicBoolean();

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

    /** Starts delivering events of a remote channel the listener kept. */
    void attachRemote() {
        NativeLink current = channel;
        current.registerObserver(observer);
        // A remote channel is usually open already when libwebrtc announces it.
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
        synchronized (sends) {
            stopped = true;
            idle = pending.isEmpty();
        }
        if (idle) {
            settled.complete(null);
        }
        return settled;
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
            settle(new IOException("Native send failed: " + error), DISCARDED.equals(error));
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

    private final class Observer implements RTCDataChannelObserver {

        @Override
        public void onBufferedAmountChange(long sentDataSize) {
            long after = queued.addAndGet(-sentDataSize);
            long level = lowWatermark;
            if (after <= level && after + sentDataSize > level) {
                peer.dispatch(listener -> listener.onQueueLow(WebRtcLink.this));
            }
        }

        @Override
        public void onStateChange() {
            NativeLink current = channel;
            if (current == null) {
                return;
            }
            switch (current.state()) {
                case OPEN -> {
                    if (openReported.compareAndSet(false, true)) {
                        peer.dispatch(listener -> listener.onChannelOpen(WebRtcLink.this));
                    }
                }
                case CLOSED -> {
                    if (closeReported.compareAndSet(false, true)) {
                        CloseCause cause = peer.linkCloseCause(current.error());
                        peer.dispatch(listener -> listener.onChannelClosed(WebRtcLink.this, cause));
                    }
                }
                default -> {
                }
            }
        }

        @Override
        public void onMessage(RTCDataChannelBuffer buffer) {
            peer.deliver(WebRtcLink.this, buffer.data);
        }
    }
}

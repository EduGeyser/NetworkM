package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.webrtc.RTCDataChannelBuffer;
import io.github.sendablemetatype.webrtc.RTCDataChannelObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelSendObserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A data link on a webrtc-java data channel.
 *
 * <p>Queued bytes are counted in Java: up on each send, down by the decreases
 * libwebrtc reports through {@code onBufferedAmountChange}. Reading libwebrtc's
 * own buffered amount would wait for the network thread on every read.
 */
final class WebRtcLink implements DataLink {
    private final WebRtcPeer peer;
    private final ChannelParameters parameters;
    private final RTCDataChannelObserver observer = new Observer();
    private volatile NativeLink channel;

    private final AtomicLong queued = new AtomicLong();
    private volatile long lowWatermark;

    // Sends handed to the engine whose result has not arrived. A send counts
    // itself before it reads stopped, and stopSending sets stopped before it
    // reads the count, so a send either fails at once or is waited for.
    private final AtomicInteger outstanding = new AtomicInteger();
    private volatile boolean stopped;
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
        int size = message.remaining();
        outstanding.incrementAndGet();
        NativeLink target = channel;
        if (stopped || target == null) {
            settle(completion, new IllegalStateException(stopped ? "Data link is closed" : "Data link is not open"));
            return;
        }
        queued.addAndGet(size);
        try {
            target.sendAsync(message, new RTCDataChannelSendObserver() {
                @Override
                public void onSuccess() {
                    settle(completion, null);
                }

                @Override
                public void onFailure(String error) {
                    // libwebrtc never counted a refused message in its buffered amount.
                    queued.addAndGet(-size);
                    settle(completion, new IOException("Native send failed: " + error));
                }
            });
        } catch (RuntimeException | Error e) {
            if (!NativeErrors.isCallFailure(e)) {
                throw e;
            }
            queued.addAndGet(-size);
            settle(completion, e);
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

    private void settle(Consumer<Throwable> completion, Throwable cause) {
        try {
            completion.accept(cause);
        } finally {
            if (outstanding.decrementAndGet() == 0 && stopped) {
                settled.complete(null);
            }
        }
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
     * @return completes once every send already handed to the engine has its result
     */
    CompletableFuture<Void> stopSending() {
        stopped = true;
        if (outstanding.get() == 0) {
            settled.complete(null);
        }
        return settled;
    }

    NativeLink channel() {
        return channel;
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

package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A data link of {@link FakePeer}. Sends are recorded and settled by the test
 * (or at once, see {@link FakeEngine#autoAcceptSends}). A send adds its size
 * to {@link #queuedBytes()} when it is made, a refusal takes it out again,
 * and otherwise it stays until the test drains it.
 */
public final class FakeDataLink implements DataLink {
    private final FakePeer peer;
    private final ChannelParameters parameters;
    private final List<Send> sends = new ArrayList<>();
    private long queued; // guarded by this
    private long watermark; // guarded by this
    private boolean announced;
    private volatile boolean closed;
    private volatile Throwable refusal;

    FakeDataLink(FakePeer peer, ChannelParameters parameters) {
        this.peer = peer;
        this.parameters = parameters;
        // A link the transport opened itself needs no announcement.
        this.announced = peer.config().role() == PeerConfig.Role.OFFERER;
    }

    /** One send: the copied bytes and whether the engine settled it yet. */
    public static final class Send {
        private final byte[] bytes;
        private final boolean direct;
        private final Consumer<Throwable> completion;
        private boolean settled;

        private Send(byte[] bytes, boolean direct, Consumer<Throwable> completion) {
            this.bytes = bytes;
            this.direct = direct;
            this.completion = completion;
        }

        public byte[] bytes() {
            return bytes.clone();
        }

        public boolean direct() {
            return direct;
        }
    }

    @Override
    public ChannelParameters parameters() {
        return parameters;
    }

    @Override
    public void send(ByteBuffer message, Consumer<Throwable> completion) {
        byte[] bytes = new byte[message.remaining()];
        message.duplicate().get(bytes);
        Send send = new Send(bytes, message.isDirect(), completion);
        Throwable refused;
        synchronized (this) {
            sends.add(send);
            queued += bytes.length;
            refused = closed ? new IOException("The data link is closed") : refusal;
        }
        if (refused != null) {
            settle(send, refused);
        } else if (peer.autoAcceptSends()) {
            settle(send, null);
        }
    }

    @Override
    public synchronized long queuedBytes() {
        return queued;
    }

    @Override
    public synchronized void lowWatermark(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Negative watermark");
        }
        watermark = bytes;
    }

    // ---- What the test inspects and plays ----

    public synchronized List<byte[]> sentBytes() {
        return sends.stream().map(Send::bytes).toList();
    }

    public synchronized List<Send> sends() {
        return List.copyOf(sends);
    }

    public synchronized int sendCount() {
        return sends.size();
    }

    public synchronized long watermark() {
        return watermark;
    }

    public boolean closed() {
        return closed;
    }

    /** Refuses every later send at once with this cause, or null to stop. */
    public void refuseSends(Throwable cause) {
        this.refusal = cause;
    }

    /** Accepts the send at this index. */
    public void accept(int index) {
        settle(send(index), null);
    }

    /** Refuses the send at this index. */
    public void reject(int index, Throwable cause) {
        settle(send(index), cause);
    }

    /** Accepts every send not settled yet. */
    public void acceptAll() {
        List<Send> unsettled;
        synchronized (this) {
            unsettled = sends.stream().filter(send -> !send.settled).toList();
        }
        for (Send send : unsettled) {
            settle(send, null);
        }
    }

    /**
     * Lowers the queue as if the engine sent this many bytes, firing
     * {@link PeerListener#onQueueLow} when it falls to the watermark.
     */
    public void drain(long bytes) {
        boolean low;
        synchronized (this) {
            boolean above = queued > watermark;
            queued = Math.max(0, queued - bytes);
            low = above && queued <= watermark;
        }
        if (low) {
            peer.queueLow(this);
        }
    }

    /** Sets the queue directly; lowering it below the watermark fires {@link PeerListener#onQueueLow}. */
    public void setQueuedBytes(long bytes) {
        long current;
        synchronized (this) {
            current = queued;
            if (bytes >= current) {
                queued = bytes;
                return;
            }
        }
        drain(current - bytes);
    }

    synchronized boolean announced() {
        return announced;
    }

    synchronized void announce() {
        announced = true;
    }

    /** Closes the link and refuses its unsettled sends. Returns false if it was closed already. */
    boolean closeByEngine() {
        List<Send> unsettled;
        synchronized (this) {
            if (closed) {
                return false;
            }
            closed = true;
            unsettled = sends.stream().filter(send -> !send.settled).toList();
        }
        for (Send send : unsettled) {
            settle(send, new IOException("The data link closed"));
        }
        return true;
    }

    private synchronized Send send(int index) {
        return sends.get(index);
    }

    private void settle(Send send, Throwable cause) {
        synchronized (this) {
            if (send.settled) {
                throw new IllegalStateException("A send is settled exactly once");
            }
            send.settled = true;
            if (cause != null) {
                queued = Math.max(0, queued - send.bytes.length);
            }
        }
        send.completion.accept(cause);
    }
}

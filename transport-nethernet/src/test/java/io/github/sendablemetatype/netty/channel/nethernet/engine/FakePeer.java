package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A peer of {@link FakeEngine}. The {@code Peer} methods record what the
 * transport asked for; the other public methods play the engine's side.
 */
public final class FakePeer implements Peer {
    private final FakeEngine engine;
    private final PeerConfig config;
    private final PeerListener listener;
    // Serializes callbacks, as the contract promises per peer.
    private final Object serial = new Object();
    // Guards running and terminating; never held while a callback runs.
    private final Object state = new Object();
    // Callbacks and send settlements in progress; the close stage waits for them.
    private int running;
    private boolean terminating;

    private final CompletableFuture<String> answer = new CompletableFuture<>();
    private final CompletableFuture<String> offer = new CompletableFuture<>();
    private final CompletableFuture<String> gathered = new CompletableFuture<>();
    private final CompletableFuture<Void> closeStage = new CompletableFuture<>();
    private final CompletionStage<Void> closeView = closeStage.minimalCompletionStage();
    private final List<String> remoteOffers = new ArrayList<>();
    private final List<String> remoteAnswers = new ArrayList<>();
    private final List<RemoteCandidate> remoteCandidates = new ArrayList<>();
    private final List<FakeDataLink> links = new ArrayList<>();
    private final AtomicInteger closeCalls = new AtomicInteger();
    private final AtomicInteger metricsRequests = new AtomicInteger();
    private final AtomicInteger gatheredRequests = new AtomicInteger();
    private volatile CompletableFuture<Void> applyAnswerResult = CompletableFuture.completedFuture(null);
    private volatile TransportMetrics metrics = TransportMetrics.NONE;
    private volatile boolean localDescription;
    private volatile boolean closeCalled;
    private volatile String closeThread;
    private volatile boolean ended;

    /** A remote candidate the transport applied. */
    public record RemoteCandidate(String candidate, String mid) {
    }

    FakePeer(FakeEngine engine, PeerConfig config, PeerListener listener) {
        this.engine = engine;
        this.config = config;
        this.listener = listener;
    }

    public PeerConfig config() {
        return config;
    }

    // ---- Peer ----

    @Override
    public CompletionStage<String> answer(String remoteOffer) {
        requireRole(PeerConfig.Role.ANSWERER);
        synchronized (remoteOffers) {
            remoteOffers.add(remoteOffer);
        }
        String scripted = engine.scriptedAnswer();
        if (scripted != null) {
            completeAnswer(scripted);
        }
        return answer.copy();
    }

    @Override
    public CompletionStage<String> offer() {
        requireRole(PeerConfig.Role.OFFERER);
        String scripted = engine.scriptedOffer();
        if (scripted != null) {
            completeOffer(scripted);
        }
        return offer.copy();
    }

    @Override
    public CompletionStage<Void> applyAnswer(String remoteAnswer) {
        requireRole(PeerConfig.Role.OFFERER);
        synchronized (remoteAnswers) {
            remoteAnswers.add(remoteAnswer);
        }
        return applyAnswerResult.copy();
    }

    @Override
    public void addRemoteCandidate(String candidate, String mid) {
        synchronized (remoteCandidates) {
            remoteCandidates.add(new RemoteCandidate(candidate, mid));
        }
    }

    @Override
    public CompletionStage<String> gatheredLocalDescription() {
        String scripted = engine.scriptedGathering();
        if (gatheredRequests.getAndIncrement() == 0 && scripted != null) {
            CompletableFuture.runAsync(() -> gatheringComplete(scripted));
        }
        return gathered.copy();
    }

    @Override
    public DataLink openChannel(ChannelParameters parameters) {
        requireRole(PeerConfig.Role.OFFERER);
        FakeDataLink link = new FakeDataLink(this, parameters);
        synchronized (links) {
            links.add(link);
        }
        return link;
    }

    @Override
    public CompletionStage<TransportMetrics> metrics() {
        metricsRequests.incrementAndGet();
        return CompletableFuture.completedFuture(closeCalled ? TransportMetrics.NONE : metrics);
    }

    @Override
    public CompletionStage<Void> close() {
        if (closeCalls.getAndIncrement() == 0) {
            closeThread = Thread.currentThread().getName();
            closeCalled = true;
            List<FakeDataLink> current;
            synchronized (links) {
                current = List.copyOf(links);
            }
            current.forEach(FakeDataLink::closeByEngine);
            if (!engine.holdPeerClose()) {
                finishClose();
            }
        }
        return closeView;
    }

    // ---- What the test inspects ----

    /** How often the transport called {@link #close()}. */
    public int closeCalls() {
        return closeCalls.get();
    }

    public boolean closeCalled() {
        return closeCalled;
    }

    /** The thread that first called {@link #close()}, or null. */
    public String closeThread() {
        return closeThread;
    }

    /** Whether the close stage completed. */
    public boolean terminated() {
        return closeStage.isDone();
    }

    public List<String> remoteOffers() {
        synchronized (remoteOffers) {
            return List.copyOf(remoteOffers);
        }
    }

    public List<String> remoteAnswers() {
        synchronized (remoteAnswers) {
            return List.copyOf(remoteAnswers);
        }
    }

    public List<RemoteCandidate> remoteCandidates() {
        synchronized (remoteCandidates) {
            return List.copyOf(remoteCandidates);
        }
    }

    /** The links the transport opened, or the remote opened, in order. */
    public List<FakeDataLink> links() {
        synchronized (links) {
            return List.copyOf(links);
        }
    }

    public int metricsRequests() {
        return metricsRequests.get();
    }

    /** How often the transport asked for the gathered local description. */
    public int gatheredRequests() {
        return gatheredRequests.get();
    }

    // ---- The engine's side ----

    /** Completes the answer stage; local candidates may follow. */
    public void completeAnswer(String sdp) {
        localDescription = true;
        answer.complete(sdp);
    }

    public void failAnswer(Throwable cause) {
        answer.completeExceptionally(cause);
    }

    /** Completes the offer stage; local candidates may follow. */
    public void completeOffer(String sdp) {
        localDescription = true;
        offer.complete(sdp);
    }

    public void failOffer(Throwable cause) {
        offer.completeExceptionally(cause);
    }

    /** What later {@link #applyAnswer} calls return. */
    public void applyAnswerResult(CompletableFuture<Void> result) {
        this.applyAnswerResult = result;
    }

    public void setMetrics(TransportMetrics metrics) {
        this.metrics = metrics;
    }

    /** Fires a local candidate, which the contract allows only after the local description. */
    public void localCandidate(String candidate, String mid) {
        if (!localDescription) {
            throw new IllegalStateException("A local candidate cannot precede the local description");
        }
        fire(() -> listener.onLocalCandidate(candidate, mid));
    }

    /** Ends gathering: fires the event and completes {@link #gatheredLocalDescription()}. */
    public void gatheringComplete(String fullDescription) {
        fire(listener::onGatheringComplete);
        gathered.complete(fullDescription);
    }

    /**
     * The remote peer opens a data channel. A link the transport refuses is
     * closed, as the engine would.
     */
    public FakeDataLink remoteChannel(ChannelParameters parameters) {
        requireRole(PeerConfig.Role.ANSWERER);
        FakeDataLink link = new FakeDataLink(this, parameters);
        synchronized (links) {
            links.add(link);
        }
        boolean[] kept = new boolean[1];
        fire(() -> kept[0] = listener.onRemoteChannel(link));
        if (kept[0]) {
            link.announce();
        } else {
            link.closeByEngine();
        }
        return link;
    }

    public void open(FakeDataLink link) {
        fire(() -> listener.onChannelOpen(link));
    }

    /**
     * Delivers a message. The buffer is a direct copy that is overwritten
     * once the callback returns, so a transport that keeps it sees garbage.
     */
    public void message(FakeDataLink link, byte... bytes) {
        if (!link.announced()) {
            throw new IllegalStateException("A link's messages cannot precede its announcement");
        }
        if (link.closed()) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length).put(bytes).flip();
        fire(() -> listener.onMessage(link, buffer));
        buffer.clear();
        while (buffer.hasRemaining()) {
            buffer.put((byte) 0x5A);
        }
    }

    public void path(TransportPath path) {
        fire(() -> listener.onPathSelected(path));
    }

    public void state(PeerState state) {
        fire(() -> listener.onStateChange(state));
    }

    public void iceState(IceState state) {
        fire(() -> listener.onIceStateChange(state));
    }

    /** A link closed for a reason other than the local close. */
    public void linkClosed(FakeDataLink link, CloseCause cause) {
        if (link.closeByEngine()) {
            fire(() -> listener.onChannelClosed(link, cause));
        }
    }

    /** The peer ended on its own. Fires at most once, and never after a local close. */
    public void closed(CloseCause cause) {
        if (closeCalled || ended) {
            return;
        }
        ended = true;
        fire(() -> listener.onClosed(cause));
    }

    /**
     * Completes a close stage held by {@link FakeEngine#holdPeerClose}. No
     * callback starts after this, and the stage completes once a callback
     * still running returned, without blocking the caller.
     */
    public void finishClose() {
        boolean complete;
        synchronized (state) {
            terminating = true;
            complete = running == 0;
        }
        if (complete) {
            closeStage.complete(null);
        }
    }

    void queueLow(FakeDataLink link) {
        fire(() -> listener.onQueueLow(link));
    }

    boolean autoAcceptSends() {
        return engine.autoAcceptSends();
    }

    private void fire(Runnable callback) {
        synchronized (serial) {
            synchronized (state) {
                if (terminating) {
                    return;
                }
                running++;
            }
            runCounted(callback);
        }
    }

    /**
     * Runs a send's settlement, which may run its completion. Unlike callbacks
     * it is not serialized with them and still runs while the peer
     * terminates, but the close stage waits for one in progress, as the
     * contract promises pending sends settled at termination.
     */
    void settling(Runnable settlement) {
        synchronized (state) {
            running++;
        }
        runCounted(settlement);
    }

    private void runCounted(Runnable task) {
        try {
            task.run();
        } finally {
            boolean complete;
            synchronized (state) {
                running--;
                complete = terminating && running == 0;
            }
            if (complete) {
                closeStage.complete(null);
            }
        }
    }

    private void requireRole(PeerConfig.Role role) {
        if (config.role() != role) {
            throw new IllegalStateException("Only an " + role + " may do this");
        }
    }
}

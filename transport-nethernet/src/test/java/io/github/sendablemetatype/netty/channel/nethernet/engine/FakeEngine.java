package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pure Java engine for transport tests. Nothing happens on its own: the test
 * completes SDP stages, fires peer events, settles sends and moves queued
 * bytes. It keeps the interface's contract: a peer's callbacks are serialized,
 * local candidates wait for the local description, a link's messages follow
 * its announcement, and nothing fires once a peer's close stage completed.
 *
 * <p>Callbacks run on the thread that triggers them, so a test that triggers
 * them from an event loop sees them on that loop; real engines never call from
 * an event loop, and channels must not rely on either.
 */
public final class FakeEngine implements NetherNetEngine {
    private final List<FakePeer> peers = new CopyOnWriteArrayList<>();
    private final List<FakeIceEndpoint> endpoints = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final AtomicInteger closeCalls = new AtomicInteger();
    private volatile boolean closing;
    private volatile RuntimeException createFailure;
    private volatile LinkageError createLinkageFailure;
    private volatile String scriptedAnswer;
    private volatile String scriptedOffer;
    private volatile String scriptedGathering;
    private volatile boolean autoAcceptSends;
    private volatile boolean holdPeerClose;

    @Override
    public Peer createPeer(PeerConfig config, PeerListener listener) {
        if (closing) {
            throw new IllegalStateException("The engine is closed");
        }
        LinkageError linkage = createLinkageFailure;
        if (linkage != null) {
            throw linkage;
        }
        RuntimeException failure = createFailure;
        if (failure != null) {
            throw failure;
        }
        FakePeer peer = new FakePeer(this, config, listener);
        peers.add(peer);
        return peer;
    }

    @Override
    public CompletionStage<IceEndpoint> openEndpoint(EndpointConfig config) {
        if (closing) {
            return CompletableFuture.failedFuture(new IllegalStateException("The engine is closed"));
        }
        FakeIceEndpoint endpoint = new FakeIceEndpoint(config.port());
        endpoints.add(endpoint);
        return CompletableFuture.completedFuture(endpoint);
    }

    @Override
    public CompletionStage<Void> close() {
        closeCalls.incrementAndGet();
        if (!closing) {
            closing = true;
            CompletableFuture<?>[] stages = peers.stream()
                    .map(peer -> peer.close().toCompletableFuture())
                    .toArray(CompletableFuture[]::new);
            endpoints.forEach(FakeIceEndpoint::close);
            CompletableFuture.allOf(stages).whenComplete((ignored, failure) -> closed.complete(null));
        }
        return closed.minimalCompletionStage();
    }

    /** Every peer created so far, in creation order. */
    public List<FakePeer> peers() {
        return List.copyOf(peers);
    }

    public FakePeer peer(int index) {
        return peers.get(index);
    }

    public FakePeer lastPeer() {
        return peers.getLast();
    }

    /** How often {@link #close()} was called. */
    public int closeCalls() {
        return closeCalls.get();
    }

    public boolean isClosed() {
        return closing;
    }

    /** Makes {@link #createPeer} throw this, or null to create peers again. */
    public void failCreation(RuntimeException failure) {
        this.createFailure = failure;
    }

    /** Makes {@link #createPeer} throw this, as a missing native library would. */
    public void failCreation(LinkageError failure) {
        this.createLinkageFailure = failure;
    }

    /** Answers every later {@link Peer#answer} at once with this description, or null to leave it to the test. */
    public void answerWith(String sdp) {
        this.scriptedAnswer = sdp;
    }

    /** Completes every later {@link Peer#offer} at once with this description, or null to leave it to the test. */
    public void offerWith(String sdp) {
        this.scriptedOffer = sdp;
    }

    /**
     * Ends gathering of every later peer with this full description, on
     * another thread, as soon as the transport asks for it; or null to leave
     * it to the test.
     */
    public void gatherWith(String fullDescription) {
        this.scriptedGathering = fullDescription;
    }

    /** Accepts every later send at once instead of leaving it to the test. */
    public void autoAcceptSends(boolean accept) {
        this.autoAcceptSends = accept;
    }

    /** Leaves later peer close stages pending until {@link FakePeer#finishClose()}. */
    public void holdPeerClose(boolean hold) {
        this.holdPeerClose = hold;
    }

    String scriptedAnswer() {
        return scriptedAnswer;
    }

    String scriptedOffer() {
        return scriptedOffer;
    }

    String scriptedGathering() {
        return scriptedGathering;
    }

    boolean autoAcceptSends() {
        return autoAcceptSends;
    }

    boolean holdPeerClose() {
        return holdPeerClose;
    }
}

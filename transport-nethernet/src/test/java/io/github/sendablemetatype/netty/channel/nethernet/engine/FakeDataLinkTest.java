package io.github.sendablemetatype.netty.channel.nethernet.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** The fake's send completions keep the engine contract the transport relies on. */
class FakeDataLinkTest {
    private static final PeerConfig OFFERER = new PeerConfig(PeerConfig.Role.OFFERER, List.of(),
            CandidatePolicy.DEFAULT, null, null, null);

    @Test
    @Timeout(10)
    void theCloseStageWaitsForASendCompletionStillRunning() throws Exception {
        FakePeer peer = (FakePeer) new FakeEngine().createPeer(OFFERER, new SilentListener());
        FakeDataLink link = (FakeDataLink) peer.openChannel(ChannelParameters.reliable("ReliableDataChannel"));
        peer.open(link);
        CompletableFuture<Void> running = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        link.send(ByteBuffer.allocate(1), cause -> {
            running.complete(null);
            release.join();
        });
        Thread engine = new Thread(() -> link.accept(0), "test-engine-completion");
        engine.start();
        try {
            running.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> closing = peer.close().toCompletableFuture();
            assertFalse(closing.isDone(), "the close stage completed while a send completion ran");
            release.complete(null);
            closing.get(5, TimeUnit.SECONDS);
        } finally {
            release.complete(null);
            engine.join();
        }
    }

    private static final class SilentListener implements PeerListener {
        @Override
        public void onLocalCandidate(String candidate, String mid) {
        }

        @Override
        public void onGatheringComplete() {
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            return false;
        }

        @Override
        public void onChannelOpen(DataLink link) {
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
        }

        @Override
        public void onTextMessage(DataLink link, ByteBuffer message) {
        }

        @Override
        public void onQueueLow(DataLink link) {
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
        }

        @Override
        public void onPathSelected(TransportPath path) {
        }

        @Override
        public void onStateChange(PeerState state) {
        }

        @Override
        public void onIceStateChange(IceState state) {
        }

        @Override
        public void onClosed(CloseCause cause) {
        }
    }
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The statistics a server's child reports: metrics, the selected pair and its addresses. */
class NetherNetChannelStatisticsTest {
    private static final InetSocketAddress CLIENT = new InetSocketAddress("192.0.2.8", 32000);
    private static final InetSocketAddress RELAY = new InetSocketAddress("198.51.100.4", 3478);
    private static final TransportMetrics METRICS = new TransportMetrics(Optional.of(Duration.ofMillis(12)),
            Optional.of(new TransportMetrics.Sctp(Duration.ofMillis(40), 4380, 0, 131072, 10, 4, 0, 0, 9, 3)));

    @Test
    void metricsComeFromTheEngineAndAreNoneOnceThePeerIsGone() {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.setMetrics(METRICS);
            assertSame(METRICS, h.channel.transportMetrics().toCompletableFuture().join());

            h.channel.close();
            h.run();
            assertSame(TransportMetrics.NONE, h.channel.transportMetrics().toCompletableFuture().join());
        }
    }

    @Test
    void aPairSelectedBeforeActivationIsTheMediaAddressAndBecomesTheReportedOneAtActivation() {
        EmbeddedChannel clock = new EmbeddedChannel();
        ChildFixture fixture = new ChildFixture(null);
        NetherNetChildChannel child = fixture.child;
        try {
            clock.eventLoop().register(child).syncUninterruptibly();
            SourcedAddress signaled = new SourcedAddress(ChildFixture.REMOTE, AddressSource.SIGNALING, null);
            assertEquals(signaled, child.signaledAddress());
            assertEquals(Optional.empty(), child.selectedPath());
            assertEquals(Optional.empty(), child.mediaAddress());

            TransportPath path = new TransportPath(ChildFixture.LOCAL, CandidateType.HOST, CLIENT,
                    CandidateType.SERVER_REFLEXIVE);
            fixture.peer.path(path);
            assertEquals(Optional.empty(), child.selectedPath(), "taken in on the event loop");
            clock.runPendingTasks();
            SourcedAddress media = new SourcedAddress(CLIENT, AddressSource.ICE, null);
            assertEquals(Optional.of(path), child.selectedPath());
            assertEquals(Optional.of(media), child.mediaAddress());
            assertEquals(signaled, child.reportedAddress(), "the signaled address until activation");
            assertEquals(ChildFixture.REMOTE, child.remoteAddress());

            fixture.openLinks();
            clock.runPendingTasks();
            assertTrue(child.isActive());
            assertEquals(media, child.reportedAddress());
            assertEquals(CLIENT, child.remoteAddress());

            child.close();
            clock.runPendingTasks();
            assertEquals(Optional.of(path), child.selectedPath(), "kept as a record after close");
            assertEquals(media, child.reportedAddress());
            assertEquals(signaled, child.signaledAddress());
        } finally {
            child.close();
            clock.finishAndReleaseAll();
        }
    }

    @Test
    void aRelayedPairReportsTheRelayAsTurn() {
        try (ActiveChild h = new ActiveChild()) {
            h.peer.path(new TransportPath(null, CandidateType.HOST, RELAY, CandidateType.RELAY));
            h.run();
            SourcedAddress relay = new SourcedAddress(RELAY, AddressSource.TURN, null);
            assertEquals(Optional.of(relay), h.channel.mediaAddress());
            assertEquals(relay, h.channel.reportedAddress());
            assertEquals(CandidateType.RELAY, h.channel.selectedPath().orElseThrow().remoteType());
        }
    }

    @Test
    void aPairWithoutAKnownRemoteIsSelectedButLocatesNothing() {
        try (ActiveChild h = new ActiveChild()) {
            TransportPath path = new TransportPath(null, CandidateType.HOST, null, CandidateType.UNKNOWN);
            h.peer.path(path);
            h.run();
            assertEquals(Optional.of(path), h.channel.selectedPath());
            assertEquals(Optional.empty(), h.channel.mediaAddress());
            assertEquals(AddressSource.SIGNALING, h.channel.reportedAddress().source());
        }
    }
}

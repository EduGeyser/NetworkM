package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeIceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SharedIcePort;
import io.github.sendablemetatype.netty.util.nethernet.IdentityKeyVerifier;
import io.github.sendablemetatype.netty.util.nethernet.OfferPolicy;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.OFFER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Joins a source authenticated itself (NXS), and the shared port's held checks such a source claims. */
@Timeout(15)
class NetherNetServerAuthenticatedJoinTest {
    private static final IceCredentials CREDENTIALS = new IceCredentials("NXS1K001token", "derivedpassword0123456789");
    private static final DtlsIdentity IDENTITY = new DtlsIdentity("certificate", "key");
    private static final SourcedAddress CHECK_SOURCE = new SourcedAddress(FakeJoinSource.CLIENT,
            AddressSource.NXS_ADMISSION, null);

    private final EndpointHarness harness = new EndpointHarness();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    private static TransportIdentityBinding binding() {
        return TransportIdentityBinding.of(new IdentityKeyVerifier() {
            @Override
            protected boolean matches(byte[] canonicalKey) {
                return false;
            }

            @Override
            protected void release() {
            }
        });
    }

    @Test
    void anAuthenticatedJoinSkipsTheOfferCheckAndUsesWhatItsSourceBrings() throws Exception {
        harness.bind();
        // This policy would refuse the unsigned offer of any signaled join.
        FakeJoinSource source = harness.source(false, OfferPolicy.REQUIRE_KEY_PROVEN);
        TransportIdentityBinding binding = binding();
        AtomicReference<NetherNetChildChannel> created = new AtomicReference<>();
        AtomicReference<Boolean> exposedYet = new AtomicReference<>();

        source.authenticated("1", OFFER, CHECK_SOURCE, new Join.Authenticated(CREDENTIALS, IDENTITY, binding, child -> {
            created.set(child);
            exposedYet.set(!harness.children.isEmpty());
        }));
        FakePeer peer = harness.negotiating();
        NetherNetChildChannel child = harness.child();

        assertEquals(CREDENTIALS, peer.config().localCredentials());
        assertEquals(IDENTITY, peer.config().identity());
        assertSame(child, created.get());
        assertFalse(exposedYet.get(), "the source sees the child before anyone else");
        assertSame(binding, child.attr(TransportIdentityBinding.ATTRIBUTE).get(), "its binding decides the login");
        assertNull(child.getOfferCheck());
        assertEquals(FakeJoinSource.CLIENT, child.remoteAddress());
    }

    @Test
    void authenticatedJoinsShareTheBudgetOfEverySource() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_MAX_SESSIONS, 1);
        harness.bind();
        FakeJoinSource http = harness.source(false);
        FakeJoinSource nxs = harness.source(false);
        http.offer("1", OFFER);
        harness.negotiating();

        AtomicReference<NetherNetChildChannel> created = new AtomicReference<>();
        nxs.authenticated("2", OFFER, CHECK_SOURCE, new Join.Authenticated(CREDENTIALS, IDENTITY, binding(),
                created::set)).assertRefused(JoinRefusal.CAPACITY);
        assertNull(created.get(), "a refused join has no child");
        assertEquals(1, harness.engine.peers().size());
    }

    @Test
    void heldChecksReachTheClaimOnlyWhileKeysAreAdmitted() throws Exception {
        harness.bind();
        FakeJoinSource nxs = harness.source(false);
        FakeIceEndpoint endpoint = harness.engine.endpoints().getFirst();
        SharedIcePort port = nxs.sink.sharedIcePort().orElseThrow();
        assertEquals(endpoint.port(), port.localAddress().getPort());
        LinkedBlockingQueue<IceEndpoint.HeldCheck> held = new LinkedBlockingQueue<>();

        SharedIcePort.HeldChecks claim = port.claimHeldChecks(held::add);
        assertFalse(endpoint.holdsChecks(), "without keys the engine would pass on every check unverified");

        claim.admit(new byte[]{1}, Map.of("NXS1K001", new byte[]{2}));
        assertTrue(endpoint.holdsChecks());
        assertEquals(Set.of("NXS1K001"), endpoint.derivedKeys().keySet());
        Check check = new Check();
        assertTrue(endpoint.hold(check));
        assertSame(check, held.poll(2, TimeUnit.SECONDS));

        claim.admit(new byte[]{1}, Map.of());
        assertFalse(endpoint.holdsChecks());
        assertEquals(Map.of(), endpoint.derivedKeys());
    }

    @Test
    void oneSourceHoldsTheHeldChecksUntilItGoes() throws Exception {
        harness.bind();
        FakeJoinSource first = harness.source(false);
        FakeJoinSource second = harness.source(false);
        FakeIceEndpoint endpoint = harness.engine.endpoints().getFirst();
        SharedIcePort.HeldChecks claim = first.sink.sharedIcePort().orElseThrow().claimHeldChecks(check -> { });
        claim.admit(new byte[]{1}, Map.of("NXS1K001", new byte[]{2}));

        SharedIcePort other = second.sink.sharedIcePort().orElseThrow();
        assertThrows(IllegalStateException.class, () -> other.claimHeldChecks(check -> { }));

        harness.server.removeSource(first);
        assertFalse(endpoint.holdsChecks(), "a removed source's claim ends with it");
        assertEquals(Map.of(), endpoint.derivedKeys());
        other.claimHeldChecks(check -> { }).close();
    }

    @Test
    void gatheringOnTheSharedPortNamesItsHostAddressesAndLeavesNoPeer() throws Exception {
        harness.bind();
        FakeIceEndpoint endpoint = harness.engine.endpoints().getFirst();
        int port = endpoint.port();
        harness.engine.offerWith(EndpointHarness.ANSWER);
        harness.engine.gatherWith(EndpointHarness.ANSWER
                + "a=candidate:1 1 udp 2122260223 192.168.1.5 " + port + " typ host\r\n"
                + "a=candidate:2 1 udp 2122260223 192.168.1.5 " + port + " typ host generation 0\r\n"
                + "a=candidate:3 1 udp 2122194687 10.8.0.2 " + port + " typ host\r\n"
                + "a=candidate:4 1 udp 2122260223 192.168.1.5 51000 typ host\r\n"
                + "a=candidate:5 1 udp 1686052607 203.0.113.9 " + port + " typ srflx raddr 192.168.1.5 rport " + port
                + "\r\na=end-of-candidates\r\n");
        SharedIcePort shared = harness.source(false).sink.sharedIcePort().orElseThrow();

        var addresses = shared.gatherHostAddresses().toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(List.of(new InetSocketAddress("192.168.1.5", port), new InetSocketAddress("10.8.0.2", port)),
                addresses, "the port's own host candidates, once each");
        FakePeer probe = harness.engine.lastPeer();
        assertEquals(endpoint, probe.config().endpoint(), "it gathers on the shared port, which binds it there");
        assertTrue(probe.terminated(), "and leaves no peer behind");
    }

    @Test
    void portsOfTheirOwnHaveNoSharedPort() throws Exception {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT, false);
        harness.bind();
        assertTrue(harness.source(false).sink.sharedIcePort().isEmpty());
    }

    /** A check the endpoint holds, as the fake hands it on. */
    private static final class Check implements IceEndpoint.HeldCheck {
        final CompletableFuture<Void> rejected = new CompletableFuture<>();

        @Override
        public InetSocketAddress localAddress() {
            return new InetSocketAddress("127.0.0.1", 40000);
        }

        @Override
        public InetSocketAddress remoteAddress() {
            return FakeJoinSource.CLIENT;
        }

        @Override
        public String localUfrag() {
            return CREDENTIALS.ufrag();
        }

        @Override
        public String remoteUfrag() {
            return "client";
        }

        @Override
        public void reject() {
            rejected.complete(null);
        }
    }
}

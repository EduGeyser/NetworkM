package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class DiscoveryPeerRegistryTest {
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("127.0.0.1", 7551);

    @Test
    void defaultPassiveCapacityIsBounded() {
        DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry();
        for (long id = 0; id < 1100; id++) {
            peers.remember(id, ADDRESS);
        }
        assertEquals(1024, peers.passiveSize());
        assertNull(peers.get(0));
        assertEquals(ADDRESS, peers.get(1099));
    }

    @Test
    void passiveCapacityEvictsTheLeastRecentlyUsedPeer() {
        DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry(2, 10, () -> 0);
        peers.remember(1, ADDRESS);
        peers.remember(2, ADDRESS);
        assertEquals(ADDRESS, peers.get(1));
        peers.remember(3, ADDRESS);

        assertNull(peers.get(2));
        assertEquals(ADDRESS, peers.get(1));
        assertEquals(ADDRESS, peers.get(3));
        assertEquals(2, peers.passiveSize());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, Long.MAX_VALUE - 5})
    void idleExpiryFollowsUsageAndHandlesClockWrap(long initialTime) {
        AtomicLong now = new AtomicLong(initialTime);
        DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry(2, 10, now::get);
        peers.remember(1, ADDRESS);
        now.addAndGet(9);
        assertEquals(ADDRESS, peers.get(1));
        now.addAndGet(9);
        assertEquals(1, peers.passiveSize());
        now.incrementAndGet();

        assertNull(peers.get(1));
        assertEquals(0, peers.passiveSize());
    }

    @Test
    void registeredConnectionsPinQuietPeersUntilEveryHandlerIsRemoved() {
        AtomicLong now = new AtomicLong();
        DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry(2, 10, now::get);
        peers.register(41);
        peers.rememberSignal(41, 1, ADDRESS);
        peers.register(41);
        peers.register(42);
        peers.rememberSignal(42, 1, ADDRESS);
        for (int i = 2; i <= 10; i++) {
            peers.remember(i, ADDRESS);
        }
        now.set(100);
        assertEquals(0, peers.passiveSize());
        assertEquals(ADDRESS, peers.get(1));
        peers.unregister(41);
        now.set(200);
        assertEquals(ADDRESS, peers.get(1));
        now.set(300);
        peers.unregister(42);

        assertNull(peers.get(1));
        assertEquals(0, peers.passiveSize());
    }

    @Test
    void heldPeersStayUntilEveryHoldIsReleased() {
        AtomicLong now = new AtomicLong();
        DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry(1, 10, now::get);
        peers.release(1);
        peers.hold(1, ADDRESS);
        peers.hold(1, ADDRESS);
        peers.remember(2, ADDRESS);
        peers.remember(3, ADDRESS);
        now.set(100);
        assertEquals(ADDRESS, peers.get(1), "a held peer is neither evicted nor expired");
        peers.release(1);
        now.set(200);
        assertEquals(ADDRESS, peers.get(1));
        now.set(300);
        peers.release(1);

        assertNull(peers.get(1));
        assertEquals(0, peers.passiveSize());
    }

    @Test
    void clearReleasesPins() {
        DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry(2, 10, () -> 0);
        peers.register(41);
        peers.rememberSignal(41, 1, ADDRESS);
        peers.hold(2, ADDRESS);
        peers.clear();

        peers.unregister(41);
        peers.release(2);
        assertNull(peers.get(1));
        assertNull(peers.get(2));
        peers.remember(1, ADDRESS);
        assertEquals(1, peers.passiveSize());
    }
}

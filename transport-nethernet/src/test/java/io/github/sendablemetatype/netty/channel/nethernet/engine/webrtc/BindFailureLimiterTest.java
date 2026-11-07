package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BindFailureLimiterTest {
    private static final Duration QUIET = Duration.ofMinutes(10);
    private static final long QUIET_NANOS = QUIET.toNanos();
    private static final int PORT = 19132;

    private final BindFailureLimiter limiter = new BindFailureLimiter(QUIET);

    // System.nanoTime() may start anywhere, and the quiet time may run past Long.MAX_VALUE
    @ParameterizedTest
    @ValueSource(longs = {0, -1_000_000_000_000L, Long.MAX_VALUE - 60_000_000_000L})
    void anAddressWarnsAgainOnceTheQuietTimeSinceItsLastWarningHasPassed(long start) {
        InetSocketAddress address = ipv4(10);

        assertTrue(limiter.warn(address, start));
        assertFalse(limiter.warn(address, start + QUIET_NANOS / 2));
        assertFalse(limiter.warn(address, start + QUIET_NANOS - 1));
        // Failures within the quiet time do not extend it
        assertTrue(limiter.warn(address, start + QUIET_NANOS));
        assertFalse(limiter.warn(address, start + 2 * QUIET_NANOS - 1));
        assertTrue(limiter.warn(address, start + 2 * QUIET_NANOS));
    }

    @Test
    void addressesWarnOnTheirOwn() {
        assertTrue(limiter.warn(ipv4(10), 0));
        assertTrue(limiter.warn(ipv4(11), 1));
        assertFalse(limiter.warn(ipv4(10), 2));
        assertFalse(limiter.warn(ipv4(11), 3));
    }

    @Test
    void linkLocalAddressesThatDifferOnlyInScopeWarnOnTheirOwn() throws Exception {
        byte[] linkLocal = InetAddress.ofLiteral("fe80::1").getAddress();
        InetSocketAddress onLink3 = new InetSocketAddress(Inet6Address.getByAddress(null, linkLocal, 3), PORT);
        InetSocketAddress onLink7 = new InetSocketAddress(Inet6Address.getByAddress(null, linkLocal, 7), PORT);
        assertEquals(onLink3, onLink7, "Java's address equality leaves the scope out");

        assertTrue(limiter.warn(onLink3, 0));
        assertTrue(limiter.warn(onLink7, 1));
        assertFalse(limiter.warn(onLink3, 2));
        assertFalse(limiter.warn(onLink7, 3));
    }

    @Test
    void onlyAddressesInTheirQuietTimeAreKept() {
        for (int i = 0; i < 100; i++) {
            limiter.warn(ipv4(i), i);
        }
        assertEquals(100, limiter.quietAddresses());

        // The quiet time of the addresses that warned at 0 to 50 is over
        assertTrue(limiter.warn(ipv4(200), 50 + QUIET_NANOS));

        assertEquals(50, limiter.quietAddresses());
    }

    private static InetSocketAddress ipv4(int host) {
        return new InetSocketAddress(InetAddress.ofLiteral("192.0.2." + host), PORT);
    }
}

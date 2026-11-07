package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Decides which failed binds of the shared ICE port log a warning: the first
 * on an address, then one each time the quiet time since the address's last
 * warning has passed. The others log at debug. Not thread safe.
 */
final class BindFailureLimiter {
    private final long quietNanos;
    // When each address last warned. Only addresses in their quiet time stay.
    private final Map<Key, Long> warnedAt = new HashMap<>();

    BindFailureLimiter(Duration quiet) {
        this.quietNanos = quiet.toNanos();
    }

    /**
     * @param address the address that failed to bind, with its scope
     * @param nowNanos the current {@link System#nanoTime()}
     * @return whether this failure logs a warning
     */
    boolean warn(InetSocketAddress address, long nowNanos) {
        warnedAt.values().removeIf(warned -> nowNanos - warned >= quietNanos);
        int scope = address.getAddress() instanceof Inet6Address ipv6 ? ipv6.getScopeId() : 0;
        return warnedAt.putIfAbsent(new Key(address, scope), nowNanos) == null;
    }

    /** Address equality ignores an IPv6 scope, which tells the link-local addresses of two links apart. */
    private record Key(InetSocketAddress address, int scope) {
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.address;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Placeholders for a peer whose address is not known yet, such as an Xbox join before ICE
 * connects.
 * <p>
 * Each placeholder is unique: downstream plugins and proxies key state by address and cannot
 * tell a shared fallback from a real shared IP, so one value for every unknown peer would make
 * them one player. The values come from 240.0.0.0/4, reserved for future use (RFC 1112), which no
 * host holds and which does not look like a LAN address to anything that classifies addresses,
 * as 10.0.0.0/8 did. One process-wide counter walks the block from a random start, so a restart
 * does not hand out the same values again, skips 255.255.255.255 (broadcast) and wraps after
 * about 268 million placeholders. Port 0.
 */
public final class PlaceholderAddress {
    private static final int BLOCK = 0xF0000000;
    private static final int HOSTS = 0x0FFFFFFF;
    private static final AtomicInteger NEXT = new AtomicInteger(ThreadLocalRandom.current().nextInt());

    private PlaceholderAddress() {
    }

    /**
     * @return A placeholder no other call in this process returned among the last 268 million
     */
    public static InetSocketAddress next() {
        return next(NEXT);
    }

    static InetSocketAddress next(AtomicInteger counter) {
        int host;
        do {
            // 2^32 is a multiple of the block size, so overflow wraps within the block
            host = counter.getAndIncrement() & HOSTS;
        } while (host == HOSTS);
        int value = BLOCK | host;
        byte[] raw = {(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
        try {
            return new InetSocketAddress(InetAddress.getByAddress(raw), 0);
        } catch (UnknownHostException impossible) {
            throw new AssertionError("four bytes are always an address", impossible);
        }
    }
}

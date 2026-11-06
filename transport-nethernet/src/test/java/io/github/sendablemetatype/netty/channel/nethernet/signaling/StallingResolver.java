package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test JVM's name resolver, registered in {@code META-INF/services}: resolves like the JDK,
 * except for the names {@link #stall()} hands out, whose lookups wait until their stall is closed
 * and then fail. That lets a test hold a request in its name lookup.
 */
public final class StallingResolver extends InetAddressResolverProvider {
    private static final AtomicInteger names = new AtomicInteger();
    private static final Map<String, Stall> stalls = new ConcurrentHashMap<>();

    /** A new name, since the JDK caches answers, whose lookups wait until the stall is closed. */
    static Stall stall() {
        Stall stall = new Stall("stalled-" + names.incrementAndGet() + ".nethernet.test", new Semaphore(0),
                new CountDownLatch(1));
        stalls.put(stall.name(), stall);
        return stall;
    }

    record Stall(String name, Semaphore started, CountDownLatch released) implements AutoCloseable {
        void awaitLookup() throws InterruptedException {
            assertTrue(started.tryAcquire(5, TimeUnit.SECONDS), "no lookup started");
        }

        @Override
        public void close() {
            released.countDown();
        }
    }

    @Override
    public InetAddressResolver get(Configuration configuration) {
        InetAddressResolver builtin = configuration.builtinResolver();
        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
                Stall stall = stalls.get(host);
                if (stall == null) {
                    return builtin.lookupByName(host, policy);
                }
                stall.started().release();
                try {
                    stall.released().await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new UnknownHostException(host);
            }

            @Override
            public String lookupByAddress(byte[] address) throws UnknownHostException {
                return builtin.lookupByAddress(address);
            }
        };
    }

    @Override
    public String name() {
        return "stalling test resolver";
    }
}

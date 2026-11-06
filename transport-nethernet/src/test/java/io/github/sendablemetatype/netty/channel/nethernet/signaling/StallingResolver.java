package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test JVM's name resolver, registered in {@code META-INF/services}: resolves like the JDK,
 * except that a lookup of {@link #NAME} waits while a {@link Stall} is open and then fails, so a
 * test can hold a request in its name lookup.
 */
public final class StallingResolver extends InetAddressResolverProvider {
    static final String NAME = "stalled.nethernet.test";
    private static volatile Stall current;

    /** Holds lookups of {@link #NAME} until the returned stall is closed. */
    static Stall stall() {
        Stall stall = new Stall(new Semaphore(0), new CountDownLatch(1));
        current = stall;
        return stall;
    }

    record Stall(Semaphore started, CountDownLatch released) implements AutoCloseable {
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
                if (!NAME.equals(host)) {
                    return builtin.lookupByName(host, policy);
                }
                Stall stall = current;
                if (stall != null) {
                    stall.started().release();
                    try {
                        stall.released().await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
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

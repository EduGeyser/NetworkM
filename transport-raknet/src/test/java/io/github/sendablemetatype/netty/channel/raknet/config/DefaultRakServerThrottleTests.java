package io.github.sendablemetatype.netty.channel.raknet.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.*;

class DefaultRakServerThrottleTests {

    @ParameterizedTest
    @CsvSource({"1, 1000", "1000, 1"})
    @Timeout(20)
    void concurrentAcceptsRespectBothLimits(int connectionsMax, int connectsMax) throws Exception {
        int attempts = 256;
        int workers = 8;
        DefaultRakServerThrottle throttle = new DefaultRakServerThrottle(connectionsMax, 60_000, connectsMax);
        InetSocketAddress[] addresses = new InetSocketAddress[attempts];
        for (int i = 0; i < attempts; i++) {
            addresses[i] = new InetSocketAddress(InetAddress.getByAddress(new byte[]{(byte) 192, 0, 2, (byte) i}), 19132);
        }
        AtomicIntegerArray accepted = new AtomicIntegerArray(attempts);
        CyclicBarrier start = new CyclicBarrier(workers);
        var executor = Executors.newFixedThreadPool(workers);
        try {
            List<Future<?>> tasks = new ArrayList<>();
            for (int worker = 0; worker < workers; worker++) {
                tasks.add(executor.submit(() -> {
                    for (int i = 0; i < attempts; i++) {
                        start.await(5, TimeUnit.SECONDS);
                        if (throttle.accept(addresses[i])) {
                            accepted.incrementAndGet(i);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            for (int i = 0; i < attempts; i++) {
                assertEquals(1, accepted.get(i), "Accepted connections for " + addresses[i]);
                throttle.closed(addresses[i]);
            }
            assertTrue(connections(throttle).isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectedReconnectDoesNotRetainAnInactiveAddress() throws Exception {
        DefaultRakServerThrottle throttle = new DefaultRakServerThrottle(1, 60_000, 1);
        InetSocketAddress address = new InetSocketAddress("192.0.2.1", 19132);

        assertTrue(throttle.accept(address));
        throttle.closed(address);
        assertFalse(throttle.accept(address));

        assertTrue(connections(throttle).isEmpty(), "Denied reconnects must not create permanent zero-count entries");
    }

    private static Map<?, ?> connections(DefaultRakServerThrottle throttle) throws Exception {
        Field field = DefaultRakServerThrottle.class.getDeclaredField("connections");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(throttle);
    }
}

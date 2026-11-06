package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint.StunReading;
import org.junit.jupiter.api.Test;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The engine monitors behind {@link StunObserver#sharedPort}: numeric servers only, reads that complete later. */
class StunObserverSharedPortTest {
    private static final InetSocketAddress SERVER = new InetSocketAddress("192.0.2.7", 3478);
    private static final InetSocketAddress MAPPED = new InetSocketAddress("203.0.113.9", 40404);

    /** Opened engine monitors by server, and the stages the test completes. */
    private final Map<InetSocketAddress, CompletableFuture<IceEndpoint.StunMonitor>> opening = new ConcurrentHashMap<>();
    private final List<EngineMonitor> monitors = new CopyOnWriteArrayList<>();
    private final List<Runnable> lookups = new ArrayList<>();

    private final StunObserver.MonitorFactory factory = StunObserver.sharedPort(server -> {
        CompletableFuture<IceEndpoint.StunMonitor> stage = new CompletableFuture<>();
        opening.put(server, stage);
        return stage;
    }, lookups::add);

    private static final class EngineMonitor implements IceEndpoint.StunMonitor {
        final InetSocketAddress server;
        final List<CompletableFuture<StunReading>> reads = new CopyOnWriteArrayList<>();
        volatile boolean closed;

        EngineMonitor(InetSocketAddress server) {
            this.server = server;
        }

        @Override
        public CompletionStage<StunReading> read() {
            CompletableFuture<StunReading> read = new CompletableFuture<>();
            reads.add(read);
            return read;
        }

        @Override
        public void close() {
            closed = true;
        }

        void answer(InetSocketAddress mapped) {
            reads.getLast().complete(new StunReading(server, null, mapped, StunReading.State.SUCCEEDED, 1, 0, 1,
                    Optional.of(java.time.Duration.ofMillis(5))));
        }
    }

    private EngineMonitor open(InetSocketAddress server) {
        EngineMonitor monitor = new EngineMonitor(server);
        monitors.add(monitor);
        opening.get(server).complete(monitor);
        return monitor;
    }

    @Test
    void aReadReturnsWhatThePreviousReadFound() {
        StunObserver.Monitor monitor = factory.open(SERVER);
        EngineMonitor engine = open(SERVER);

        assertEquals(List.of(), monitor.read(), "the first read only asks the engine");
        engine.answer(MAPPED);
        List<StunReading> readings = monitor.read();
        assertEquals(1, readings.size());
        assertEquals(MAPPED, readings.getFirst().mapped());
        assertEquals(2, engine.reads.size(), "every read asks again");
    }

    @Test
    void aNameIsLookedUpOnTheResolverAndAskedOncePerFamily() {
        StunObserver.Monitor monitor = factory.open(InetSocketAddress.createUnresolved("localhost", 3478));
        assertTrue(opening.isEmpty(), "nothing is looked up on the caller's thread");
        assertEquals(1, lookups.size());

        lookups.getFirst().run();
        assertFalse(opening.isEmpty());
        assertTrue(opening.size() <= 2, "at most one address per family: " + opening.keySet());
        long v4 = opening.keySet().stream().filter(server -> server.getAddress() instanceof Inet4Address).count();
        assertTrue(v4 <= 1 && opening.size() - v4 <= 1, opening.keySet().toString());
        assertTrue(opening.keySet().stream().noneMatch(InetSocketAddress::isUnresolved));
        monitor.close();
    }

    @Test
    void aNameThatDoesNotResolveFailsTheMonitor() {
        StunObserver.Monitor monitor = factory.open(InetSocketAddress.createUnresolved("stun.invalid", 3478));
        lookups.getFirst().run();

        assertThrows(IllegalStateException.class, monitor::read);
    }

    @Test
    void anAddressTheEndpointCannotReachIsLeftOutUnlessNoneOpens() {
        StunObserver.Monitor monitor = factory.open(SERVER);
        opening.get(SERVER).completeExceptionally(new IllegalStateException("cannot send there"));

        assertThrows(IllegalStateException.class, monitor::read, "no monitor opened at all");
    }

    @Test
    void anEngineReadThatFailsFailsTheNextRead() {
        StunObserver.Monitor monitor = factory.open(SERVER);
        EngineMonitor engine = open(SERVER);
        monitor.read();
        engine.reads.getLast().completeExceptionally(new IllegalStateException("The endpoint is closed"));

        assertThrows(IllegalStateException.class, monitor::read);
    }

    @Test
    void closingClosesEveryEngineMonitorAlsoOneThatOpensLater() {
        StunObserver.Monitor monitor = factory.open(SERVER);
        monitor.close();
        EngineMonitor late = open(SERVER);

        assertTrue(late.closed, "a monitor that opens after close is closed at once");
    }
}

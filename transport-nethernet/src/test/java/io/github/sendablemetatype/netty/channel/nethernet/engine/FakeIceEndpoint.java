package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * An {@link IceEndpoint} of {@link FakeEngine} that binds nothing. It records
 * its settings and lets a test hand held checks to the handler.
 */
public final class FakeIceEndpoint implements IceEndpoint {
    private final int port;
    private volatile Consumer<HeldCheck> handler;
    private volatile Executor executor;
    private volatile List<String> relays = List.of();
    private volatile Map<String, byte[]> derivedKeys = Map.of();
    private volatile boolean closed;

    FakeIceEndpoint(int port) {
        this.port = port;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public void onHeldCheck(Consumer<HeldCheck> handler, Executor executor) {
        this.handler = handler;
        this.executor = executor;
    }

    @Override
    public void clearHeldCheckHandler() {
        this.handler = null;
        this.executor = null;
    }

    @Override
    public void heldCheckLimits(int maxPending, Duration timeout) {
        if (maxPending < 1) {
            throw new IllegalArgumentException("maxPending must be at least 1");
        }
    }

    @Override
    public void derivedPasswords(byte[] context, Map<String, byte[]> keys) {
        this.derivedKeys = Map.copyOf(keys);
    }

    @Override
    public void clearDerivedPasswords() {
        this.derivedKeys = Map.of();
    }

    @Override
    public void proxyRelays(List<String> relays) {
        this.relays = List.copyOf(relays);
    }

    @Override
    public Optional<InetSocketAddress> relayOf(InetSocketAddress client) {
        return Optional.empty();
    }

    @Override
    public StunMonitor monitorStun(InetSocketAddress server) {
        InetSocketAddress local = new InetSocketAddress(port);
        return new StunMonitor() {
            @Override
            public StunReading read() {
                return new StunReading(server, local, null, StunReading.State.PENDING, 0, 0, 0, Optional.empty());
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public Stats stats() {
        return new Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @Override
    public CompletionStage<Void> close() {
        closed = true;
        handler = null;
        return CompletableFuture.completedFuture(null);
    }

    public boolean closed() {
        return closed;
    }

    public List<String> relays() {
        return relays;
    }

    public Map<String, byte[]> derivedKeys() {
        return derivedKeys;
    }

    /** Hands a held check to the handler on its executor; false if none is set. */
    public boolean hold(HeldCheck check) {
        Consumer<HeldCheck> current = handler;
        Executor runner = executor;
        if (current == null || runner == null || closed) {
            return false;
        }
        runner.execute(() -> current.accept(check));
        return true;
    }
}

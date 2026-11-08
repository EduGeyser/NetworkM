package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * An {@link IceEndpoint} of {@link FakeEngine} that binds nothing. It records
 * its settings, lets a test hand held checks to the handler, and lets STUN
 * monitors and {@link #sockets()} report what a test scripts.
 */
public final class FakeIceEndpoint implements IceEndpoint {
    private final int port;
    private volatile Consumer<HeldCheck> handler;
    private volatile Executor executor;
    private volatile List<String> relays = List.of();
    private volatile Map<String, byte[]> derivedKeys = Map.of();
    private final Map<InetSocketAddress, InetSocketAddress> stunMappings = new ConcurrentHashMap<>();
    private final List<InetSocketAddress> stunMonitors = new CopyOnWriteArrayList<>();
    private final AtomicInteger stunReads = new AtomicInteger();
    private volatile CompletionStage<Sockets> socketsAnswer = CompletableFuture.completedFuture(Sockets.NONE);
    private final boolean holdClose;
    private final CompletableFuture<IceEndpoint> opened = new CompletableFuture<>();
    private final CompletableFuture<Void> closeStage = new CompletableFuture<>();
    private volatile boolean closed;

    FakeIceEndpoint(int port, boolean holdClose) {
        this.port = port;
        this.holdClose = holdClose;
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
    public CompletionStage<Optional<InetSocketAddress>> relayOf(InetSocketAddress client) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletionStage<StunMonitor> monitorStun(InetSocketAddress server) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("The endpoint is closed"));
        }
        InetSocketAddress local = new InetSocketAddress(port);
        stunMonitors.add(server);
        return CompletableFuture.completedFuture(new StunMonitor() {
            private volatile boolean stopped;

            @Override
            public CompletionStage<StunReading> read() {
                if (stopped || closed) {
                    return CompletableFuture.failedFuture(new IllegalStateException("The monitor is closed"));
                }
                stunReads.incrementAndGet();
                InetSocketAddress mapped = stunMappings.get(server);
                return CompletableFuture.completedFuture(mapped == null
                        ? new StunReading(server, local, null, StunReading.State.PENDING, 0, 0, 0, Optional.empty())
                        : new StunReading(server, local, mapped, StunReading.State.SUCCEEDED, 1, 0, 1,
                                Optional.of(Duration.ZERO)));
            }

            @Override
            public void close() {
                stopped = true;
            }
        });
    }

    @Override
    public CompletionStage<Stats> stats() {
        return CompletableFuture.completedFuture(new Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    @Override
    public CompletionStage<Sockets> sockets() {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("The endpoint is closed"));
        }
        return socketsAnswer;
    }

    @Override
    public CompletionStage<Void> close() {
        closed = true;
        handler = null;
        if (!holdClose) {
            closeStage.complete(null);
        }
        return closeStage.minimalCompletionStage();
    }

    /** Whether {@link #close()} was called. */
    public boolean closed() {
        return closed;
    }

    /** Whether the close stage completed. */
    public boolean terminated() {
        return closeStage.isDone();
    }

    /** Completes the open stage {@link FakeEngine#holdEndpointOpen} held. */
    public void finishOpen() {
        opened.complete(this);
    }

    /** Completes a close stage {@link FakeEngine#holdEndpointClose} held. */
    public void finishClose() {
        closeStage.complete(null);
    }

    CompletionStage<IceEndpoint> opened() {
        return opened.minimalCompletionStage();
    }

    public List<String> relays() {
        return relays;
    }

    public Map<String, byte[]> derivedKeys() {
        return derivedKeys;
    }

    /** Whether a held check handler is set. */
    public boolean holdsChecks() {
        return handler != null;
    }

    /** Makes monitors of this server report this mapping, answered just now. */
    public void mapStun(InetSocketAddress server, InetSocketAddress mapped) {
        stunMappings.put(server, mapped);
    }

    /** The servers monitors were opened for, in order. */
    public List<InetSocketAddress> stunMonitors() {
        return List.copyOf(stunMonitors);
    }

    /** Makes {@link #sockets()} answer with this stage while the endpoint is open; it answers with none at first. */
    public void answerSockets(CompletionStage<Sockets> answer) {
        this.socketsAnswer = answer;
    }

    /** How many monitor reads ran. */
    public int stunReads() {
        return stunReads.get();
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

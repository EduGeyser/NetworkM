package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A channel's engine and who owns it. A shared engine belongs to the caller
 * and is never closed here. An owned engine is created on first use and
 * closed when the channel releases it.
 */
final class EngineRef {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(EngineRef.class);

    private final NetherNetEngine shared;
    private final Supplier<? extends NetherNetEngine> supplier;
    private NetherNetEngine owned;
    private boolean released;

    private EngineRef(NetherNetEngine shared, Supplier<? extends NetherNetEngine> supplier) {
        this.shared = shared;
        this.supplier = supplier;
    }

    static EngineRef shared(NetherNetEngine engine) {
        return new EngineRef(Objects.requireNonNull(engine, "engine"), null);
    }

    static EngineRef owned(Supplier<? extends NetherNetEngine> supplier) {
        return new EngineRef(null, Objects.requireNonNull(supplier, "supplier"));
    }

    /**
     * The engine, created now if it is owned and was not created yet.
     *
     * @throws IllegalStateException after {@link #release()}
     */
    synchronized NetherNetEngine get() {
        if (released) {
            throw new IllegalStateException("The channel's engine was released");
        }
        if (shared != null) {
            return shared;
        }
        if (owned == null) {
            owned = Objects.requireNonNull(supplier.get(), "The engine supplier returned null");
        }
        return owned;
    }

    /** The engine if it exists, without creating it; null after {@link #release()}. */
    synchronized NetherNetEngine current() {
        return released ? null : shared != null ? shared : owned;
    }

    /** Closes an owned engine; leaves a shared one alone. Idempotent. */
    void release() {
        NetherNetEngine engine;
        synchronized (this) {
            released = true;
            engine = owned;
            owned = null;
        }
        if (engine != null) {
            engine.close().whenComplete((ignored, failure) -> {
                if (failure != null) {
                    log.warn("Closing the NetherNet engine failed", failure);
                }
            });
        }
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * The health of a source that listens on a local port and has nothing to reconnect: HTTP and LAN.
 * It is connected while it listens. Listeners run on the thread that changed the state.
 */
final class SourceHealth {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(SourceHealth.class);

    private final List<Consumer<SignalingHealth>> listeners = new CopyOnWriteArrayList<>();
    private volatile SignalingHealth health = new SignalingHealth(SignalingState.NEW, 0, null, null);

    SignalingHealth get() {
        return health;
    }

    void addListener(Consumer<SignalingHealth> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    void removeListener(Consumer<SignalingHealth> listener) {
        listeners.remove(listener);
    }

    /** Moves to a state; a closed source stays closed. */
    void set(SignalingState state, Throwable failure) {
        SignalingHealth next;
        synchronized (this) {
            if (health.state() == SignalingState.CLOSED || health.state() == state) {
                return;
            }
            next = new SignalingHealth(state, failure == null ? 0 : 1, failure, null);
            health = next;
        }
        for (Consumer<SignalingHealth> listener : listeners) {
            try {
                listener.accept(next);
            } catch (RuntimeException e) {
                log.warn("A signaling health listener failed", e);
            }
        }
    }
}

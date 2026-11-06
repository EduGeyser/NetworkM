package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.io.IOException;

/**
 * Holds the client's saved state. A change takes effect only once it is on disk; a failed save warns, stops the
 * client and fails the step that wanted it, since a host whose state cannot be saved must not keep publishing.
 */
final class StateKeeper {
    private final ProviderStateStore store;
    private final ProviderLog log;
    private final Runnable stop;
    private ProviderState state = ProviderState.EMPTY;

    StateKeeper(ProviderStateStore store, ProviderLog log, Runnable stop) {
        this.store = store;
        this.log = log;
        this.stop = stop;
    }

    ProviderState state() {
        return state;
    }

    void load() throws IOException {
        state = ProviderState.fromJson(store.read());
    }

    void save(ProviderState next) throws IOException {
        try {
            store.write(next.toJson());
        } catch (IOException failure) {
            log.failed(ProviderLog.Operation.STORAGE, ProviderFailures.describe(failure));
            stop.run();
            throw failure;
        }
        state = next;
    }

    void close() throws IOException {
        store.close();
    }
}

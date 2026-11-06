package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.OperatorAction;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The provider's link for the host's operator. It may carry a one-time credential, so it lives in memory only: the
 * completion may offer one, and a heartbeat that says one is available lets the client request a fresh one.
 */
final class OperatorActions {
    private OperatorAction action;
    private boolean available;

    /** @param action The link a completion offered, or null */
    void offered(OperatorAction action) {
        this.action = action;
    }

    void available(boolean available) {
        this.available = available;
    }

    CompletableFuture<Optional<OperatorAction>> current(ClientRuntime runtime, Exchanger exchanger) {
        if (action != null && action.expiresAt() > runtime.clock.millis()) {
            return CompletableFuture.completedFuture(Optional.of(action));
        }
        action = null;
        if (!runtime.started || !available) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return exchanger.signed("operator-action", "{}").thenApply(reply -> {
            action = WireCodec.operatorAction(reply.get("operatorAction")).orElse(null);
            return Optional.ofNullable(action);
        });
    }
}

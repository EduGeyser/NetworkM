package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface NetherNetClientSignaling extends NetherNetSignaling {
    /**
     * Connects to the signaling medium (Client mode).
     *
     * @param remoteAddress The address of the signaling server to connect to.
     * @return the STUN and TURN servers the signaling hands out, possibly none
     */
    CompletableFuture<List<IceServer>> connect(SocketAddress remoteAddress);

    /**
     * Sets a handler to be called when a signaling message is received for an unknown connection ID.
     *
     * @param handler The handler to process incoming signaling messages for unknown connection IDs.
     */
    void setNotFoundHandler(NotFoundHandler handler);

    /**
     * Functional interface for handling "Not Found" signals.
     */
    @FunctionalInterface
    interface NotFoundHandler {
        /**
         * Called when the signaling service indicates the target peer was not found.
         *
         * @param reason The reason or raw message payload regarding the failure.
         */
        void onNotFound(String reason);
    }
}

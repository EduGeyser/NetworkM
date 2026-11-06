package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;

import java.net.SocketAddress;
import java.time.Duration;
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
     * Whether candidates travel as signals of their own after the offer. Signaling that does not
     * trickle, such as HTTP, carries one offer and one answer: the client channel then waits for
     * gathering to finish and sends the offer with every candidate in it, as the server does with
     * its answer, and makes one attempt only, since the server refuses a second offer for the same
     * network ID while the first is pending.
     *
     * @return true unless the signaling carries only full descriptions
     */
    default boolean trickles() {
        return true;
    }

    /**
     * How long the signaling itself waits for the server to answer one of its requests, a status
     * request or the offer, before it reports a failure of its own. The client channel gives each
     * attempt this much on top of its handshake timeout, until the answer arrives, so that the
     * attempt does not end while the signaling can still deliver one.
     *
     * @return {@link Duration#ZERO} unless the signaling has such a wait
     */
    default Duration answerTimeout() {
        return Duration.ZERO;
    }

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

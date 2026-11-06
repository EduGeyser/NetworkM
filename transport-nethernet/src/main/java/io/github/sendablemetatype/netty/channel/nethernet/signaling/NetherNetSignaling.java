package io.github.sendablemetatype.netty.channel.nethernet.signaling;

public interface NetherNetSignaling extends AutoCloseable {

    /**
     * Sends a NetherNet signal to the remote peer.
     *
     * @param targetNetworkId The destination's opaque Network ID. LAN discovery requires a uint64 decimal string.
     * @param signal          The signal.
     */
    void sendSignal(String targetNetworkId, Signal signal);

    /**
     * Sets a handler for the signals of one connection ID from one remote peer. A signal with that
     * connection ID from any other sender does not reach the handler, so a third party that learned
     * the ID can neither answer nor fail the attempt (go-nethernet's dialer filters the same way).
     *
     * @param connectionId    The connection ID to listen for.
     * @param remoteNetworkId The Network ID the signals must come from: the one the offer was sent to.
     * @param handler         The handler to process incoming signaling messages.
     */
    void setSignalHandler(String connectionId, String remoteNetworkId, SignalHandler handler);

    /**
     * Removes the signaling handler for a specific connection ID.
     *
     * @param connectionId The connection ID whose handler should be removed.
     */
    void removeSignalHandler(String connectionId);

    /**
     * Returns this client's local Network ID as an opaque string.
     */
    String getLocalNetworkId();

    /**
     * Closes the signaling channel and releases any associated resources.
     */
    @Override
    void close();

    /**
     * Functional interface for handling incoming signals.
     */
    @FunctionalInterface
    interface SignalHandler {
        /**
         * Called when a signal is received for the registered connection ID.
         *
         * @param signal the parsed signal
         */
        void onSignal(Signal signal);

        /**
         * Called when client signaling gave up on the offer of the registered connection ID. By
         * default this arrives as a {@code CONNECTERROR} signal with the failure's error code, which
         * drops the reason.
         *
         * @param connectionId the registered connection ID
         * @param failure      why the signaling gave up
         */
        default void onFailure(String connectionId, SignalingFailure failure) {
            onSignal(new Signal(SignalType.CONNECT_ERROR, connectionId,
                    Integer.toString(failure.errorCode())));
        }
    }
}

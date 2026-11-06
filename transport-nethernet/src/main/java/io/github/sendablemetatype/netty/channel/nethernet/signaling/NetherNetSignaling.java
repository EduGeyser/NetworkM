package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;

public interface NetherNetSignaling extends AutoCloseable {

    /**
     * Sends a signaling message to the remote peer.
     *
     * @param targetNetworkId The destination's opaque Network ID. LAN discovery requires a uint64 decimal string.
     * @param data            The raw signaling payload.
     */
    void sendSignal(String targetNetworkId, String data);

    /**
     * Sets a handler to receive signaling messages for a specific connection ID.
     *
     * @param connectionId The connection ID to listen for.
     * @param handler      The handler to process incoming signaling messages.
     */
    void setSignalHandler(String connectionId, SignalHandler handler);

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
         * @param signal The raw signal payload.
         */
        void onSignal(String signal);

        /**
         * Called when client signaling gave up on the offer of the registered connection ID. By
         * default this arrives as a {@code CONNECTERROR} signal with the failure's error code, which
         * drops the reason.
         *
         * @param connectionId the registered connection ID
         * @param failure      why the signaling gave up
         */
        default void onFailure(String connectionId, SignalingFailure failure) {
            onSignal(NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR + " " + connectionId
                    + " " + failure.errorCode());
        }
    }
}

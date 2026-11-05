package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.concurrent.CompletionStage;

/**
 * One WebRTC engine: its threads, its sockets and its process-level settings.
 * An engine has exactly one owner, which closes it. A channel that receives an
 * engine it did not create never closes it.
 *
 * <p>See the {@linkplain io.github.sendablemetatype.netty.channel.nethernet.engine
 * package documentation} for the threading, ordering and teardown contract.
 */
public interface NetherNetEngine {

    /**
     * Creates a peer without waiting for the engine. The peer exists from the
     * moment this method returns; a failure to create it natively fails the
     * peer's first stage and fires {@link PeerListener#onClosed} with
     * {@link CloseCause#NEGOTIATION_FAILED}.
     *
     * @param config the peer's configuration
     * @param listener receives the peer's events on engine threads
     * @return the new peer, owned by the caller
     */
    Peer createPeer(PeerConfig config, PeerListener listener);

    /**
     * Opens a UDP port that peers can share for their ICE host candidates.
     *
     * @param config the port and the local addresses to bind on
     * @return completes with the endpoint once its sockets are bound, or
     *         exceptionally if binding failed
     */
    CompletionStage<IceEndpoint> openEndpoint(EndpointConfig config);

    /**
     * Closes every peer and endpoint of this engine, then the engine's threads.
     * Idempotent.
     *
     * @return completes once every peer's close stage has completed and the
     *         engine's native resources are released
     */
    CompletionStage<Void> close();
}

/**
 * The boundary between the NetherNet transport and a WebRTC engine.
 *
 * <p>No engine types appear above this package: channels, signaling and
 * options use only the types defined here. An engine is chosen per server or
 * client channel; nothing here is process wide.
 *
 * <h2>Threading</h2>
 * <ul>
 * <li>Every method of {@link NetherNetEngine}, {@link Peer}, {@link DataLink}
 * and {@link IceEndpoint} may be called from any thread, including event loops
 * and listener callbacks, and never blocks on an engine thread. Operations
 * that the engine performs on its own threads return a
 * {@link java.util.concurrent.CompletionStage}.</li>
 * <li>{@link PeerListener} callbacks run on engine threads. For one peer they
 * are serialized and ordered. They must return promptly and must not block.
 * Send completions are the exception: they may arrive on another engine
 * thread, concurrently with listener callbacks.</li>
 * <li>A buffer handed to {@link PeerListener#onMessage} or
 * {@link PeerListener#onTextMessage} is valid only during the call.</li>
 * </ul>
 *
 * <h2>Ordering</h2>
 * <ul>
 * <li>{@link PeerListener#onLocalCandidate} never fires before the local
 * description it belongs to has been returned by {@link Peer#answer} or
 * {@link Peer#offer}.</li>
 * <li>{@link PeerListener#onRemoteChannel} fires before any message of that
 * link.</li>
 * <li>No callback fires for a peer after its {@link Peer#close} stage
 * completed.</li>
 * </ul>
 *
 * <h2>Teardown</h2>
 * Native teardown never runs inside an engine callback. {@link Peer#close} is
 * idempotent, allowed from callbacks, and completes only after the peer's
 * termination: listeners removed, data links closed, the native peer closed,
 * pending sends settled with their completions returned, and native handles
 * released. Capacity that a peer holds (admission slots, throttle entries) is
 * released when that stage completes, not when close is called.
 */
package io.github.sendablemetatype.netty.channel.nethernet.engine;

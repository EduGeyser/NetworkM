package io.github.sendablemetatype.netty.channel.nethernet.signaling;

/**
 * The state of a signaling source that keeps its own connection to a signaling service and
 * recovers it by itself.
 */
public enum SignalingState {
    /** Created; nothing started yet. */
    NEW,
    /** Started and not connected yet. Failed attempts are retried with backoff. */
    STARTING,
    /** Connected and healthy: joins can arrive. */
    CONNECTED,
    /** A connection was lost; attempts to replace it are running or scheduled. Players already
     * connected are not affected. */
    RECONNECTING,
    /** Closed for good. */
    CLOSED
}

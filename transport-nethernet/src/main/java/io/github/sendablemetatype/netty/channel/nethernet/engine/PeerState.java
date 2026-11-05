package io.github.sendablemetatype.netty.channel.nethernet.engine;

/** A peer's overall connection state, as in the WebRTC specification. */
public enum PeerState {
    NEW,
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
    FAILED,
    CLOSED
}

package io.github.sendablemetatype.netty.channel.nethernet.engine;

/** A peer's ICE connection state, as in the WebRTC specification. */
public enum IceState {
    NEW,
    CHECKING,
    CONNECTED,
    COMPLETED,
    DISCONNECTED,
    FAILED,
    CLOSED
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;

/**
 * The engine a server or client channel creates for itself when the caller
 * supplies none. The channel owns the result and closes it.
 */
final class DefaultEngine {

    private DefaultEngine() {
    }

    /**
     * Creates the default engine.
     *
     * @throws UnsupportedOperationException until an engine implementation is wired in
     */
    static NetherNetEngine create() {
        // MERGE: return the webrtc-java engine here (engine.webrtc.WebRtcEngine with its default settings:
        // no TCP candidates, IPv6 on Wi-Fi, shared socket, costly and link-local networks skipped).
        throw new UnsupportedOperationException("No default NetherNet engine is wired into this build");
    }
}

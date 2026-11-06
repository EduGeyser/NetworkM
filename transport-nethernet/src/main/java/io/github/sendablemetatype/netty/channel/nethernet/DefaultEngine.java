package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.WebRtcEngine;

/**
 * The engine a server or client channel creates for itself when the caller
 * supplies none. The channel owns the result and closes it.
 */
final class DefaultEngine {

    private DefaultEngine() {
    }

    /**
     * Creates the default engine: webrtc-java with its default settings, which
     * keep the port allocator flags NetworkM's channels always used (no TCP,
     * IPv6 also on Wi-Fi, shared socket, costly and link-local networks
     * skipped).
     */
    static NetherNetEngine create() {
        return WebRtcEngine.builder().build();
    }
}

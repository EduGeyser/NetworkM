package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.List;

/**
 * A STUN or TURN server.
 *
 * @param urls the server's URLs, for example {@code turn:relay.example:3478}
 * @param username the TURN username, or null
 * @param credential the TURN credential, or null. Never printed by
 *        {@link #toString()}.
 */
public record IceServer(List<String> urls, String username, String credential) {

    public IceServer {
        urls = List.copyOf(urls);
        if (urls.isEmpty()) {
            throw new IllegalArgumentException("An ICE server needs at least one URL");
        }
    }

    @Override
    public String toString() {
        return "IceServer[urls=" + urls + ", username=" + username
                + ", credential=" + (credential == null ? "null" : "<redacted>") + "]";
    }
}

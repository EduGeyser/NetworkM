package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.List;

/**
 * A STUN or TURN server, as a host configures it or signaling hands it out.
 *
 * <p>{@link #toString()} never prints the credentials, nor any a URL carries
 * in its authority: this type ends up in log lines and error reports, and a
 * record's own form would print the TURN password.
 *
 * @param urls the server's URLs, for example {@code turn:relay.example:3478}
 * @param username the TURN username, or null
 * @param credential the TURN credential, or null
 */
public record IceServer(List<String> urls, String username, String credential) {

    public IceServer {
        urls = List.copyOf(urls);
        if (urls.isEmpty()) {
            throw new IllegalArgumentException("An ICE server needs at least one URL");
        }
    }

    /** Whether the server relays: any of its URLs is a {@code turn:} or {@code turns:} one. */
    public boolean isTurn() {
        return urls.stream().anyMatch(url -> url.regionMatches(true, 0, "turn:", 0, 5)
                || url.regionMatches(true, 0, "turns:", 0, 6));
    }

    @Override
    public String toString() {
        return "IceServer[urls=" + urls.stream().map(IceServer::redacted).toList()
                + ", username=" + (username == null ? "null" : "<redacted>")
                + ", credential=" + (credential == null ? "null" : "<redacted>") + "]";
    }

    /** The URL with any userinfo replaced. */
    private static String redacted(String url) {
        int at = url.lastIndexOf('@');
        if (at < 0) {
            return url;
        }
        int scheme = url.indexOf(':');
        int authority = scheme < 0 ? 0 : url.startsWith("://", scheme) ? scheme + 3 : scheme + 1;
        return url.substring(0, Math.min(authority, at)) + "<redacted>@" + url.substring(at + 1);
    }
}

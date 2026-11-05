package io.github.sendablemetatype.netty.channel.nethernet.ice;

import java.util.List;
import java.util.Objects;

/**
 * A STUN or TURN server for ICE, as signaling hands it out or a host configures it. The engine
 * takes the credentials as separate fields (webrtc-java's {@code RTCIceServer}).
 * <p>
 * {@link #toString()} never prints the credentials, since this type ends up in log lines and
 * error reports, and a record's own form would print the TURN password.
 *
 * @param username The username, empty for none
 * @param password The password, empty for none
 * @param urls     The server's URLs, such as {@code turn:relay.example:3478?transport=udp}
 */
public record IceServerInfo(String username, String password, List<String> urls) {

    public IceServerInfo {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        urls = List.copyOf(urls);
    }

    /**
     * Never prints the credentials, nor any a URL carries in its authority.
     */
    @Override
    public String toString() {
        return "IceServerInfo[urls=" + this.urls.stream().map(IceServerInfo::redacted).toList()
                + ", username=" + (this.username.isEmpty() ? "" : "***") + "]";
    }

    /** The URL with any userinfo replaced. */
    private static String redacted(String url) {
        int at = url.lastIndexOf('@');
        if (at < 0) {
            return url;
        }
        int scheme = url.indexOf(':');
        int authority = scheme < 0 ? 0 : url.startsWith("://", scheme) ? scheme + 3 : scheme + 1;
        return url.substring(0, Math.min(authority, at)) + "***@" + url.substring(at + 1);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * What the handlers of one HTTP signaling connection share. Touched only on the connection's
 * event loop.
 */
final class HttpConnection {
    final Channel channel;
    /** The client a trusted proxy declared in its PROXY header, or null. */
    InetSocketAddress proxyDeclared;
    /** Whether the connection speaks TLS. */
    boolean tls;
    /** Whether the connection was counted against a client address, or decided not to be. */
    boolean addressCounted;
    /** The response that may leave the connection open for one more request; any other ends it. */
    HttpResponse keepOpen;

    private final HttpConnectionLimits limits;

    HttpConnection(Channel channel, HttpConnectionLimits limits) {
        this.channel = channel;
        this.limits = limits;
    }

    /**
     * Counts this connection against its client's address, once.
     *
     * @param client the client's address
     * @return false when that address already holds its share, in which case the caller closes
     */
    boolean countAddress(InetAddress client) {
        if (addressCounted) {
            return true;
        }
        addressCounted = true;
        InetAddress key = limits.admit(client);
        if (key == null) {
            return false;
        }
        // Given back on the close itself, which no handler can hold up
        channel.closeFuture().addListener(future -> limits.release(key));
        return true;
    }
}

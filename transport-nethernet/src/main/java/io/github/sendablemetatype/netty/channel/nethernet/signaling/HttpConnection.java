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
    /** The response that may leave the connection open for one more request; any other ends it. */
    HttpResponse keepOpen;

    private final HttpConnectionLimits limits;
    /** The key the connection is counted under in the per-address cap, or null. */
    private InetAddress counted;

    HttpConnection(Channel channel, HttpConnectionLimits limits) {
        this.channel = channel;
        this.limits = limits;
        // Given back on the close itself, which no handler can hold up
        channel.closeFuture().addListener(future -> release());
    }

    /**
     * Counts this connection against the client it now serves. A connection counts under one
     * address at a time: when a proxy sends another client's request on it, the count moves to
     * that client.
     *
     * @param client the client's address
     * @return false when that address already holds its share, or the connection is closed; the
     *         connection is then counted under no address, and the caller closes it
     */
    boolean countAddress(InetAddress client) {
        if (HttpConnectionLimits.key(client).equals(counted)) {
            return true;
        }
        release();
        if (!channel.isOpen()) {
            // The close already gave back what the connection held, and nothing would give this back
            return false;
        }
        counted = limits.admit(client);
        return counted != null;
    }

    private void release() {
        if (counted != null) {
            limits.release(counted);
            counted = null;
        }
    }
}

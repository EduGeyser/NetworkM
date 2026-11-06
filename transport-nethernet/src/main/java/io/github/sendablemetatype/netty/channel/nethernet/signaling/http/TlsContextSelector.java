package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.netty.handler.ssl.SslContext;

/**
 * Picks the TLS context for one connection to HTTP signaling. Asked only when a connection opens
 * with a TLS ClientHello, so plain HTTP connections never call it, and a new certificate applies to
 * the next connection without a rebind.
 *
 * <p>Answering null withdraws TLS for that connection: it is closed before any byte reaches HTTP.
 * The client sees TLS refused, which on current clients leads to plain HTTP on the same port and
 * the trust prompt, whereas a certificate it rejects may end the join (see
 * {@code docs/client-behavior.md}). Withdrawing is the way to avoid serving a certificate a client
 * would reject, such as an IP address certificate to a player who connected by domain name.
 */
@FunctionalInterface
public interface TlsContextSelector {

    /**
     * Called on the connection's I/O thread; must not block.
     *
     * @param serverName the server name the client asked for (SNI), lowercase, or null when it
     *                   sent none, which clients do when they connect to an IP address
     * @return the context to serve, or null to withdraw TLS for this connection
     */
    SslContext select(String serverName);
}

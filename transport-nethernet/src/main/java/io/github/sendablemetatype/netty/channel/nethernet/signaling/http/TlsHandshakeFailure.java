package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * A TLS handshake on HTTP signaling that started and did not complete: the {@link TlsContextSelector}
 * served a context, and the handshake ended in an error, most often because the client rejected the
 * certificate. Refused TLS is not one, since no handshake started: the selector withdrew TLS, or no
 * selector is configured.
 *
 * @param client     the client: the address a trusted proxy declared in its PROXY protocol header,
 *                   else the connection's peer
 * @param serverName the server name the client asked for (SNI), lowercase, or null when it sent none
 * @param cause      what ended the handshake, as the TLS handler reported it: an
 *                   {@link javax.net.ssl.SSLException} for a TLS error, such as the client's alert
 *                   for a certificate it rejected; a {@link java.nio.channels.ClosedChannelException}
 *                   when the connection closed before the handshake finished
 */
public record TlsHandshakeFailure(InetSocketAddress client, String serverName, Throwable cause) {

    public TlsHandshakeFailure {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(cause, "cause");
    }
}

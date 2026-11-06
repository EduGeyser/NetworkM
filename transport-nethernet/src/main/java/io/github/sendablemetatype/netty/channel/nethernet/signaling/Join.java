package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;

import java.util.Objects;

/**
 * One client's request to join: its offer, where it came from, and how to answer it.
 *
 * @param connectionId the connection ID the client chose for this attempt
 * @param remoteNetworkId the client's network ID, for logs and for sources that address replies
 *        by it
 * @param offer the client's SDP offer, as received
 * @param source the address the join came from as the source saw it; {@link SourcedAddress#unknown()}
 *        when the source cannot tell (Xbox RPC)
 * @param reply the way back to this client
 * @param addressed what the client reached, when the source sees it (HTTP signaling); null for
 *        every other source
 */
public record Join(long connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply,
                   Addressed addressed) {

    public Join {
        Objects.requireNonNull(remoteNetworkId, "remoteNetworkId");
        Objects.requireNonNull(offer, "offer");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(reply, "reply");
    }

    /** A join from a source that does not see what the client reached. */
    public Join(long connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply) {
        this(connectionId, remoteNetworkId, offer, source, reply, null);
    }

    /**
     * What the client reached, as an HTTP request shows it. The endpoint announces the endpoint the
     * player typed from it (ruling G-2), see {@code CandidateAnnouncement.typedEndpoint}.
     *
     * @param host the request's {@code Host} header, or null when it had none. Clients put what the
     *        player reached there: a domain, or the IP a server listing returned
     * @param localPort the local port the request arrived on, 0 when unknown, to tell whether ICE
     *        shares it
     * @param viaTrustedProxy whether a trusted proxy sent the request, in either role; its
     *        {@code Host} then names the proxy, not this server
     */
    public record Addressed(String host, int localPort, boolean viaTrustedProxy) {
    }
}

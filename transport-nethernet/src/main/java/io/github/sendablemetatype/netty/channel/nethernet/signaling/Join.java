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
 * @param reached what the client says it reached, or null when the source cannot tell
 */
public record Join(long connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply,
                   Reached reached) {

    public Join {
        Objects.requireNonNull(remoteNetworkId, "remoteNetworkId");
        Objects.requireNonNull(offer, "offer");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(reply, "reply");
    }

    /** A join from a source that cannot tell what the client reached. */
    public Join(long connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply) {
        this(connectionId, remoteNetworkId, offer, source, reply, null);
    }

    /**
     * The address a player typed, as an HTTP client names it in the {@code Host} header. The
     * endpoint may announce it in the answer (ruling G-2), since it is what an operator would
     * otherwise advertise.
     *
     * @param host the {@code Host} header as received
     * @param localPort the local port the request arrived on, which tells whether the typed port
     *        is the outside port of the ICE port's forward too
     */
    public record Reached(String host, int localPort) {

        public Reached {
            Objects.requireNonNull(host, "host");
        }
    }
}

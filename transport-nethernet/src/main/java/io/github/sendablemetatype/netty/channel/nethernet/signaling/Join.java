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
 */
public record Join(long connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply) {

    public Join {
        Objects.requireNonNull(remoteNetworkId, "remoteNetworkId");
        Objects.requireNonNull(offer, "offer");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(reply, "reply");
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.ClientInfo;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * One client's request to join: its offer, where it came from, and how to answer it.
 *
 * @param connectionId the connection ID the client chose for this attempt, exactly as it wrote it
 * @param remoteNetworkId the client's network ID, for logs and for sources that address replies
 *        by it
 * @param offer the client's SDP offer, as received
 * @param source the address the join came from as the source saw it; {@link SourcedAddress#unknown()}
 *        when the source cannot tell (Xbox RPC)
 * @param reply the way back to this client
 * @param addressed what the client reached, when the source sees it (HTTP signaling); null for
 *        every other source
 * @param authenticated what a source that authenticated the client itself brings instead of an
 *        offer check (NXS); null for every signaled join
 */
public record Join(String connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply,
                   Addressed addressed, Authenticated authenticated) {

    /**
     * @throws IllegalArgumentException if the connection ID is not one {@link Signal} accepts, so a
     *                                  custom source cannot put an ID into logs and child attributes
     *                                  that the built-in sources would refuse
     */
    public Join {
        Signal.requireValidConnectionId(connectionId);
        Objects.requireNonNull(remoteNetworkId, "remoteNetworkId");
        Objects.requireNonNull(offer, "offer");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(reply, "reply");
    }

    /** A join from a source that does not see what the client reached. */
    public Join(String connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply) {
        this(connectionId, remoteNetworkId, offer, source, reply, null, null);
    }

    /** A signaled join. */
    public Join(String connectionId, String remoteNetworkId, String offer, SourcedAddress source, ReplyPath reply,
                Addressed addressed) {
        this(connectionId, remoteNetworkId, offer, source, reply, addressed, null);
    }

    /**
     * What the client reached and said about itself, as an HTTP request shows it. The endpoint
     * announces the endpoint the player typed from it, see
     * {@code CandidateAnnouncement.typedEndpoint}, and shows it to the host's join filter.
     *
     * @param host the request's {@code Host} header, or null when it had none. Clients put what the
     *        player reached there: a domain, or the IP a server listing returned
     * @param localPort the local port the request arrived on, 0 when unknown, to tell whether ICE
     *        shares it
     * @param viaTrustedProxy whether a trusted proxy sent the request, in either role; its
     *        {@code Host} then names the proxy, not this server
     * @param tls whether the connection the request came over spoke TLS (HTTPS) to this server. A
     *        forwarder that ended TLS itself and passed the request on in plain HTTP makes it false
     * @param client what the request's query said about the client, or null when it said nothing
     *        usable
     */
    public record Addressed(String host, int localPort, boolean viaTrustedProxy, boolean tls, ClientInfo client) {
    }

    /**
     * A join whose source authenticated the client itself, as NXS does with its admission tokens and its
     * provider's verified client keys. The endpoint runs no offer check for it: it still applies the throttle and the
     * capacity budget, creates the peer with these credentials and this identity, and installs this binding on the
     * child, which decides the login. The client built or got its view of the host's answer from what the source
     * published, so the peer must match it exactly.
     *
     * @param credentials the local ICE credentials the peer must use
     * @param identity    the DTLS identity the peer presents, whose fingerprint the client knows
     * @param binding     the login binding the child carries
     * @param created     told about the child once it exists, on the endpoint's event loop, before the endpoint
     *                    exposes it; must not block. A join refused before that never calls it.
     */
    public record Authenticated(IceCredentials credentials, DtlsIdentity identity, TransportIdentityBinding binding,
                                Consumer<NetherNetChildChannel> created) {

        public Authenticated {
            Objects.requireNonNull(credentials, "credentials");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(created, "created");
        }
    }
}

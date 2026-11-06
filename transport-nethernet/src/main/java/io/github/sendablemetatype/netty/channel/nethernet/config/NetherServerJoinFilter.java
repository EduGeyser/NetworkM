package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.ClientInfo;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;

import java.util.Objects;

/**
 * The host's say over who joins a server channel, for example an allow list or a ban list. The
 * endpoint asks it for every join from every source, in its admission stage: after the identity
 * check passed the source's policy, before any peer exists.
 *
 * <p>It runs off the I/O loops, on the executor the identity checks run on, and shares that
 * executor's threads and queue with them. It may block, but its time counts against the join's
 * identity check deadline ({@link NetherChannelOption#NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS}),
 * which runs from the start of the check: a filter that answers past it refuses the join with
 * {@link JoinRefusal#NO_ANSWER_IN_TIME}, and its late answer admits nothing. A filter that throws
 * refuses the join with {@link JoinRefusal#INTERNAL}, as a failed identity check does; the first
 * failure is logged as a warning, later ones at debug. A join the executor has no room for is
 * refused with {@link JoinRefusal#CAPACITY}.
 */
@FunctionalInterface
public interface NetherServerJoinFilter {

    /**
     * @param join what is known about the join
     * @return true to let it in, false to refuse it with {@link JoinRefusal#HOST_REFUSED}
     */
    boolean allows(Request join);

    /**
     * One join as the filter sees it. Everything except the source and the check result comes from
     * the client unchecked.
     *
     * @param source          the source that delivered the join, to tell HTTP, Xbox RPC, LAN or
     *                        NXS joins apart
     * @param remoteNetworkId the client's network ID
     * @param address         where the join came from, as the source saw it
     * @param host            the {@code Host} header of an HTTP join: what the player reached, a
     *                        domain or the IP a server listing returned; null on other sources
     * @param client          what an HTTP join's query said about the client, or null
     * @param check           the offer's identity check, which the source's policy admitted; null
     *                        for a join whose source authenticated the client itself (NXS)
     */
    record Request(JoinSource source, String remoteNetworkId, SourcedAddress address, String host,
                   ClientInfo client, OfferCheckResult check) {

        public Request {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(remoteNetworkId, "remoteNetworkId");
            Objects.requireNonNull(address, "address");
        }
    }
}

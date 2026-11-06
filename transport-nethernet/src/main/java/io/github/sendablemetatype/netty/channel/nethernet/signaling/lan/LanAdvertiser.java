package io.github.sendablemetatype.netty.channel.nethernet.signaling.lan;

/**
 * Decides what a server advertises to each LAN discovery request: the LAN counterpart of
 * {@link io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusProvider}, so
 * the player count can be live and a requester can be left without a response.
 *
 * <p>Called for every discovery request that passed decryption and validation, off the I/O
 * threads, with at most 64 calls running at once and a deadline of 1 s each. An answer that comes
 * later, a call that throws or answers null, server data too large for one datagram, and a request
 * beyond those 64 are all answered with the last server data this advertiser produced, or with no
 * response when there is none yet. Anyone on the network can send discovery requests, so keep the
 * work per call small, for example by returning a cached record.
 */
@FunctionalInterface
public interface LanAdvertiser {

    /**
     * @param request the request
     * @return the server data to answer with, or a decline
     */
    LanAdvertisement advertise(LanDiscoveryRequest request);
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling.lan;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * One LAN discovery request, as a {@link LanAdvertiser} sees it. It was decrypted and checked
 * before the advertiser is asked, but the discovery key is public, so anyone on the network can
 * send one with any network ID and, on a network that does not filter it, any source address.
 *
 * @param requester where the request came from
 * @param networkId the network ID the requester announced
 */
public record LanDiscoveryRequest(InetSocketAddress requester, String networkId) {

    public LanDiscoveryRequest {
        Objects.requireNonNull(requester, "requester");
        Objects.requireNonNull(networkId, "networkId");
    }
}

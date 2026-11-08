/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.address;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

/**
 * A peer's address and where it came from.
 *
 * @param address The address, always resolved
 * @param source  Where it came from
 * @param via     The proxy, forwarder or relay that declared it, or null when nothing stood in between or it is
 *                not known
 */
public record SourcedAddress(InetSocketAddress address, AddressSource source, InetSocketAddress via) {

    public SourcedAddress {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(source, "source");
        if (address.isUnresolved()) {
            throw new IllegalArgumentException("An unresolved address names no peer: " + address);
        }
    }

    /**
     * @return A peer nothing has located yet, at a unique placeholder
     */
    public static SourcedAddress unknown() {
        return new SourcedAddress(PlaceholderAddress.next(), AddressSource.UNKNOWN, null);
    }

    /**
     * Where an HTTP signaling request came from. A PROXY protocol header from a trusted proxy wins,
     * as the more trustworthy of the two; then {@code X-Forwarded-For} from a trusted forwarder;
     * otherwise the connection itself.
     *
     * @param peer          The connection's remote address
     * @param proxyProtocol The client a PROXY protocol header declared, or null when none was read
     * @param forwardedFor  Every {@code X-Forwarded-For} header line, in order, empty when there is none
     * @param trusted       The trusted proxies
     * @return The client's address and its source
     */
    public static SourcedAddress signaling(InetSocketAddress peer, InetSocketAddress proxyProtocol,
                                           List<String> forwardedFor, TrustedProxies trusted) {
        if (proxyProtocol != null && trusted.trustsProxyProtocol(peer)) {
            return new SourcedAddress(proxyProtocol, AddressSource.SIGNALING_PROXY_PROTOCOL, peer);
        }
        return trusted.forwardedClient(peer, forwardedFor)
                .map(client -> new SourcedAddress(client, AddressSource.SIGNALING_FORWARDED_FOR, peer))
                .orElseGet(() -> new SourcedAddress(peer, AddressSource.SIGNALING, null));
    }

    /**
     * The remote side of a selected ICE pair. TURN when the remote candidate is a relay, or is one
     * the peer signaled as its relay, which is how a TURN relay shows when its checks arrive through
     * a PROXY protocol relay (as peer reflexive). Then the reported address is the relay itself:
     * the transport peer actually connected, as with a proxy.
     *
     * @param remote        The remote side's address, real even when peer reflexive
     * @param remoteType    The remote candidate's type
     * @param signaledRelay Whether the peer signaled {@code remote} as one of its TURN relays
     * @param proxyRelay    The PROXY protocol relay that brought the peer, or null when none did
     * @return The address and its source
     */
    public static SourcedAddress selectedPair(InetSocketAddress remote, CandidateType remoteType,
                                              boolean signaledRelay, InetSocketAddress proxyRelay) {
        AddressSource source = remoteType == CandidateType.RELAY || signaledRelay ? AddressSource.TURN
                : proxyRelay != null ? AddressSource.ICE_PROXY_PROTOCOL : AddressSource.ICE;
        return new SourcedAddress(remote, source, proxyRelay);
    }

    /**
     * @return Whether a trusted proxy in front of signaling declared this address
     */
    public boolean proxied() {
        return this.source == AddressSource.SIGNALING_PROXY_PROTOCOL
                || this.source == AddressSource.SIGNALING_FORWARDED_FOR;
    }
}

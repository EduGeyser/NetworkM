package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.util.address.EndpointAddress;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * What an answer announces besides, or instead of, what ICE gathered. Endpoints with port 0 take
 * the port of the host candidate they are based on, which is the media port.
 * <p>
 * Pick one per join with {@link #of}, then apply it with {@link AnswerCandidates#fullAnswer} or
 * {@link AnswerCandidates#trickled}.
 */
public sealed interface CandidateAnnouncement {

    /** Announces what ICE gathered, as it is. */
    CandidateAnnouncement AS_GATHERED = new Added(List.of());

    /**
     * Endpoints the host configured. Listed endpoints this host gathered narrow the host
     * candidates to them; the others are the public side of a NAT that forwards to the media port
     * and are announced as server reflexive candidates. A list that names no gathered endpoint
     * narrows nothing.
     *
     * @param endpoints The resolved endpoints, see {@link AdvertisedAddresses}
     */
    record Advertised(List<InetSocketAddress> endpoints) implements CandidateAnnouncement {
        public Advertised {
            endpoints = List.copyOf(endpoints);
        }
    }

    /**
     * The PROXY protocol relays that front the media port. Only the relays are announced, as server
     * reflexive candidates with their base hidden, plus this host's TURN relay candidates with
     * their related address hidden, so players never learn this host's own addresses.
     *
     * @param relays The relays' public endpoints
     */
    record Relayed(List<InetSocketAddress> relays) implements CandidateAnnouncement {
        public Relayed {
            relays = List.copyOf(relays);
        }
    }

    /**
     * Endpoints added to what ICE gathered, which stays as it is: fresh STUN mappings and the
     * endpoint the player typed. An endpoint some UDP candidate already names is not repeated.
     *
     * @param endpoints The endpoints to add
     */
    record Added(List<InetSocketAddress> endpoints) implements CandidateAnnouncement {
        public Added {
            endpoints = List.copyOf(endpoints);
        }
    }

    /**
     * Picks what a join's answer announces: the configured addresses when there are any, as relays
     * when PROXY protocol relays front the media port; otherwise the fresh STUN mappings and the
     * endpoint the player typed.
     * <p>
     * With relays but no advertised addresses, the answer stays as gathered and names this host;
     * players can then only connect if something in front of signaling rewrites the answers.
     * The endpoint should warn about that configuration when it binds.
     *
     * @param advertised   The resolved advertised addresses, empty when none are configured
     * @param mediaRelayed Whether PROXY protocol relays front the media port
     * @param discovered   The STUN mappings fresh at the time of the answer
     * @param typed        The endpoint the player typed, from {@link #typedEndpoint}
     * @return The announcement
     */
    static CandidateAnnouncement of(List<InetSocketAddress> advertised, boolean mediaRelayed,
                                    List<InetSocketAddress> discovered, Optional<InetSocketAddress> typed) {
        if (!advertised.isEmpty()) {
            return mediaRelayed ? new Relayed(advertised) : new Advertised(advertised);
        }
        List<InetSocketAddress> added = new ArrayList<>(discovered);
        if (!mediaRelayed) {
            typed.ifPresent(added::add);
        }
        return added.isEmpty() ? AS_GATHERED : new Added(added);
    }

    /**
     * The endpoint the player typed, read from the HTTP {@code Host} header, which is what an
     * operator would otherwise advertise. Clients put what the player reached there: a domain, or
     * the IP a server listing returned (2026-11-05 captures). Only an IP literal a peer could reach
     * counts, since a name would need a lookup per join, and never through a trusted proxy, whose
     * {@code Host} names the proxy.
     * <p>
     * When ICE shares the port number the request arrived on, one forward carries both, so the
     * typed port is the outside one for media too. Otherwise the typed port is signaling's alone,
     * and the media port is assumed forwarded as it is.
     *
     * @param host            The {@code Host} header, or null when the request had none
     * @param viaTrustedProxy Whether the request came from a trusted proxy in either role
     * @param icePort         The fixed ICE port, or 0 when each session takes its own
     * @param requestOnIcePort Whether the request arrived on the ICE port number
     * @return The endpoint to add, or empty
     */
    static Optional<InetSocketAddress> typedEndpoint(String host, boolean viaTrustedProxy, int icePort,
                                                     boolean requestOnIcePort) {
        if (host == null || viaTrustedProxy) {
            return Optional.empty();
        }
        InetSocketAddress endpoint;
        try {
            endpoint = EndpointAddress.parseEndpoint(host.strip());
        } catch (IllegalArgumentException notALiteral) {
            return Optional.empty();
        }
        if (!EndpointAddress.advertisable(endpoint.getAddress(), false)) {
            return Optional.empty();
        }
        if (icePort == 0) {
            // Port 0 leaves each host candidate's own
            return Optional.of(new InetSocketAddress(endpoint.getAddress(), 0));
        }
        int port = requestOnIcePort && endpoint.getPort() != 0 ? endpoint.getPort() : icePort;
        return Optional.of(new InetSocketAddress(endpoint.getAddress(), port));
    }
}

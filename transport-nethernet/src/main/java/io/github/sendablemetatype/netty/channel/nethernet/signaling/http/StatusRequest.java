package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;

import java.util.Objects;

/**
 * One {@code GET /v1/join} status request, as a {@link StatusProvider} sees it. Everything here
 * except the source comes from the client unchecked.
 *
 * @param host      the {@code Host} header, or null when the request had none. Clients put what
 *                  the player reached there: a domain such as {@code play.example.com:19132}, or
 *                  the IP a server listing returned (captured 2026-11-05)
 * @param source    where the request came from, through trusted proxies as configured
 * @param client    what the client said about itself in the query, or null when it said nothing
 *                  usable, as on the status request right before a join
 * @param networkId the {@code id} query parameter, or null. On the server list poll it is the
 *                  client's NetherNet network ID for that run, the one its later
 *                  {@code POST /v1/join/{id}} carries (retail 1.26.62)
 */
public record StatusRequest(String host, SourcedAddress source, ClientInfo client, String networkId) {

    public StatusRequest {
        Objects.requireNonNull(source, "source");
    }
}

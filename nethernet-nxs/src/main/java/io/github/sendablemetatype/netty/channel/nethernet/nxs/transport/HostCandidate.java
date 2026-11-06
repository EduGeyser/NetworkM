package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * A UDP candidate on the host's gameplay port that players are offered.
 *
 * @param endpoint A numeric address that is neither wildcard nor multicast, and a fixed port
 * @param type     Host, server reflexive or relay, the types a host profile has in NXS v1. A host learns no
 *                 peer-reflexive candidate of its own before a client checks it, so none can be published.
 */
public record HostCandidate(InetSocketAddress endpoint, CandidateType type) {
    public HostCandidate {
        Objects.requireNonNull(type, "type");
        if (type == CandidateType.PEER_REFLEXIVE || type == CandidateType.UNKNOWN) {
            throw new IllegalArgumentException("A host profile publishes host, server reflexive or relay candidates");
        }
        if (endpoint == null || endpoint.isUnresolved() || endpoint.getPort() < 1
                || endpoint.getAddress().isAnyLocalAddress() || endpoint.getAddress().isMulticastAddress()) {
            throw new IllegalArgumentException("A concrete numeric UDP endpoint is required");
        }
        try {
            // Drops any scope ID and host name, so equal addresses compare equal
            endpoint = new InetSocketAddress(InetAddress.getByAddress(endpoint.getAddress().getAddress()),
                    endpoint.getPort());
        } catch (UnknownHostException impossible) {
            throw new IllegalArgumentException(impossible);
        }
    }

    /** A host candidate. */
    public static HostCandidate host(InetSocketAddress endpoint) {
        return new HostCandidate(endpoint, CandidateType.HOST);
    }

    /** The endpoint's address family. */
    public AddressFamily family() {
        return AddressFamily.of(endpoint.getAddress());
    }

    /** The type's name in a host profile, which is also the one SDP gives it after {@code typ}. */
    public String typeName() {
        return switch (type) {
            case HOST -> "host";
            case SERVER_REFLEXIVE -> "srflx";
            case RELAY -> "relay";
            case PEER_REFLEXIVE, UNKNOWN -> throw new AssertionError("Refused when the candidate was made");
        };
    }
}

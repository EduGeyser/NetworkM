package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * A UDP candidate on the host's gameplay port that players are offered.
 *
 * @param endpoint A numeric address that is neither wildcard nor multicast, and a fixed port
 */
public record HostCandidate(InetSocketAddress endpoint, CandidateType type) {
    public HostCandidate {
        Objects.requireNonNull(type, "type");
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

    public static HostCandidate host(InetSocketAddress endpoint) {
        return new HostCandidate(endpoint, CandidateType.HOST);
    }

    public AddressFamily family() {
        return AddressFamily.of(endpoint.getAddress());
    }
}

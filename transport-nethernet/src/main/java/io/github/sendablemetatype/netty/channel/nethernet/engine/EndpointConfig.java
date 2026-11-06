package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetAddress;
import java.util.List;

/**
 * Where an {@link IceEndpoint} binds.
 *
 * @param port the UDP port, the same on every local address
 * @param addresses the local addresses to bind on; empty for every usable
 *        network
 */
public record EndpointConfig(int port, List<InetAddress> addresses) {

    public EndpointConfig {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port " + port);
        }
        addresses = List.copyOf(addresses);
    }
}

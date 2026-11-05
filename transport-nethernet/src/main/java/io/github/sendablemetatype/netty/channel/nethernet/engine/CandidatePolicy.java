package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetAddress;
import java.util.List;

/**
 * Where a peer gathers its own ICE candidates. NetherNet gathers UDP
 * candidates only; TCP candidates are never gathered.
 *
 * @param ipv6 whether IPv6 addresses are gathered
 * @param addresses the local addresses to gather on; empty for every usable
 *        network
 * @param ports the local port window for host candidates, or null for
 *        ephemeral ports. Ignored for a peer on a shared {@link IceEndpoint}.
 */
public record CandidatePolicy(boolean ipv6, List<InetAddress> addresses, PortRange ports) {

    /** Every network, IPv4 and IPv6, ephemeral ports. */
    public static final CandidatePolicy DEFAULT = new CandidatePolicy(true, List.of(), null);

    public CandidatePolicy {
        addresses = List.copyOf(addresses);
    }

    /**
     * An inclusive range of UDP ports.
     *
     * @param min the lowest port, at least 1
     * @param max the highest port, at least {@code min}, at most 65535
     */
    public record PortRange(int min, int max) {

        public PortRange {
            if (min < 1 || max > 65535 || min > max) {
                throw new IllegalArgumentException("Invalid port range " + min + "-" + max);
            }
        }
    }
}

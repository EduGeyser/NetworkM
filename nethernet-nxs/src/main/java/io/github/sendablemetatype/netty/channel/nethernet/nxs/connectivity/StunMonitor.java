package io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity;

import java.net.InetSocketAddress;

/**
 * A STUN binding the endpoint keeps alive from its gameplay port, so the port's NAT mapping stays open and known
 * with no player connected. webrtc-java's {@code IceUdpEndpoint.StunMonitor} has this shape; the endpoint adapts it.
 */
public interface StunMonitor extends AutoCloseable {
    /**
     * @return The binding's latest state; never blocks
     * @throws RuntimeException If the monitor or its endpoint failed or closed, which withdraws its mapping
     */
    StunReading read();

    /** Stops keeping the binding alive. */
    @Override
    void close();

    /** Opens monitors that share the gameplay port, never a socket of their own. */
    @FunctionalInterface
    interface Factory {
        /**
         * @param server A numeric STUN server of the family the monitor serves
         * @throws RuntimeException If the endpoint cannot monitor from its port
         */
        StunMonitor open(InetSocketAddress server);
    }
}

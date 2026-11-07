package io.github.sendablemetatype.netty.channel.nethernet.config;

import java.io.Serial;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * A NetherNet peer addressed by its network ID, as Xbox and LAN signaling route to it. Two
 * addresses are equal when their network IDs are, so maps and sets keyed by remote address work.
 */
public final class NetherNetAddress extends SocketAddress {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String networkId;

    /**
     * Creates a NetherNetAddress from an opaque Network ID, preserving its value.
     *
     * @param networkId The string Network ID.
     */
    public NetherNetAddress(String networkId) {
        this.networkId = Objects.requireNonNull(networkId, "networkId");
    }

    /**
     * Gets the Network ID as a String.
     *
     * @return the Network ID
     */
    public String getNetworkId() {
        return networkId;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof NetherNetAddress address && networkId.equals(address.networkId);
    }

    @Override
    public int hashCode() {
        return networkId.hashCode();
    }

    /**
     * Returns the string representation of the Network ID.
     *
     * @return the Network ID as a string
     */
    @Override
    public String toString() {
        return networkId;
    }
}

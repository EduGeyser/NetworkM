package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import java.net.Inet4Address;
import java.net.InetAddress;

/** An IP address family, numbered 4 and 6 on the wire. */
public enum AddressFamily {
    IPV4(4),
    IPV6(6);

    private final int number;

    AddressFamily(int number) {
        this.number = number;
    }

    public static AddressFamily of(InetAddress address) {
        return address instanceof Inet4Address ? IPV4 : IPV6;
    }

    /**
     * The family with this wire number.
     *
     * @throws IllegalArgumentException If the number is neither 4 nor 6
     */
    public static AddressFamily of(int number) {
        return switch (number) {
            case 4 -> IPV4;
            case 6 -> IPV6;
            default -> throw new IllegalArgumentException("Unknown address family " + number);
        };
    }

    /** The wire number, 4 or 6. */
    public int number() {
        return number;
    }
}

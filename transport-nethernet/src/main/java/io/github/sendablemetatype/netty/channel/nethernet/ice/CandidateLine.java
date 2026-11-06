package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import io.netty.util.NetUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/**
 * One ICE candidate as SDP writes it (RFC 8839 section 5.1): {@code candidate:<foundation>
 * <component> <transport> <priority> <address> <port> typ <type>}, then optional attributes such
 * as {@code raddr} and {@code rport}. Immutable. Reading one never looks a name up.
 */
public final class CandidateLine {
    private static final String ATTRIBUTE_PREFIX = "a=";
    private static final String PREFIX = "candidate:";

    private final String sdp;
    private final String[] tokens;
    private final CandidateType type;
    private final int port;

    private CandidateLine(String sdp, String[] tokens, CandidateType type, int port) {
        this.sdp = sdp;
        this.tokens = tokens;
        this.type = type;
        this.port = port;
    }

    /**
     * @param line A candidate as an engine reports it ({@code candidate:...}), as an SDP attribute
     *             ({@code a=candidate:...}) or as a trickle signal carries it
     * @return The candidate, or empty when the line is not one or lacks a field
     */
    public static Optional<CandidateLine> parse(String line) {
        String sdp = line.strip();
        if (sdp.startsWith(ATTRIBUTE_PREFIX)) {
            sdp = sdp.substring(ATTRIBUTE_PREFIX.length());
        }
        if (!sdp.startsWith(PREFIX)) {
            return Optional.empty();
        }
        String[] tokens = sdp.split(" ");
        if (tokens.length < 8 || !"typ".equals(tokens[6]) || !tokens[1].matches("[0-9]{1,3}")
                || !tokens[5].matches("[0-9]{1,5}")) {
            return Optional.empty();
        }
        int port = Integer.parseInt(tokens[5]);
        if (port > 65535) {
            return Optional.empty();
        }
        String finalSdp = sdp;
        return CandidateType.fromSdpName(tokens[7]).map(type -> new CandidateLine(finalSdp, tokens, type, port));
    }

    /**
     * @return The candidate as an engine takes it, {@code candidate:...}
     */
    public String sdp() {
        return this.sdp;
    }

    /**
     * @return The candidate as an SDP attribute line, {@code a=candidate:...}, without a line end
     */
    public String attribute() {
        return ATTRIBUTE_PREFIX + this.sdp;
    }

    /**
     * @return The component, 1 for the only one a data channel uses
     */
    public int component() {
        return Integer.parseInt(this.tokens[1]);
    }

    /**
     * @return Whether the candidate is UDP rather than TCP
     */
    public boolean isUdp() {
        return "udp".equalsIgnoreCase(this.tokens[2]);
    }

    /**
     * @return The transport as written, such as {@code udp} or {@code UDP}
     */
    public String transport() {
        return this.tokens[2];
    }

    /**
     * @return The connection address as written: an IP literal, or a name such as an mDNS one
     */
    public String address() {
        return this.tokens[4];
    }

    /**
     * @return The connection port as written
     */
    public int port() {
        return this.port;
    }

    /**
     * @return The candidate's type
     */
    public CandidateType type() {
        return this.type;
    }

    /**
     * @return The address, if it is an IP literal
     */
    public Optional<InetAddress> ipAddress() {
        try {
            return Optional.of(EndpointAddress.parse(this.tokens[4]));
        } catch (UnknownHostException name) {
            return Optional.empty();
        }
    }

    /**
     * @return The address and port, if the address is an IP literal and the port is not 0
     */
    public Optional<InetSocketAddress> endpoint() {
        return this.port == 0 ? Optional.empty() : ipAddress().map(address -> new InetSocketAddress(address, this.port));
    }

    /**
     * The same candidate with its connection address in canonical form. Java reads some literals
     * that the engine's parser refuses, such as IPv4 with leading zeros ({@code 008.008.008.008});
     * the engine would take those for a name and look it up. A name comes back as it is.
     *
     * @return The candidate with a canonical address
     */
    public CandidateLine withCanonicalAddress() {
        Optional<InetAddress> address = ipAddress();
        if (address.isEmpty()) {
            return this;
        }
        String canonical = NetUtil.toAddressString(address.get());
        if (canonical.equals(this.tokens[4])) {
            return this;
        }
        String[] tokens = this.tokens.clone();
        tokens[4] = canonical;
        return new CandidateLine(String.join(" ", tokens), tokens, this.type, this.port);
    }

    /**
     * The same candidate with its related address, which names where a reflexive or relayed
     * candidate came from, replaced by the unspecified address and port 0. A candidate without
     * one comes back as it is.
     *
     * @return The candidate without its origin
     */
    public CandidateLine withRelatedAddressHidden() {
        String[] hidden = this.tokens.clone();
        boolean changed = false;
        for (int i = 8; i + 1 < hidden.length; i += 2) {
            if ("raddr".equals(hidden[i])) {
                String unspecified = hidden[i + 1].indexOf(':') >= 0 ? "::" : "0.0.0.0";
                changed |= !unspecified.equals(hidden[i + 1]);
                hidden[i + 1] = unspecified;
            } else if ("rport".equals(hidden[i])) {
                changed |= !"0".equals(hidden[i + 1]);
                hidden[i + 1] = "0";
            }
        }
        return changed ? new CandidateLine(String.join(" ", hidden), hidden, this.type, this.port) : this;
    }

    /** The address and port in one canonical form, so that equal endpoints written two ways compare equal. */
    String endpointKey() {
        return ipAddress().map(NetUtil::toAddressString).orElse(this.tokens[4]) + " " + this.port;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CandidateLine line && this.sdp.equals(line.sdp);
    }

    @Override
    public int hashCode() {
        return this.sdp.hashCode();
    }

    @Override
    public String toString() {
        return this.sdp;
    }
}

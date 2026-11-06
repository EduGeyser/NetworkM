package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Reads addresses and types from candidate attributes
 * ({@code candidate:<foundation> <component> <transport> <priority> <address> <port> typ <type> ...}).
 */
final class CandidateLines {
    private static final int ADDRESS = 4;
    private static final int PORT = 5;
    private static final int TYP = 6;
    private static final int TYPE = 7;

    private CandidateLines() {
    }

    /**
     * The selected pair as the transport sees it. webrtc-java builds both
     * candidates from the selected connection, so a peer reflexive remote
     * candidate carries its real address instead of libwebrtc's redacted one.
     * Only when the connection could not be read (while closing) does the
     * redacted copy remain; its address is no IP literal and becomes null.
     */
    static TransportPath path(String localCandidate, String remoteCandidate) {
        String[] local = fields(localCandidate);
        String[] remote = fields(remoteCandidate);
        return new TransportPath(address(local), type(local), address(remote), type(remote));
    }

    private static String[] fields(String candidate) {
        return SdpEdits.candidateAttribute(candidate == null ? "" : candidate).split("\\s+");
    }

    /** The address, or null unless it is an IP literal with a port: never resolved through DNS. */
    private static InetSocketAddress address(String[] fields) {
        if (fields.length <= PORT) {
            return null;
        }
        InetAddress address;
        int port;
        try {
            address = InetAddress.ofLiteral(fields[ADDRESS]);
            port = Integer.parseInt(fields[PORT]);
        } catch (IllegalArgumentException e) {
            return null;
        }
        if (address.isAnyLocalAddress() || port < 1 || port > 65535) {
            return null;
        }
        return new InetSocketAddress(address, port);
    }

    private static CandidateType type(String[] fields) {
        if (fields.length <= TYPE || !"typ".equals(fields[TYP])) {
            return CandidateType.UNKNOWN;
        }
        return CandidateType.fromSdpName(fields[TYPE]);
    }
}

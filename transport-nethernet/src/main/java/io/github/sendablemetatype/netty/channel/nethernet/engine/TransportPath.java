package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * A selected ICE candidate pair.
 *
 * @param local the local candidate's address, or null if the engine does not
 *        report it
 * @param localType the local candidate's type
 * @param remote the address the remote side sends from: for a remote relay
 *        candidate the TURN relay's address, never the related address the
 *        remote side declared. Null if the engine does not know it.
 * @param remoteType the remote candidate's type
 */
public record TransportPath(
        InetSocketAddress local,
        CandidateType localType,
        InetSocketAddress remote,
        CandidateType remoteType) {

    public TransportPath {
        Objects.requireNonNull(localType, "localType");
        Objects.requireNonNull(remoteType, "remoteType");
    }

    /** Whether either side of the pair is a TURN relay candidate. */
    public boolean relayed() {
        return localType == CandidateType.RELAY || remoteType == CandidateType.RELAY;
    }

    /** An ICE candidate type. */
    public enum CandidateType {
        HOST,
        SERVER_REFLEXIVE,
        PEER_REFLEXIVE,
        RELAY,
        UNKNOWN
    }
}

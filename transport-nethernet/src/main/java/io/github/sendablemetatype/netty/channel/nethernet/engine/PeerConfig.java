package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.List;
import java.util.Objects;

/**
 * How a peer is set up.
 *
 * @param role whether the peer makes the offer or answers one
 * @param iceServers STUN and TURN servers for this peer, possibly empty
 * @param candidates where the peer gathers its own candidates
 * @param endpoint the shared UDP port the peer gathers its host candidates on,
 *        or null for ports of its own
 * @param identity the DTLS certificate the peer presents, or null for one the
 *        engine generates
 * @param localCredentials the local ICE ufrag and password the peer must use,
 *        or null for ones the engine picks. A peer that takes over a
 *        {@linkplain IceEndpoint.HeldCheck held check} uses the check's local
 *        ufrag here.
 */
public record PeerConfig(
        Role role,
        List<IceServer> iceServers,
        CandidatePolicy candidates,
        IceEndpoint endpoint,
        DtlsIdentity identity,
        IceCredentials localCredentials) {

    public PeerConfig {
        Objects.requireNonNull(role, "role");
        iceServers = List.copyOf(iceServers);
        Objects.requireNonNull(candidates, "candidates");
    }

    public enum Role {
        /** Creates the offer and opens the data channels: the client. */
        OFFERER,
        /** Answers a remote offer and accepts the remote data channels: the server. */
        ANSWERER
    }
}

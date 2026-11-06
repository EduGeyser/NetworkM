package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherOptions.Rule;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherOptions.Setting;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.netty.channel.Channel;

import java.util.List;

/**
 * The config of a NetherNet client channel: the connection's options, plus how its peer gathers
 * candidates, how the handshake is attempted, and the identity it presents.
 */
public class DefaultNetherClientChannelConfig extends DefaultNetherChannelConfig {
    public static final int DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = 3000;
    public static final int DEFAULT_MAX_HANDSHAKE_ATTEMPTS = 3;

    private final Setting<List<IceServer>> iceServers = options.add(NetherChannelOption.NETHER_ICE_SERVERS,
            List.of(), Rule.list());
    private final Setting<CandidatePolicy> candidatePolicy = options.add(NetherChannelOption.NETHER_CANDIDATE_POLICY,
            CandidatePolicy.DEFAULT, Rule.present());
    private final Setting<Integer> handshakeTimeoutMillis = options.add(
            NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, DEFAULT_HANDSHAKE_TIMEOUT_MILLIS, Rule.atLeast(1));
    private final Setting<Integer> maxHandshakeAttempts = options.add(
            NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS, DEFAULT_MAX_HANDSHAKE_ATTEMPTS, Rule.atLeast(1));
    private final Setting<ClientIdentity> identity = options.add(NetherChannelOption.NETHER_CLIENT_IDENTITY,
            null, Rule.orNone());

    public DefaultNetherClientChannelConfig(Channel channel) {
        super(channel);
    }

    public List<IceServer> getIceServers() {
        return iceServers.get();
    }

    public CandidatePolicy getCandidatePolicy() {
        return candidatePolicy.get();
    }

    public int getHandshakeTimeoutMillis() {
        return handshakeTimeoutMillis.get();
    }

    /** @return how many attempts a connect gets, the first one included */
    public int getMaxHandshakeAttempts() {
        return maxHandshakeAttempts.get();
    }

    /** @return the identity the offer presents, or null for none */
    public ClientIdentity getIdentity() {
        return identity.get();
    }
}

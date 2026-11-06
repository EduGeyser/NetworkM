package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherOptions.Rule;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherOptions.Setting;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AdvertisedAddresses;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The config of a NetherNet server channel: admission, identity, addressing and how every
 * child's peer gathers candidates. The children's own connection options are set with
 * {@code ServerBootstrap.childOption}; this config refuses them, like every option it does not
 * take, with an {@link IllegalArgumentException} naming it.
 */
public class DefaultNetherServerChannelConfig extends DefaultChannelConfig {
    public static final int DEFAULT_RTC_HANDSHAKE_TIMEOUT_SECONDS = 30;
    public static final int DEFAULT_OFFER_CHECK_TIMEOUT_MILLIS = 10_000;
    public static final int DEFAULT_MAX_PENDING_JOINS = 256;
    public static final Duration DEFAULT_STUN_MAX_AGE = Duration.ofMinutes(5);

    private final NetherOptions options = new NetherOptions();

    private final Setting<List<IceServer>> iceServers = options.add(NetherChannelOption.NETHER_ICE_SERVERS,
            List.of(), Rule.list());
    private final Setting<CandidatePolicy> candidatePolicy = options.add(NetherChannelOption.NETHER_CANDIDATE_POLICY,
            CandidatePolicy.DEFAULT, Rule.present());
    private final Setting<Integer> rtcHandshakeTimeoutSeconds = options.add(
            NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS, DEFAULT_RTC_HANDSHAKE_TIMEOUT_SECONDS,
            Rule.atLeast(1));
    private final Setting<NetherServerMetrics> serverMetrics = options.add(NetherChannelOption.NETHER_SERVER_METRICS,
            null, Rule.orNone());
    private final Setting<ServerIdentity> serverIdentity = options.add(NetherChannelOption.NETHER_SERVER_IDENTITY,
            null, Rule.orNone());
    private final Setting<OfferCheck> offerCheck = options.add(NetherChannelOption.NETHER_SERVER_OFFER_CHECK,
            OfferCheck.minecraft(), Rule.present());
    private final Setting<Integer> offerCheckTimeoutMillis = options.add(
            NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS, DEFAULT_OFFER_CHECK_TIMEOUT_MILLIS,
            Rule.atLeast(1));
    private final Setting<NetherServerThrottle> throttle = options.add(NetherChannelOption.NETHER_SERVER_THROTTLE,
            new DefaultNetherServerThrottle(), Rule.present());
    private final Setting<Integer> maxSessions = options.add(NetherChannelOption.NETHER_SERVER_MAX_SESSIONS,
            Integer.MAX_VALUE, Rule.atLeast(1));
    private final Setting<Integer> maxPendingJoins = options.add(NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS,
            DEFAULT_MAX_PENDING_JOINS, Rule.atLeast(1));
    private final Setting<Boolean> sharedIcePort = options.add(NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT,
            true, Rule.present());
    private final Setting<List<InetSocketAddress>> advertisedAddresses = options.add(
            NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES, List.of(),
            Rule.list(AdvertisedAddresses.MAX_ENDPOINTS, (option, address) -> address));
    private final Setting<List<InetSocketAddress>> stunServers = options.add(
            NetherChannelOption.NETHER_SERVER_STUN_SERVERS, List.of(), stunServerList());
    private final Setting<Duration> stunMaxAge = options.add(NetherChannelOption.NETHER_SERVER_STUN_MAX_AGE,
            DEFAULT_STUN_MAX_AGE, Rule.positiveDuration());
    private final Setting<Boolean> inferPeerCandidates = options.add(
            NetherChannelOption.NETHER_SERVER_INFER_PEER_CANDIDATES, true, Rule.present());
    private final Setting<TrustedProxies> trustedProxies = options.add(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES,
            TrustedProxies.NONE, Rule.present());
    private final Setting<Boolean> preferProxiedAddress = options.add(
            NetherChannelOption.NETHER_SERVER_PREFER_PROXIED_ADDRESS, false, Rule.present());

    public DefaultNetherServerChannelConfig(Channel channel) {
        super(channel);
    }

    /** Distinct servers, each with a port; names stay unresolved until the channel binds. */
    private static Rule<List<InetSocketAddress>> stunServerList() {
        Rule<List<InetSocketAddress>> servers = Rule.list(Integer.MAX_VALUE, (option, server) -> {
            if (server.getPort() < 1) {
                throw new IllegalArgumentException(option + " needs a port for every server, not " + server);
            }
            return server;
        });
        return (option, value) -> servers.apply(option, value).stream().distinct().toList();
    }

    @Override
    public Map<ChannelOption<?>, Object> getOptions() {
        return getOptions(super.getOptions(), options.options());
    }

    @Override
    public <T> T getOption(ChannelOption<T> option) {
        return options.handles(option) ? options.get(option) : super.getOption(option);
    }

    /**
     * Sets a NetherNet option this channel takes, or one of Netty's.
     *
     * @return true, since every other option is refused
     * @throws IllegalArgumentException if this channel does not take the option, or the value is
     *                                  out of its range
     * @throws NullPointerException     if the value is null and the option has no "none"
     */
    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        if (options.handles(option)) {
            options.set(option, value);
            return true;
        }
        if (super.setOption(option, value)) {
            return true;
        }
        throw NetherOptions.unknown(channel, option);
    }

    public List<IceServer> getIceServers() {
        return iceServers.get();
    }

    public CandidatePolicy getCandidatePolicy() {
        return candidatePolicy.get();
    }

    public int getServerRtcHandshakeTimeoutSeconds() {
        return rtcHandshakeTimeoutSeconds.get();
    }

    public NetherServerMetrics getServerMetrics() {
        return serverMetrics.get();
    }

    /** The identity that signs answers, or null while none was set. */
    public ServerIdentity getServerIdentity() {
        return serverIdentity.get();
    }

    public OfferCheck getOfferCheck() {
        return offerCheck.get();
    }

    public int getOfferCheckTimeoutMillis() {
        return offerCheckTimeoutMillis.get();
    }

    public NetherServerThrottle getThrottle() {
        return throttle.get();
    }

    public int getMaxSessions() {
        return maxSessions.get();
    }

    public int getMaxPendingJoins() {
        return maxPendingJoins.get();
    }

    public boolean isSharedIcePort() {
        return sharedIcePort.get();
    }

    public List<InetSocketAddress> getAdvertisedAddresses() {
        return advertisedAddresses.get();
    }

    public List<InetSocketAddress> getStunServers() {
        return stunServers.get();
    }

    public Duration getStunMaxAge() {
        return stunMaxAge.get();
    }

    public boolean isInferPeerCandidates() {
        return inferPeerCandidates.get();
    }

    public TrustedProxies getTrustedProxies() {
        return trustedProxies.get();
    }

    public boolean isPreferProxiedAddress() {
        return preferProxiedAddress.get();
    }
}

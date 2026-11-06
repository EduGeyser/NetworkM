package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class DefaultNetherServerChannelConfig extends DefaultNetherChannelConfig {
    public static final int DEFAULT_OFFER_CHECK_TIMEOUT_MILLIS = 10_000;
    public static final int DEFAULT_MAX_PENDING_JOINS = 256;

    private volatile int serverRtcHandshakeTimeoutSeconds = 30;
    private volatile NetherServerMetrics serverMetrics;
    private volatile ServerIdentity serverIdentity;
    private volatile OfferCheck offerCheck;
    private volatile int offerCheckTimeoutMillis = DEFAULT_OFFER_CHECK_TIMEOUT_MILLIS;
    private volatile NetherServerThrottle throttle = new DefaultNetherServerThrottle();
    private volatile int maxSessions = Integer.MAX_VALUE;
    private volatile int maxPendingJoins = DEFAULT_MAX_PENDING_JOINS;
    private volatile boolean sharedIcePort = true;
    private volatile List<InetSocketAddress> advertisedAddresses = List.of();
    private volatile boolean inferPeerCandidates = true;
    private volatile TrustedProxies trustedProxies = TrustedProxies.NONE;
    private volatile boolean preferProxiedAddress;

    public DefaultNetherServerChannelConfig(Channel channel) {
        super(channel);
    }

    @Override
    public Map<ChannelOption<?>, Object> getOptions() {
        return this.getOptions(
                super.getOptions(),
                NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS,
                NetherChannelOption.NETHER_SERVER_METRICS,
                NetherChannelOption.NETHER_SERVER_IDENTITY,
                NetherChannelOption.NETHER_SERVER_OFFER_CHECK,
                NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS,
                NetherChannelOption.NETHER_SERVER_THROTTLE,
                NetherChannelOption.NETHER_SERVER_MAX_SESSIONS,
                NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS,
                NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT,
                NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES,
                NetherChannelOption.NETHER_SERVER_INFER_PEER_CANDIDATES,
                NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES,
                NetherChannelOption.NETHER_SERVER_PREFER_PROXIED_ADDRESS
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOption(ChannelOption<T> option) {
        Object value;
        if (option == NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS) {
            value = serverRtcHandshakeTimeoutSeconds;
        } else if (option == NetherChannelOption.NETHER_SERVER_METRICS) {
            value = serverMetrics;
        } else if (option == NetherChannelOption.NETHER_SERVER_IDENTITY) {
            value = serverIdentity;
        } else if (option == NetherChannelOption.NETHER_SERVER_OFFER_CHECK) {
            value = getOfferCheck();
        } else if (option == NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS) {
            value = offerCheckTimeoutMillis;
        } else if (option == NetherChannelOption.NETHER_SERVER_THROTTLE) {
            value = throttle;
        } else if (option == NetherChannelOption.NETHER_SERVER_MAX_SESSIONS) {
            value = maxSessions;
        } else if (option == NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS) {
            value = maxPendingJoins;
        } else if (option == NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT) {
            value = sharedIcePort;
        } else if (option == NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES) {
            value = advertisedAddresses;
        } else if (option == NetherChannelOption.NETHER_SERVER_INFER_PEER_CANDIDATES) {
            value = inferPeerCandidates;
        } else if (option == NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES) {
            value = trustedProxies;
        } else if (option == NetherChannelOption.NETHER_SERVER_PREFER_PROXIED_ADDRESS) {
            value = preferProxiedAddress;
        } else {
            return super.getOption(option);
        }
        return (T) value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        this.validate(option, value);

        if (option == NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS) {
            this.serverRtcHandshakeTimeoutSeconds = (Integer) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_METRICS) {
            this.serverMetrics = (NetherServerMetrics) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_IDENTITY) {
            this.serverIdentity = (ServerIdentity) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_OFFER_CHECK) {
            this.offerCheck = (OfferCheck) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS) {
            this.offerCheckTimeoutMillis = positive("offerCheckTimeoutMillis", (Integer) value);
        } else if (option == NetherChannelOption.NETHER_SERVER_THROTTLE) {
            this.throttle = (NetherServerThrottle) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_MAX_SESSIONS) {
            this.maxSessions = positive("maxSessions", (Integer) value);
        } else if (option == NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS) {
            this.maxPendingJoins = positive("maxPendingJoins", (Integer) value);
        } else if (option == NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT) {
            this.sharedIcePort = (Boolean) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES) {
            this.advertisedAddresses = List.copyOf((List<InetSocketAddress>) value);
        } else if (option == NetherChannelOption.NETHER_SERVER_INFER_PEER_CANDIDATES) {
            this.inferPeerCandidates = (Boolean) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES) {
            this.trustedProxies = (TrustedProxies) value;
        } else if (option == NetherChannelOption.NETHER_SERVER_PREFER_PROXIED_ADDRESS) {
            this.preferProxiedAddress = (Boolean) value;
        } else {
            return super.setOption(option, value);
        }
        return true;
    }

    private static int positive(String name, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    public int getServerRtcHandshakeTimeoutSeconds() {
        return serverRtcHandshakeTimeoutSeconds;
    }

    public NetherServerMetrics getServerMetrics() {
        return serverMetrics;
    }

    /** The identity that signs answers, or null while none was set. */
    public ServerIdentity getServerIdentity() {
        return serverIdentity;
    }

    public OfferCheck getOfferCheck() {
        OfferCheck check = offerCheck;
        if (check == null) {
            // Created on first use, so a host that sets its own never builds the default's key cache.
            check = OfferCheck.minecraft();
            offerCheck = check;
        }
        return check;
    }

    public int getOfferCheckTimeoutMillis() {
        return offerCheckTimeoutMillis;
    }

    public NetherServerThrottle getThrottle() {
        return throttle;
    }

    public int getMaxSessions() {
        return maxSessions;
    }

    public int getMaxPendingJoins() {
        return maxPendingJoins;
    }

    public boolean isSharedIcePort() {
        return sharedIcePort;
    }

    public List<InetSocketAddress> getAdvertisedAddresses() {
        return advertisedAddresses;
    }

    public boolean isInferPeerCandidates() {
        return inferPeerCandidates;
    }

    public TrustedProxies getTrustedProxies() {
        return trustedProxies;
    }

    public boolean isPreferProxiedAddress() {
        return preferProxiedAddress;
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AdvertisedAddresses;
import io.github.sendablemetatype.netty.channel.nethernet.ice.StunObserver;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinRefusal;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.github.sendablemetatype.netty.util.nethernet.ServerTrust;
import io.netty.channel.ChannelOption;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

/**
 * The NetherNet channel options. Each channel config takes the ones that act on its channel and
 * refuses every other with an {@link IllegalArgumentException}, as it refuses a value out of an
 * option's range: options named {@code NETHER_SERVER_*} go on the server channel
 * ({@code ServerBootstrap.option}), {@code NETHER_CLIENT_*} on a client channel. The connection
 * options ({@link #NETHER_MAX_REASSEMBLED_BYTES}, {@link #NETHER_SEND_PAUSE_BYTES},
 * {@link #NETHER_MAX_QUEUED_BYTES}, {@link #NETHER_METRICS}) go on a client channel or on a server's
 * children ({@code ServerBootstrap.childOption}); {@link #NETHER_ICE_SERVERS} and
 * {@link #NETHER_CANDIDATE_POLICY} on a client channel or the server channel, whose children's
 * peers use them.
 */
public class NetherChannelOption<T> extends ChannelOption<T> {

    /**
     * The identity that signs every answer of a server channel, on every join source (ruling
     * C-7). Required: binding fails without one. Use
     * {@link ServerIdentity#loadOrCreate} to keep one key across restarts, or
     * {@link ServerIdentity#ephemeral} to accept that plain HTTP players are asked to trust the
     * server again after every restart.
     */
    public static final ChannelOption<ServerIdentity> NETHER_SERVER_IDENTITY =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_IDENTITY");

    /**
     * Checks the identity assertion of every offer a server channel admits, whatever its source.
     * Defaults to {@link OfferCheck#minecraft()}. Read when the channel binds, which also starts
     * fetching the trusted issuers' keys.
     */
    public static final ChannelOption<OfferCheck> NETHER_SERVER_OFFER_CHECK =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_OFFER_CHECK");

    /**
     * How long a server channel waits for an offer's identity check, in milliseconds, 10,000 by
     * default, at least 1. A join whose check takes longer is refused with
     * {@link JoinRefusal#NO_ANSWER_IN_TIME}, and a check that finishes later admits nothing.
     */
    public static final ChannelOption<Integer> NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS");

    /**
     * Limits the joins one address may start and the connections it may hold, on every join
     * source that knows the client's address. Defaults to a {@link DefaultNetherServerThrottle}
     * of its own per server channel.
     */
    public static final ChannelOption<NetherServerThrottle> NETHER_SERVER_THROTTLE =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_THROTTLE");

    /**
     * The most connections a server channel holds at once, over all join sources, from admission
     * until each connection's teardown completed. Unlimited by default; at least 1.
     */
    public static final ChannelOption<Integer> NETHER_SERVER_MAX_SESSIONS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_MAX_SESSIONS");

    /**
     * The most joins a server channel holds at once that have not gone active, over all join
     * sources: those waiting for their identity check, negotiating, or still tearing down their
     * peer. 256 by default, at least 1.
     */
    public static final ChannelOption<Integer> NETHER_SERVER_MAX_PENDING_JOINS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_MAX_PENDING_JOINS");

    /**
     * Whether every peer of a server channel gathers its host candidates on one UDP port, at the
     * address and port the channel binds. True by default. False gives each session ports of its
     * own, from {@link #NETHER_CANDIDATE_POLICY}'s window, the way BDS allocates them.
     */
    public static final ChannelOption<Boolean> NETHER_SERVER_SHARED_ICE_PORT =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_SHARED_ICE_PORT");

    /**
     * Addresses a server channel's answers announce, as {@link AdvertisedAddresses#parse} reads
     * them: IP literals or unresolved names, port 0 for the media port. Names are looked up once,
     * off the event loop, when the channel binds; until they resolve, answers announce the
     * literals only, and a name that does not resolve closes the channel. At most
     * {@link AdvertisedAddresses#MAX_ENDPOINTS}. Gathered ones narrow the
     * host candidates to them, others are announced as the public side of a NAT. Empty by
     * default: answers carry what ICE gathered and the address the player typed.
     */
    public static final ChannelOption<List<InetSocketAddress>> NETHER_SERVER_ADVERTISED_ADDRESSES =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_ADVERTISED_ADDRESSES");

    /**
     * STUN servers a server channel asks for this host's public address from its shared ICE port,
     * as numeric addresses or unresolved names with a port ({@link StunObserver#parseServer} reads
     * {@code stun:} URLs). Each answer then announces the fresh mappings as server reflexive
     * candidates, the way an advertised address that this host does not hold is announced. Empty
     * by default: no discovery.
     * <p>
     * The servers are asked in the background, every 15 seconds, which also keeps the NAT mapping
     * open, so no join waits for them; names are looked up off the event loop, and again after
     * {@link #NETHER_SERVER_STUN_MAX_AGE} without a mapping. A mapping is announced until that age
     * after its last answer, or until the server reports another. A mapping on another port than
     * the ICE port is announced on the ICE port too, for a router that moved the port on the way
     * out but forwards the ICE port itself; without such a forward that costs a peer one failed
     * check.
     * <p>
     * Binding fails when this is set together with {@link #NETHER_SERVER_ADVERTISED_ADDRESSES}, with
     * PROXY protocol relays in front of the ICE port, or without the shared ICE port: a port window
     * has no fixed port whose mapping could be learned.
     */
    public static final ChannelOption<List<InetSocketAddress>> NETHER_SERVER_STUN_SERVERS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_STUN_SERVERS");

    /**
     * How long a mapping {@link #NETHER_SERVER_STUN_SERVERS} found is still announced after its
     * last answer, as long as the server reports no other. 5 minutes by default, longer than zero.
     */
    public static final ChannelOption<Duration> NETHER_SERVER_STUN_MAX_AGE =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_STUN_MAX_AGE");

    /**
     * Whether a server channel checks guessed candidates at the address a client signaled from,
     * when the client offers only host candidates and this host gathered no public one. True by
     * default.
     */
    public static final ChannelOption<Boolean> NETHER_SERVER_INFER_PEER_CANDIDATES =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_INFER_PEER_CANDIDATES");

    /**
     * The proxies a server channel trusts, parsed once at startup with
     * {@link TrustedProxies#parseList}. The PROXY protocol list also names the relays whose PROXY
     * protocol version 2 headers the shared ICE port reads; answers then announce only the relays
     * and TURN, and no remote candidate is applied. HTTP signaling reads its own list
     * ({@code NetherNetHttpSignaling.Builder#trustedProxies}); give it the same parsed set.
     * {@link TrustedProxies#NONE} by default.
     */
    public static final ChannelOption<TrustedProxies> NETHER_SERVER_TRUSTED_PROXIES =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_TRUSTED_PROXIES");

    /**
     * Whether a child channel keeps reporting the client a trusted signaling proxy declared,
     * rather than the selected ICE pair. A TURN pair is reported as the relay either way. False by
     * default.
     */
    public static final ChannelOption<Boolean> NETHER_SERVER_PREFER_PROXIED_ADDRESS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_PREFER_PROXIED_ADDRESS");

    /**
     * STUN and TURN servers every peer of a server or client channel uses, in
     * addition to the ones its signaling hands out. Empty by default.
     */
    public static final ChannelOption<List<IceServer>> NETHER_ICE_SERVERS =
            valueOf(NetherChannelOption.class, "NETHER_ICE_SERVERS");

    /**
     * Where the peers of a server or client channel gather their own
     * candidates: IPv6, local addresses and the port window. Defaults to
     * {@link CandidatePolicy#DEFAULT}.
     */
    public static final ChannelOption<CandidatePolicy> NETHER_CANDIDATE_POLICY =
            valueOf(NetherChannelOption.class, "NETHER_CANDIDATE_POLICY");

    /**
     * The largest message accepted from the remote peer, 16 MiB by default, at
     * least 1 byte. A larger one ends the connection with
     * {@link TransportDisconnectReason#SPLIT_QUEUE_TOO_LONG}. The inbound queue
     * holds up to twice this many bytes of whole messages that wait for the
     * application.
     */
    public static final ChannelOption<Integer> NETHER_MAX_REASSEMBLED_BYTES =
            valueOf(NetherChannelOption.class, "NETHER_MAX_REASSEMBLED_BYTES");

    /**
     * How many bytes may wait in a data channel's engine queue before the
     * channel stops handing segments to the engine, 2 MiB by default, at least
     * 1. Segments
     * then wait in Java and the channel turns unwritable until the engine queue
     * drained to a quarter of this. A change applies from the next pause.
     */
    public static final ChannelOption<Integer> NETHER_SEND_PAUSE_BYTES =
            valueOf(NetherChannelOption.class, "NETHER_SEND_PAUSE_BYTES");

    /**
     * How many bytes may wait in Java while the engine queue is full, 64 MiB by
     * default, at least 1. Past it the connection ends with
     * {@link TransportDisconnectReason#QUEUE_TOO_LONG}.
     */
    public static final ChannelOption<Integer> NETHER_MAX_QUEUED_BYTES =
            valueOf(NetherChannelOption.class, "NETHER_MAX_QUEUED_BYTES");

    /** Per connection counters and events, or null (the default) for none. */
    public static final ChannelOption<NetherChannelMetrics> NETHER_METRICS =
            valueOf(NetherChannelOption.class, "NETHER_METRICS");

    /** A server channel's own counters and events, or null (the default) for none. */
    public static final ChannelOption<NetherServerMetrics> NETHER_SERVER_METRICS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_METRICS");

    /**
     * How long one client attempt may take to open the data channels, in milliseconds: 3,000 by
     * default, at least 1. Over signaling that waits for its server's answers itself (HTTP), the
     * attempt also gets that wait until the answer arrived.
     */
    public static final ChannelOption<Integer> NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS =
            valueOf(NetherChannelOption.class, "NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS");

    /**
     * How many attempts a client connect gets before it fails, the first one included: 3 by
     * default, at least 1. An attempt that does not finish within
     * {@link #NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS}, or whose peer fails, starts over with a new peer
     * and connection ID. Signaling that does not trickle (HTTP) gets one attempt whatever this
     * says: it carries one offer, and the server refuses a second one for the same network ID as a
     * duplicate while the first is still pending.
     */
    public static final ChannelOption<Integer> NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS =
            valueOf(NetherChannelOption.class, "NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS");

    /**
     * The identity a client channel presents in its offer, or null (the default) for an offer
     * without one, which vanilla BDS and this library's servers refuse. Tokens expire, so set a
     * fresh identity per connection.
     */
    public static final ChannelOption<ClientIdentity> NETHER_CLIENT_IDENTITY =
            valueOf(NetherChannelOption.class, "NETHER_CLIENT_IDENTITY");

    /**
     * The server identities a client channel accepts, or null (the default) to leave the answer's
     * identity unchecked. When set, the {@code a=identity} of the server's answer must name a key the trust
     * pins, be self-signed by it, carry a valid signature over the answer's DTLS fingerprints, and
     * not be expired. An answer without an identity, with one that does not verify, or with a key
     * that is not pinned fails the connect at once, without another attempt, since every attempt
     * meets the same server. See {@link ServerTrust#pinnedTo}.
     */
    public static final ChannelOption<ServerTrust> NETHER_CLIENT_SERVER_TRUST =
            valueOf(NetherChannelOption.class, "NETHER_CLIENT_SERVER_TRUST");

    /**
     * How long a server's child may take from its admission to going active, in seconds: 30 by
     * default, at least 1. A child that is not active by then is closed.
     */
    public static final ChannelOption<Integer> NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS =
            valueOf(NetherChannelOption.class, "NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS");

    @SuppressWarnings("deprecation")
    protected NetherChannelOption(String name) {
        super(name);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.ice.AdvertisedAddresses;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferFixtures;
import io.github.sendablemetatype.netty.util.nethernet.OfferPolicy;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * An endpoint on the fake engine and one event loop, which the server and its children share, so
 * {@link #sync()} lets everything queued so far run. Offers are checked against the stand-in
 * Minecraft key, without a network fetch.
 */
final class EndpointHarness implements AutoCloseable {
    /** An offer without an identity assertion. */
    static final String OFFER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:data\r\n"
            + "a=max-message-size:262144\r\n";
    /** An answer as the engine creates it, before gathering. */
    static final String ANSWER = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\n"
            + "a=fingerprint:sha-256 " + OfferFixtures.DIGEST + "\r\na=mid:data\r\n";
    static final OfferPolicy ADMIT_ALL = result -> Optional.empty();
    static final ServerIdentity IDENTITY = ServerIdentity.ephemeral("test");

    final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final FakeEngine engine = new FakeEngine();
    final LinkedBlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    final ServerBootstrap bootstrap;
    NetherNetServerChannel server;

    EndpointHarness() {
        this(OfferCheck::newExecutor);
    }

    EndpointHarness(Supplier<? extends ExecutorService> checkExecutors) {
        this(checkExecutors, AdvertisedAddresses::resolve);
    }

    EndpointHarness(Supplier<? extends ExecutorService> checkExecutors, NetherNetServerChannel.AdvertisedLookup names) {
        bootstrap = new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(EngineRef.shared(engine), checkExecutors, names))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, IDENTITY)
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, OfferFixtures.retailCheck())
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel child) {
                        children.add((NetherNetChildChannel) child);
                    }
                });
    }

    NetherNetServerChannel bind() throws InterruptedException {
        server = (NetherNetServerChannel) bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        return server;
    }

    /** A source on the bound endpoint whose joins pass any identity check. */
    FakeJoinSource source(boolean trickles) {
        return source(trickles, ADMIT_ALL);
    }

    FakeJoinSource source(boolean trickles, OfferPolicy policy) {
        FakeJoinSource source = new FakeJoinSource(trickles);
        server.addSource(source, policy);
        return source;
    }

    /** Lets everything queued on the event loop so far run. */
    void sync() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            group.next().submit(() -> { }).sync();
        }
    }

    /** The next child the endpoint exposed. */
    NetherNetChildChannel child() throws InterruptedException {
        NetherNetChildChannel child = children.poll(2, TimeUnit.SECONDS);
        assertNotNull(child, "no child was exposed");
        return child;
    }

    /** Waits until the newest peer was asked to answer. */
    FakePeer negotiating() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (!engine.peers().isEmpty() && !engine.lastPeer().remoteOffers().isEmpty()) {
                return engine.lastPeer();
            }
            Thread.sleep(5);
        }
        throw new AssertionError("The child never negotiated");
    }

    /** The client opens both data channels and both open, which activates the child. */
    void activate(FakePeer peer) throws InterruptedException {
        FakeDataLink reliable = peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        FakeDataLink unreliable = peer.remoteChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        peer.open(reliable);
        peer.open(unreliable);
        sync();
    }

    @Override
    public void close() {
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }
}

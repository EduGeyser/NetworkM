package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.WebRtcEngine;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** STUN discovery on the shared ICE port of a real libwebrtc endpoint (ruling G-6, item 1). */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
@Timeout(60)
class NetherNetStunNativeTest {
    /** The public mapping the test STUN server reports, on another port than the ICE port. */
    private static final InetSocketAddress MAPPED = new InetSocketAddress("93.184.216.40", 45678);

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final NetherNetEngine engine = WebRtcEngine.builder().factories(1).build();
    private Channel server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        engine.close().toCompletableFuture().get(20, TimeUnit.SECONDS);
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void answersOfARealJoinAnnounceTheMappingStunReportsForThePort() throws Exception {
        try (TestStunServer stun = new TestStunServer(InetAddress.getLoopbackAddress(), MAPPED)) {
            server = new ServerBootstrap()
                    .group(group)
                    .channelFactory(() -> new NetherNetServerChannel(engine))
                    .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                    .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                    .option(NetherChannelOption.NETHER_SERVER_STUN_SERVERS, List.of(stun.address()))
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel child) {
                        }
                    })
                    .bind(new InetSocketAddress("0.0.0.0", 0)).sync().channel();
            int icePort = ((InetSocketAddress) server.localAddress()).getPort();
            FakeJoinSource source = new FakeJoinSource(false);
            ((NetherNetServerChannel) server).addSource(source, EndpointHarness.ADMIT_ALL);
            String offer = clientOffer();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            String answer = "";
            for (long id = 1; !answer.contains(" typ srflx") && System.nanoTime() < deadline; id++) {
                answer = source.offer(Long.toString(id), offer).answer();
                Thread.sleep(200);
            }

            assertTrue(stun.answered() > 0, "the engine asked the server from the shared port");
            assertTrue(answer.contains(" 93.184.216.40 45678 typ srflx"), answer);
            assertTrue(answer.contains(" 93.184.216.40 " + icePort + " typ srflx"), "and its ICE port twin: " + answer);
        }
    }

    /** A real client's full offer. */
    private String clientOffer() throws Exception {
        Peer client = engine.createPeer(new PeerConfig(PeerConfig.Role.OFFERER, List.of(), CandidatePolicy.DEFAULT,
                null, null, null), new Ignored());
        client.openChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        client.openChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        client.offer().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return client.gatheredLocalDescription().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static final class Ignored implements PeerListener {
        @Override
        public void onLocalCandidate(String candidate, String mid) {
        }

        @Override
        public void onGatheringComplete() {
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            return false;
        }

        @Override
        public void onChannelOpen(DataLink link) {
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
        }

        @Override
        public void onQueueLow(DataLink link) {
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
        }

        @Override
        public void onPathSelected(TransportPath path) {
        }

        @Override
        public void onStateChange(PeerState state) {
        }

        @Override
        public void onIceStateChange(IceState state) {
        }

        @Override
        public void onClosed(CloseCause cause) {
        }
    }
}

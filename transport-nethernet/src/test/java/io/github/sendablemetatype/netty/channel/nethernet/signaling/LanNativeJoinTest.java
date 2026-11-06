package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetClientChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.WebRtcEngine;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.educationClaims;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.educationToken;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.keyPair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A client channel joining a server endpoint over LAN discovery on libwebrtc, the way a client
 * joins an entry of its LAN list: the discovery request and response, a trickled offer signed with
 * the client's identity, the answer, data both ways. Discovery runs over UDP loopback; runs where
 * the build provides webrtc-java natives. Upstream's equivalent is {@code LanDiscoveryJoinTest}.
 */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
@Timeout(60)
class LanNativeJoinTest {
    private static final long WAIT_SECONDS = 20;

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final NetherNetEngine engine = WebRtcEngine.builder().factories(2).build();
    private final CompletableFuture<InetSocketAddress> discoveryBound = new CompletableFuture<>();
    private final BlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> serverReads = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> clientReads = new LinkedBlockingQueue<>();
    private Channel server;
    private Channel client;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close().syncUninterruptibly();
        }
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        engine.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aClientJoinsOverLanWithAProvenKeyAndExchangesData() throws Exception {
        server = new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                // Proves keys and trusts no issuer, so nothing is fetched
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel child) {
                        children.add((NetherNetChildChannel) child);
                        child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object message) {
                                ByteBuf data = (ByteBuf) message;
                                serverReads.add(data.toString(StandardCharsets.UTF_8));
                                ctx.writeAndFlush(data);
                            }
                        });
                    }
                })
                .bind(new InetSocketAddress("0.0.0.0", 0)).sync().channel();
        NetherNetDiscovery serverDiscovery = new NetherNetDiscovery(1, () -> new MultiThreadIoEventLoopGroup(1,
                NioIoHandler.newFactory())) {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                discoveryBound.complete((InetSocketAddress) ctx.channel().localAddress());
                super.channelActive(ctx);
            }
        };
        NetherNetDiscoverySignaling lan = new NetherNetDiscoverySignaling(1, new InetSocketAddress("127.0.0.1", 0),
                serverDiscovery, 10_000);
        lan.setServerData(new LanServerData.V7("Server", 2225, "1.26.62", "World", 0, 10, 0, false, false, true,
                false, "abc", 4));
        ((NetherNetServerChannel) server).addSource(lan);

        KeyPair key = keyPair("EC", "secp384r1");
        client = new Bootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetClientChannel(engine,
                        new NetherNetDiscoverySignaling(2, new InetSocketAddress("127.0.0.1", 0))))
                .option(NetherChannelOption.NETHER_CLIENT_IDENTITY,
                        ClientIdentity.of(key, educationToken(educationClaims(key.getPublic()), key), ""))
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object message) {
                        try {
                            clientReads.add(((ByteBuf) message).toString(StandardCharsets.UTF_8));
                        } finally {
                            ReferenceCountUtil.release(message);
                        }
                    }
                })
                .connect(discoveryBound.get(WAIT_SECONDS, TimeUnit.SECONDS)).sync().channel();

        NetherNetChildChannel child = children.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(child);
        assertEquals(OfferCheckResult.Level.KEY_PROVEN, child.getOfferCheck().level(),
                "the LAN offer carries an assertion whose key is proven");
        assertEquals(TransportIdentityBinding.State.PENDING, TransportIdentityBinding.state(child));
        assertEquals(TransportIdentityBinding.Result.ACCEPTED, TransportIdentityBinding.verifyLogin(child,
                key.getPublic()));

        client.writeAndFlush(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8)).sync();
        assertEquals("ping", serverReads.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("ping", clientReads.poll(WAIT_SECONDS, TimeUnit.SECONDS), "the server's echo");
    }
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.WebRtcEngine;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSink;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.JoinSource;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetHttpClientSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetHttpSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalingHealth;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.HttpClientSettings;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.educationClaims;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.educationToken;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.keyPair;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A client channel joining a server endpoint through HTTP signaling on libwebrtc, as a proxy joins
 * a server behind it: the probe, a full-ICE offer signed with the client's identity, the answer,
 * data both ways and the reason the client gets when the server ends the connection. Signaling
 * runs over TCP loopback; runs where the build provides webrtc-java natives.
 */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
@Timeout(60)
class NetherNetHttpNativeTest {
    private static final long WAIT_SECONDS = 20;

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final NetherNetEngine engine = WebRtcEngine.builder().factories(2).build();
    private final BlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> offers = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> serverReads = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> clientReads = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> clientEvents = new LinkedBlockingQueue<>();
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
    void aClientJoinsThroughHttpWithASignedFullIceOffer() throws Exception {
        NetherNetHttpSignaling http = bindServer(ServerIdentity.ephemeral("test"));
        client = connect(http, bootstrap -> { }).sync().channel();

        String offer = offers.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(offer);
        assertTrue(offer.contains("a=identity:"), offer);
        assertTrue(offer.contains("a=candidate:") && offer.contains("a=end-of-candidates"),
                "HTTP carries every candidate in the offer: " + offer);
        NetherNetChildChannel child = children.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(child);
        assertEquals(OfferCheckResult.Level.KEY_PROVEN, child.getOfferCheck().level());

        echo();

        child.close().sync();
        Object reason = clientEvents.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertTrue(EnumSet.of(TransportDisconnectReason.CLOSED_BY_REMOTE_PEER,
                TransportDisconnectReason.ABORTED_BY_REMOTE_PEER).contains(reason), String.valueOf(reason));
        assertEquals("inactive", clientEvents.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void bothEndsReportTheRealPairAddressesAndMetrics() throws Exception {
        NetherNetHttpSignaling http = bindServer(ServerIdentity.ephemeral("test"));
        NetherNetClientChannel connected = (NetherNetClientChannel) connect(http, bootstrap -> { })
                .sync().channel();
        client = connected;
        NetherNetChildChannel child = children.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(child);
        echo();

        TransportPath clientPath = connected.selectedPath().orElseThrow();
        TransportPath childPath = child.selectedPath().orElseThrow();
        assertNotNull(clientPath.local());
        assertNotNull(childPath.local());
        assertEquals(clientPath.remote(), childPath.local(), "one pair seen from both ends");
        assertEquals(childPath.remote(), clientPath.local());

        assertEquals(new SourcedAddress(http.boundAddress(), AddressSource.SIGNALING, null),
                connected.signaledAddress());
        assertEquals(new SourcedAddress(clientPath.remote(), AddressSource.ICE, null), connected.reportedAddress());
        assertEquals(AddressSource.SIGNALING, child.signaledAddress().source(), "the HTTP connection");
        assertEquals(new SourcedAddress(childPath.remote(), AddressSource.ICE, null),
                child.mediaAddress().orElseThrow());
        assertEquals(child.mediaAddress().orElseThrow(), child.reportedAddress());
        assertEquals(childPath.remote(), child.remoteAddress());

        for (NetherNetChannel channel : List.of(connected, child)) {
            TransportMetrics metrics = channel.transportMetrics().toCompletableFuture()
                    .get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertTrue(metrics.iceRoundTripTime().isPresent(), "ICE measured the pair: " + metrics);
            TransportMetrics.Sctp sctp = metrics.sctp().orElseThrow();
            assertTrue(sctp.messagesSent() >= 1 && sctp.messagesReceived() >= 1, "the echo went both ways: " + sctp);
            assertTrue(sctp.packetsSent() >= sctp.messagesSent() && sctp.congestionWindowBytes() > 0, sctp.toString());
        }

        child.close().sync();
        assertEquals(TransportMetrics.NONE, child.transportMetrics().toCompletableFuture().get());
        assertEquals(Optional.of(childPath), child.selectedPath(), "the pair stays readable after close");
    }

    /** Binds an endpoint with {@code identity} and its HTTP signaling on loopback. */
    private NetherNetHttpSignaling bindServer(ServerIdentity identity) throws Exception {
        server = new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, identity)
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
        NetherNetHttpSignaling http = NetherNetHttpSignaling.builder(group).build();
        http.bind(new InetSocketAddress("127.0.0.1", 0));
        ((NetherNetServerChannel) server).addSource(new RecordingSource(http, offers::add));
        return http;
    }

    /** Connects a client with an Education-shaped identity through the server's HTTP signaling. */
    private ChannelFuture connect(NetherNetHttpSignaling http, Consumer<Bootstrap> options) throws Exception {
        KeyPair key = keyPair("EC", "secp384r1");
        NetherNetHttpClientSignaling signaling = new NetherNetHttpClientSignaling(HttpClientSettings.DEFAULT, group);
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetClientChannel(engine, signaling))
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

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                        clientEvents.add(event);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) {
                        clientEvents.add("inactive");
                    }
                });
        options.accept(bootstrap);
        return bootstrap.connect(http.boundAddress());
    }

    /** Sends one message from the client and waits for the server's echo. */
    private void echo() throws InterruptedException {
        client.writeAndFlush(Unpooled.copiedBuffer("ping", StandardCharsets.UTF_8)).sync();
        assertEquals("ping", serverReads.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("ping", clientReads.poll(WAIT_SECONDS, TimeUnit.SECONDS), "the server's echo");
    }

    /** The HTTP source, handing on its joins and keeping the offer of each. */
    private record RecordingSource(NetherNetHttpSignaling http, Consumer<String> offers) implements JoinSource {
        @Override
        public void start(JoinSink sink) {
            http.start(new JoinSink() {
                @Override
                public void join(Join join) {
                    offers.accept(join.offer());
                    sink.join(join);
                }

                @Override
                public boolean admits(SourcedAddress source) {
                    return sink.admits(source);
                }
            });
        }

        @Override
        public SignalingHealth health() {
            return http.health();
        }

        @Override
        public void addHealthListener(Consumer<SignalingHealth> listener) {
            http.addHealthListener(listener);
        }

        @Override
        public void removeHealthListener(Consumer<SignalingHealth> listener) {
            http.removeHealthListener(listener);
        }

        @Override
        public List<IceServer> iceServers() {
            return http.iceServers();
        }

        @Override
        public void close() {
            http.close();
        }
    }
}

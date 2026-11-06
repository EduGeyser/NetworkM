package io.github.sendablemetatype.netty;

import io.github.sendablemetatype.netty.channel.raknet.RakChannelFactory;
import io.github.sendablemetatype.netty.channel.raknet.RakChildChannel;
import io.github.sendablemetatype.netty.channel.raknet.RakClientChannel;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelMetrics;
import io.github.sendablemetatype.netty.channel.raknet.config.RakChannelOption;
import io.github.sendablemetatype.netty.channel.raknet.config.RakServerMetrics;
import io.github.sendablemetatype.netty.channel.raknet.packet.RakMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.ID_CONNECTION_REQUEST;
import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.ID_NEW_INCOMING_CONNECTION;
import static org.junit.jupiter.api.Assertions.*;

/** A client and server over loopback UDP whose metrics hooks throw: connections and messages still go through. */
@Timeout(20)
class RakMetricsFailureTests {
    private static final byte[] MESSAGE = {(byte) 0xFE, 1, 2, 3};

    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final CountDownLatch childActive = new CountDownLatch(1);
    private Channel server;
    private Channel client;

    @AfterEach
    void close() {
        if (client != null) {
            client.close().awaitUninterruptibly();
        }
        if (server != null) {
            server.close().awaitUninterruptibly();
        }
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }

    @Test
    void aThrowingConnectionPacketHookStillLetsTheClientConnect() throws Exception {
        Set<Integer> packets = ConcurrentHashMap.newKeySet();
        bind(new RakServerMetrics() {
            @Override
            public void connectionInitPacket(InetSocketAddress address, int packetId) {
                packets.add(packetId);
                throw new IllegalStateException("Metrics unavailable");
            }
        }, child -> {
        });

        connect(null);

        assertTrue(childActive.await(5, TimeUnit.SECONDS), "The server child goes active");
        assertTrue(packets.containsAll(Set.of((int) ID_CONNECTION_REQUEST, (int) ID_NEW_INCOMING_CONNECTION)), packets.toString());
    }

    @Test
    void throwingServerHooksDoNotBreakAConnection() throws Exception {
        ThrowingMetrics<RakServerMetrics> metrics = ThrowingMetrics.of(RakServerMetrics.class);
        bind(metrics.hooks(), child -> {
        });

        assertEcho(connect(null));

        // The child's own hooks hand these to the server's through DefaultChannelToServerProxyMetrics
        assertTrue(metrics.called().containsAll(Set.of("bytesIn", "bytesOut", "rakDatagramsIn", "rakDatagramsOut",
                "encapsulatedIn", "encapsulatedOut", "stateChange")), metrics.called().toString());
    }

    @Test
    void throwingChildHooksDoNotBreakTheChild() throws Exception {
        ThrowingMetrics<RakChannelMetrics> metrics = ThrowingMetrics.of(RakChannelMetrics.class);
        CountDownLatch installed = new CountDownLatch(1);
        bind(null, child -> {
            child.config().setOption(RakChannelOption.RAK_METRICS, metrics.hooks());
            installed.countDown();
        });

        CompletableFuture<byte[]> echo = connect(null);
        assertTrue(installed.await(5, TimeUnit.SECONDS));
        assertEcho(echo);

        // bytesIn from RakServerRouteHandler, bytesOut from RakChildDatagramHandler
        assertTrue(metrics.called().containsAll(Set.of("bytesIn", "bytesOut")), metrics.called().toString());
    }

    @Test
    void throwingClientHooksDoNotBreakTheClient() throws Exception {
        ThrowingMetrics<RakChannelMetrics> metrics = ThrowingMetrics.of(RakChannelMetrics.class);
        bind(null, child -> {
        });

        assertEcho(connect(metrics.hooks()));

        // bytesIn and bytesOut from RakClientProxyRouteHandler
        assertTrue(metrics.called().containsAll(Set.of("bytesIn", "bytesOut")), metrics.called().toString());
    }

    private void bind(RakServerMetrics metrics, Consumer<RakChildChannel> configure) {
        server = new ServerBootstrap()
                .channelFactory(RakChannelFactory.server(NioDatagramChannel.class))
                .group(group)
                .option(RakChannelOption.RAK_SUPPORTED_PROTOCOLS, new int[]{11})
                .option(RakChannelOption.RAK_SERVER_METRICS, metrics)
                .childHandler(new ChannelInitializer<RakChildChannel>() {
                    @Override
                    protected void initChannel(RakChildChannel channel) {
                        configure.accept(channel);
                        channel.pipeline().addLast(new SimpleChannelInboundHandler<RakMessage>() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                childActive.countDown();
                                ctx.fireChannelActive();
                            }

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, RakMessage message) {
                                ctx.writeAndFlush(new RakMessage(message.content().retain()));
                            }
                        });
                    }
                })
                .bind(new InetSocketAddress("127.0.0.1", 0)).syncUninterruptibly().channel();
    }

    /** Connects a client and returns the first message it receives. */
    private CompletableFuture<byte[]> connect(RakChannelMetrics metrics) {
        CompletableFuture<byte[]> received = new CompletableFuture<>();
        client = new Bootstrap()
                .channelFactory(RakChannelFactory.client(NioDatagramChannel.class))
                .group(group)
                .option(RakChannelOption.RAK_PROTOCOL_VERSION, 11)
                .option(RakChannelOption.RAK_METRICS, metrics)
                .handler(new ChannelInitializer<RakClientChannel>() {
                    @Override
                    protected void initChannel(RakClientChannel channel) {
                        channel.pipeline().addLast(new SimpleChannelInboundHandler<RakMessage>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, RakMessage message) {
                                received.complete(ByteBufUtil.getBytes(message.content()));
                            }
                        });
                    }
                })
                .connect(server.localAddress()).awaitUninterruptibly().channel();
        assertTrue(client.isActive(), "The client connects");
        return received;
    }

    private void assertEcho(CompletableFuture<byte[]> received) throws Exception {
        client.writeAndFlush(new RakMessage(Unpooled.wrappedBuffer(MESSAGE))).syncUninterruptibly();
        assertArrayEquals(MESSAGE, received.get(5, TimeUnit.SECONDS));
    }
}

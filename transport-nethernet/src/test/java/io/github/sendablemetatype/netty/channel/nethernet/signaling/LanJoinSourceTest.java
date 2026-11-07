package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.util.nethernet.OfferFixtures;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class LanJoinSourceTest {
    private static final long SERVER_ID = 1;
    private static final long CLIENT_ID = 20;
    private static final long OTHER_ID = 30;

    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final CompletableFuture<InetSocketAddress> bound = new CompletableFuture<>();
    private final NetherNetDiscovery discovery = new NetherNetDiscovery(SERVER_ID, () -> group) {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            bound.complete((InetSocketAddress) ctx.channel().localAddress());
            super.channelActive(ctx);
        }
    };
    private final NetherNetDiscoverySignaling source = new NetherNetDiscoverySignaling(SERVER_ID,
            new InetSocketAddress("127.0.0.1", 0), discovery, 10_000);
    private final RecordingSink sink = new RecordingSink();
    private final DatagramSocket client;

    LanJoinSourceTest() throws Exception {
        client = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        client.setSoTimeout(2000);
    }

    @AfterEach
    void tearDown() {
        source.close();
        client.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void anOfferBecomesAJoinThatRepliesToWhereItCameFrom() throws Exception {
        CompletableFuture<SignalingHealth> connected = new CompletableFuture<>();
        source.addHealthListener(health -> {
            if (health.state() == SignalingState.CONNECTED) {
                connected.complete(health);
            }
        });
        source.start(sink);
        connected.get(2, TimeUnit.SECONDS);

        send("CONNECTREQUEST 42 v=0");
        Join join = awaitJoin(1);
        assertEquals("42", join.connectionId());
        assertEquals(Long.toUnsignedString(CLIENT_ID), join.remoteNetworkId());
        assertEquals("v=0", join.offer());
        assertEquals(new SourcedAddress((InetSocketAddress) client.getLocalSocketAddress(),
                AddressSource.SIGNALING_LAN, null), join.source());
        assertTrue(source.iceServers().isEmpty());

        join.reply().answer("v=0 answer").toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals("CONNECTRESPONSE 42 v=0 answer", receive());
        join.reply().refuse(JoinRefusal.IDENTITY_MISSING);
        assertEquals("CONNECTERROR 42 16", receive());
    }

    @Test
    void theClientsSignalsReachTheJoinUntilItFinishes() throws Exception {
        source.start(sink);
        bound.get(2, TimeUnit.SECONDS);
        send("CONNECTREQUEST 42 v=0");
        Join join = awaitJoin(1);
        send("CANDIDATEADD 42 candidate:1 1 udp 1 192.168.1.20 5000 typ host");
        send("CONNECTREQUEST 42 v=0");
        List<String> signals = RecordingSink.signalsOf(join);
        send("CONNECTERROR 42 2");
        awaitSize(signals, 2);
        assertEquals(List.of("candidate candidate:1 1 udp 1 192.168.1.20 5000 typ host", "error 2"), signals);
        assertEquals(1, sink.delivered.size(), "A repeated offer is no new join");

        join.reply().finished();
        send("CANDIDATEADD 42 late");
        send("CONNECTREQUEST 42 retry");
        Join retry = awaitJoin(2);
        assertEquals("retry", retry.offer());
        assertEquals(2, signals.size());
    }

    @Test
    void aJoinTakesSignalsOnlyFromTheClientAndAddressItsOfferCameFrom() throws Exception {
        source.start(sink);
        bound.get(2, TimeUnit.SECONDS);
        try (DatagramSocket other = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0))) {
            send("CONNECTREQUEST 42 v=0");
            Join join = awaitJoin(1);
            List<String> signals = RecordingSink.signalsOf(join);

            send(other, OTHER_ID, "CONNECTREQUEST 42 other");
            Join otherJoin = awaitJoin(2);
            assertEquals("other", otherJoin.offer(), "another client's offer with the same ID is a join of its own");
            List<String> otherSignals = RecordingSink.signalsOf(otherJoin);
            send(other, OTHER_ID, "CANDIDATEADD 42 candidate:2 1 udp 1 192.168.1.30 5000 typ host");
            send(other, OTHER_ID, "CONNECTERROR 42 2");
            send(other, CLIENT_ID, "CONNECTERROR 42 3");
            send("CANDIDATEADD 42 candidate:1 1 udp 1 192.168.1.20 5000 typ host");

            awaitSize(signals, 1);
            assertEquals(List.of("candidate candidate:1 1 udp 1 192.168.1.20 5000 typ host"), signals,
                    "neither the other client nor its address reaches the join");
            assertEquals(List.of("candidate candidate:2 1 udp 1 192.168.1.30 5000 typ host", "error 2"), otherSignals);

            otherJoin.reply().finished();
            send("CONNECTERROR 42 2");
            awaitSize(signals, 2);
            assertEquals("error 2", signals.get(1), "the other join's end leaves this one routed");
        }
    }

    @Test
    void aConnectionIdIsMatchedAndEchoedExactlyAsWritten() throws Exception {
        source.start(sink);
        bound.get(2, TimeUnit.SECONDS);
        send("CONNECTREQUEST 00042 v=0");
        send("CONNECTREQUEST pfab-7Q:x~! v=0");
        Join opaque = awaitJoin(2);
        Join padded = sink.delivered.getFirst();
        assertEquals("00042", padded.connectionId());
        assertEquals("pfab-7Q:x~!", opaque.connectionId());

        List<String> signals = RecordingSink.signalsOf(opaque);
        send("CANDIDATEADD pfab-7Q:x~! candidate:1");
        awaitSize(signals, 1);
        padded.reply().answer("v=0 answer").toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals("CONNECTRESPONSE 00042 v=0 answer", receive());
    }

    @Test
    void aSignalWithAnInvalidConnectionIdIsDropped() throws Exception {
        source.start(sink);
        bound.get(2, TimeUnit.SECONDS);
        send("CONNECTREQUEST 4\u00072 v=0");
        send("CONNECTREQUEST " + "1".repeat(65) + " v=0");
        send("CONNECTREQUEST  v=0");
        send("CONNECTREQUEST " + "1".repeat(64) + " v=0");

        // The valid offer went last, and loopback keeps one socket's datagrams in order, so the
        // others were handled before it arrived.
        assertEquals("1".repeat(64), awaitJoin(1).connectionId());
    }

    @Test
    void anEndpointChecksTheIdentityOfLanJoins() throws Exception {
        FakeEngine engine = new FakeEngine();
        Channel endpoint = new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, OfferFixtures.retailCheck())
                .childHandler(new ChannelInboundHandlerAdapter())
                .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        try {
            ((NetherNetServerChannel) endpoint).addSource(source);

            send("CONNECTREQUEST 42 " + OfferFixtures.offer("%%%", OfferFixtures.DIGEST));
            assertEquals("CONNECTERROR 42 16", receive(), "a malformed assertion is refused");
            send("CONNECTREQUEST 43 " + OfferFixtures.educationOffer()
                    .replace(OfferFixtures.DIGEST, OfferFixtures.OTHER_DIGEST));
            assertEquals("CONNECTERROR 43 16", receive(), "a failed proof is refused");
            assertTrue(engine.peers().isEmpty());

            // Retail 1.26.63 sends its LAN offer with no a=identity.
            send("CONNECTREQUEST 44 " + OfferFixtures.offer(null, OfferFixtures.DIGEST));
            awaitPeers(engine, 1, "an offer without an assertion is admitted");
            send("CONNECTREQUEST 45 " + OfferFixtures.educationOffer());
            awaitPeers(engine, 2, "a proven key is admitted");
        } finally {
            endpoint.close().sync();
        }
    }

    private static void awaitPeers(FakeEngine engine, int count, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (engine.peers().size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(count, engine.peers().size(), message);
    }

    private void send(String signal) throws Exception {
        send(client, CLIENT_ID, signal);
    }

    /** Sends a signal from this socket, claiming this network ID. */
    private void send(DatagramSocket from, long senderId, String signal) throws Exception {
        ByteBuf packet = Unpooled.buffer().writeShortLE(NetherNetConstants.ID_DISCOVERY_MESSAGE).writeLongLE(senderId)
                .writeZero(8).writeLongLE(SERVER_ID).writeIntLE(signal.length())
                .writeBytes(signal.getBytes(StandardCharsets.UTF_8));
        try {
            byte[] encrypted = NetherNetConstants.encryptDiscoveryPacket(packet);
            from.send(new DatagramPacket(encrypted, encrypted.length, bound.get(2, TimeUnit.SECONDS)));
        } finally {
            packet.release();
        }
    }

    /** The signal in the next discovery message the client receives. */
    private String receive() throws Exception {
        DatagramPacket datagram = new DatagramPacket(new byte[65535], 65535);
        client.receive(datagram);
        ByteBuf message = NetherNetConstants.decryptDiscoveryPacket(
                Unpooled.wrappedBuffer(datagram.getData(), 0, datagram.getLength()));
        try {
            assertEquals(NetherNetConstants.ID_DISCOVERY_MESSAGE, message.readUnsignedShortLE());
            assertEquals(SERVER_ID, message.readLongLE());
            message.skipBytes(8);
            assertEquals(CLIENT_ID, message.readLongLE());
            int length = message.readIntLE();
            return message.readCharSequence(length, StandardCharsets.UTF_8).toString();
        } finally {
            message.release();
        }
    }

    private Join awaitJoin(int count) throws InterruptedException {
        awaitSize(sink.delivered, count);
        return sink.delivered.get(count - 1);
    }

    private static void awaitSize(List<?> list, int size) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (list.size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(size, list.size());
    }
}

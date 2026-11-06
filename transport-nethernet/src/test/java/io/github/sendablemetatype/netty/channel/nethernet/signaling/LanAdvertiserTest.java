package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.codec.NetherNetServerDataCodec;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertisement;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertiser;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanDiscoveryRequest;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LAN discovery answering each request from a {@link LanAdvertiser} (gap ruling: per-request LAN advertisement). */
@Timeout(15)
class LanAdvertiserTest {
    private static final long SERVER_ID = 1;
    private static final long CLIENT_ID = 20;
    private static final Duration DEADLINE = Duration.ofMillis(200);

    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final CompletableFuture<InetSocketAddress> bound = new CompletableFuture<>();
    private final NetherNetDiscovery discovery = new NetherNetDiscovery(SERVER_ID, () -> group,
            new DiscoveryPeerRegistry(), DEADLINE, null) {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            bound.complete((InetSocketAddress) ctx.channel().localAddress());
            super.channelActive(ctx);
        }
    };
    private final DatagramSocket client;

    LanAdvertiserTest() throws Exception {
        client = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        client.setSoTimeout(1000);
    }

    @AfterEach
    void tearDown() {
        discovery.close();
        client.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private static LanServerData.V7 data(int players) {
        return new LanServerData.V7("Server", 2225, "1.26.62", "World", players, 10, 0, false, false, true, false,
                "abc", 4);
    }

    @Test
    void eachRequestIsAskedWithItsRequesterAndAnsweredAnew() throws Exception {
        List<LanDiscoveryRequest> asked = new CopyOnWriteArrayList<>();
        AtomicInteger players = new AtomicInteger();
        discovery.setAdvertiser(request -> {
            asked.add(request);
            return LanAdvertisement.advertise(data(players.incrementAndGet()));
        });
        discovery.bind(new InetSocketAddress("127.0.0.1", 0));

        assertEquals(data(1), discover());
        assertEquals(data(2), discover(), "the advertiser is asked for every request");
        assertEquals(List.of(new LanDiscoveryRequest((InetSocketAddress) client.getLocalSocketAddress(),
                Long.toUnsignedString(CLIENT_ID))), asked.subList(0, 1));
    }

    @Test
    void aDeclineSendsNoResponse() throws Exception {
        discovery.setAdvertiser(request -> LanAdvertisement.decline());
        discovery.bind(new InetSocketAddress("127.0.0.1", 0));

        sendRequest(Unpooled.EMPTY_BUFFER);
        assertThrows(SocketTimeoutException.class, this::receive);
    }

    @Test
    void anAdvertiserThatDoesNotDeliverFallsBackToTheLastServerDataItProduced() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        LanServerData.V7 oversized = new LanServerData.V7("x".repeat(40_000), 0, "", "", 0, 0, 0, false, false,
                false, false, "abc", 4);
        discovery.setAdvertiser(request -> switch (calls.incrementAndGet()) {
            case 1 -> throw new IllegalStateException("nothing to advertise yet");
            case 2 -> LanAdvertisement.advertise(data(5));
            case 3 -> throw new IllegalStateException("broken");
            case 4 -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                yield LanAdvertisement.advertise(data(6));
            }
            case 5 -> null;
            default -> LanAdvertisement.advertise(oversized);
        });
        discovery.bind(new InetSocketAddress("127.0.0.1", 0));
        try {
            sendRequest(Unpooled.EMPTY_BUFFER);
            assertThrows(SocketTimeoutException.class, this::receive, "nothing to fall back on yet");
            assertEquals(data(5), discover());
            assertEquals(data(5), discover(), "a throwing advertiser");
            assertEquals(data(5), discover(), "a late advertiser");
            assertEquals(data(5), discover(), "a null answer");
            assertEquals(data(5), discover(), "server data too large for a datagram");
        } finally {
            release.countDown();
        }
    }

    @Test
    void onlyRequestsThatPassValidationReachTheAdvertiser() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        discovery.setAdvertiser(request -> {
            calls.incrementAndGet();
            return LanAdvertisement.advertise(data(1));
        });
        discovery.bind(new InetSocketAddress("127.0.0.1", 0));

        sendRequest(Unpooled.wrappedBuffer(new byte[] {1}));
        assertThrows(SocketTimeoutException.class, this::receive);
        assertEquals(0, calls.get());
        assertEquals(data(1), discover());
    }

    @Test
    void fixedServerDataAndAnAdvertiserReplaceEachOther() throws Exception {
        discovery.setAdvertiser(request -> LanAdvertisement.advertise(data(1)));
        discovery.setServerData(data(2));
        discovery.bind(new InetSocketAddress("127.0.0.1", 0));
        assertEquals(data(2), discover());

        discovery.setAdvertiser(request -> LanAdvertisement.advertise(data(3)));
        assertEquals(data(3), discover());
    }

    /** Sends a discovery request and reads the server data of the response. */
    private LanServerData discover() throws Exception {
        sendRequest(Unpooled.EMPTY_BUFFER);
        return receive();
    }

    private void sendRequest(ByteBuf trailing) throws Exception {
        ByteBuf request = Unpooled.buffer().writeShortLE(NetherNetConstants.ID_DISCOVERY_REQUEST)
                .writeLongLE(CLIENT_ID).writeZero(8).writeBytes(trailing);
        try {
            byte[] encrypted = NetherNetConstants.encryptDiscoveryPacket(request);
            client.send(new DatagramPacket(encrypted, encrypted.length, bound.get(2, TimeUnit.SECONDS)));
        } finally {
            request.release();
        }
    }

    private LanServerData receive() throws Exception {
        DatagramPacket datagram = new DatagramPacket(new byte[65535], 65535);
        client.receive(datagram);
        ByteBuf response = NetherNetConstants.decryptDiscoveryPacket(
                Unpooled.wrappedBuffer(datagram.getData(), 0, datagram.getLength()));
        try {
            assertEquals(NetherNetConstants.ID_DISCOVERY_RESPONSE, response.readUnsignedShortLE());
            assertEquals(SERVER_ID, response.readLongLE());
            response.skipBytes(8);
            int length = response.readIntLE();
            assertTrue(length == response.readableBytes(), "the declared length covers the server data");
            String hex = response.readCharSequence(length, StandardCharsets.US_ASCII).toString();
            ByteBuf data = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hex));
            try {
                return NetherNetServerDataCodec.decode(data);
            } finally {
                data.release();
            }
        } finally {
            response.release();
        }
    }
}

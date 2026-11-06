package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class NetherNetDiscoveryProtocolTest {
    @Test
    void receivesACompleteLargeUdpSignalWithVanillasUnderstatedLength() throws Exception {
        try (Discovery discovery = new Discovery()) {
            CompletableFuture<Signal> received = new CompletableFuture<>();
            discovery.value.registerSignalHandler("42", sender -> sender == 20, received::complete);
            String answer = "v=0\r\na=padding:" + "x".repeat(6000);
            String signal = "CONNECTRESPONSE 42 " + answer;
            byte[] encrypted = encrypt(message(20, 1, 1140, signal));
            assertTrue(encrypted.length > 2048);

            discovery.client.send(new DatagramPacket(encrypted, encrypted.length, discovery.address()));

            assertEquals(new Signal(SignalType.CONNECT_RESPONSE, "42", answer),
                    received.get(2, TimeUnit.SECONDS));
            assertEquals(discovery.sender(), discovery.peers.get(20));
        }
    }

    /** Vanilla caps a signal's declared length at 1,140 bytes and sends the whole signal after it. */
    @Test
    void aSignalDeclaresItsLengthAsVanillaDoesAndIsSentWhole() throws Exception {
        try (Discovery discovery = new Discovery()) {
            String shortSignal = "CONNECTERROR 42 1";
            String longSignal = "CONNECTRESPONSE 42 v=0\r\na=padding:" + "x".repeat(6000);
            for (String signal : List.of(shortSignal, longSignal)) {
                discovery.value.sendMessage(discovery.sender(), 20, signal).sync();
                DatagramPacket packet = new DatagramPacket(new byte[65535], 65535);
                discovery.client.receive(packet);
                ByteBuf data = NetherNetConstants.decryptDiscoveryPacket(
                        Unpooled.wrappedBuffer(packet.getData(), 0, packet.getLength()));
                try {
                    data.skipBytes(2 + 8 + 8 + 8); // Packet ID, sender, padding, recipient
                    assertEquals(Math.min(signal.length(), 1140), data.readIntLE());
                    assertEquals(signal, data.toString(StandardCharsets.UTF_8));
                } finally {
                    data.release();
                }
            }
        }
    }

    /**
     * A client that dials an address registers its attempt for target "0", the server discovery
     * finds, before discovery runs: no sender is that server until discovery found it, and then
     * only the server it found is.
     */
    @Test
    void aClientDialingAnAddressTakesSignalsOnlyFromTheServerDiscoveryFound() throws Exception {
        try (Discovery discovery = new Discovery();
             NetherNetDiscoverySignaling signaling = new NetherNetDiscoverySignaling(1, new InetSocketAddress(0),
                     discovery.value, 10_000)) {
            List<Signal> received = new CopyOnWriteArrayList<>();
            signaling.setSignalHandler("42", "0", received::add);

            discovery.receive(message(21, 1, 0, "CONNECTERROR 42 1"));
            discovery.receive(message(20, 1, 0, "CONNECTERROR 42 1"));
            assertEquals(List.of(), received, "before discovery, no sender is the server");

            CompletableFuture<?> connected = signaling.connect(discovery.sender());
            discovery.receive(response(20).writeIntLE(0));
            connected.get(2, TimeUnit.SECONDS);

            discovery.receive(message(21, 1, 0, "CONNECTERROR 42 1"));
            assertEquals(List.of(), received, "a sender other than the server discovery found");
            discovery.receive(message(20, 1, 0, "CONNECTERROR 42 1"));
            assertEquals(List.of(new Signal(SignalType.CONNECT_ERROR, "42", "1")), received);
        }
    }

    @Test
    void aClientAttemptTakesSignalsOnlyFromTheServerItsOfferWentTo() throws Exception {
        try (Discovery discovery = new Discovery()) {
            List<Signal> received = new ArrayList<>();
            discovery.value.registerSignalHandler("42", sender -> sender == 20, received::add);

            discovery.receive(message(21, 1, 0, "CONNECTERROR 42 1"));
            assertEquals(List.of(), received, "A third party that knows the connection ID must not fail the attempt");
            assertNull(discovery.peers.get(21));

            discovery.receive(message(20, 1, 0, "CONNECTERROR 42 1"));
            assertEquals(List.of(new Signal(SignalType.CONNECT_ERROR, "42", "1")), received);
        }
    }

    @Test
    void malformedUnknownAndWrongRecipientMessagesDoNotTeachRoutes() throws Exception {
        try (Discovery discovery = new Discovery()) {
            AtomicInteger received = new AtomicInteger();
            discovery.value.registerSignalHandler("42", sender -> sender == 20, signal -> received.incrementAndGet());
            String valid = "CONNECTRESPONSE 42 v=0";
            List<ByteBuf> invalid = new ArrayList<>();
            invalid.add(header(NetherNetConstants.ID_DISCOVERY_MESSAGE, 20));
            invalid.add(header(NetherNetConstants.ID_DISCOVERY_MESSAGE, 20).writeLongLE(1));
            invalid.add(message(20, 1, -1, valid));
            invalid.add(message(20, 1, Integer.MAX_VALUE, valid));
            invalid.add(message(20, 99, valid.length(), valid));
            invalid.add(message(20, 1, 0, "UNKNOWN 42 data"));
            invalid.add(message(20, 1, 0, "CONNECTRESPONSE nope data"));
            invalid.add(message(20, 1, 0, "CONNECTRESPONSE 42"));
            invalid.add(header(0x7fff, 20));
            invalid.add(Unpooled.buffer().writeShortLE(NetherNetConstants.ID_DISCOVERY_REQUEST).writeLongLE(20));
            invalid.add(header(NetherNetConstants.ID_DISCOVERY_REQUEST, 20).writeByte(1));
            try {
                for (ByteBuf packet : invalid) {
                    discovery.receive(packet);
                    assertNull(discovery.peers.get(20));
                }
            } finally {
                invalid.forEach(packet -> {
                    if (packet.refCnt() != 0) {
                        packet.release();
                    }
                });
            }
            assertEquals(0, received.get());
            discovery.receive(message(20, 0, 0, "CONNECTERROR 42"));
            assertEquals(1, received.get());
        }
    }

    @Test
    void malformedResponsesAreDroppedWithoutTransferringPayloadOwnership() throws Exception {
        try (Discovery discovery = new Discovery()) {
            AtomicInteger responses = new AtomicInteger();
            AtomicReference<ByteBuf> received = new AtomicReference<>();
            discovery.value.sendDiscoveryRequest(discovery.sender(), (id, payload) -> {
                received.set(payload);
                responses.incrementAndGet();
                payload.release();
            });
            List<ByteBuf> invalid = List.of(
                    response(20).writeZero(3), response(20).writeIntLE(-1),
                    response(20).writeIntLE(4).writeBytes(new byte[]{'0', '4'}),
                    response(20).writeIntLE(2).writeBytes(new byte[]{'0', '4', '0', '0'}),
                    response(20).writeIntLE(1).writeByte('a'),
                    response(20).writeIntLE(2).writeBytes(new byte[]{'g', 'g'}));
            try {
                for (ByteBuf packet : invalid) {
                    discovery.receive(packet);
                    assertNull(discovery.peers.get(20));
                }
            } finally {
                invalid.forEach(packet -> {
                    if (packet.refCnt() != 0) {
                        packet.release();
                    }
                });
            }
            assertEquals(0, responses.get());

            discovery.receive(response(20).writeIntLE(2).writeBytes(new byte[]{'0', '4'}));

            assertEquals(1, responses.get());
            assertEquals(0, received.get().refCnt());
            assertEquals(discovery.sender(), discovery.peers.get(20));
        }
    }

    @Test
    void responsesFromAnUnrequestedSocketDoNotTeachRoutes() throws Exception {
        try (Discovery discovery = new Discovery()) {
            discovery.value.sendDiscoveryRequestToPeer(discovery.sender(), (id, payload) -> {
                payload.release();
                fail("Unexpected responder");
            });
            discovery.receive(response(20).writeIntLE(0), new InetSocketAddress("127.0.0.1", 1));
            assertNull(discovery.peers.get(20));
        }
    }

    @Test
    void offerCallbacksCanRegisterJoinsAndQuietJoinsKeepTheirRoutes() throws Exception {
        try (Discovery discovery = new Discovery()) {
            CompletableFuture<Void> registered = new CompletableFuture<>();
            discovery.value.setOfferHandler((connectionId, senderId, offer, sender) -> {
                try {
                    assertTrue(CompletableFuture.supplyAsync(() -> discovery.value.registerJoin(senderId, connectionId,
                            sender, signal -> { })).get(2, TimeUnit.SECONDS));
                    registered.complete(null);
                } catch (Exception e) {
                    registered.completeExceptionally(e);
                }
            });
            discovery.receive(message(20, 1, 0, "CONNECTREQUEST 42 v=0"));
            registered.get(2, TimeUnit.SECONDS);
            for (long peer = 30; peer < 35; peer++) {
                discovery.receive(header(NetherNetConstants.ID_DISCOVERY_REQUEST, peer));
            }
            discovery.now.set(100);
            assertEquals(0, discovery.peers.passiveSize());

            discovery.value.sendSignal(20, new Signal(SignalType.CANDIDATE_ADD, "42", "candidate"));
            DatagramPacket signal = new DatagramPacket(new byte[65535], 65535);
            discovery.client.receive(signal);
            assertTrue(signal.getLength() > 0);
            discovery.value.unregisterJoin(20, "42");
            discovery.now.set(111);
            assertThrows(IllegalArgumentException.class, () -> discovery.value.sendSignal(20, new Signal(SignalType.CANDIDATE_ADD, "42", "candidate")));
            discovery.value.close();
            assertEquals(0, discovery.peers.passiveSize());
        }
    }

    private static ByteBuf header(int id, long peer) {
        return Unpooled.buffer().writeShortLE(id).writeLongLE(peer).writeZero(8);
    }

    private static ByteBuf response(long peer) {
        return header(NetherNetConstants.ID_DISCOVERY_RESPONSE, peer);
    }

    private static ByteBuf message(long peer, long recipient, int declaredLength, String signal) {
        return header(NetherNetConstants.ID_DISCOVERY_MESSAGE, peer).writeLongLE(recipient).writeIntLE(declaredLength)
                .writeBytes(signal.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] encrypt(ByteBuf packet) throws Exception {
        try {
            return NetherNetConstants.encryptDiscoveryPacket(packet);
        } finally {
            packet.release();
        }
    }

    private static final class Discovery implements AutoCloseable {
        final AtomicLong now = new AtomicLong();
        final DiscoveryPeerRegistry peers = new DiscoveryPeerRegistry(2, 10, now::get);
        final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        final DatagramSocket client = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        ChannelHandlerContext context;
        final NetherNetDiscovery value = new NetherNetDiscovery(1, () -> group, peers) {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) {
                context = ctx;
            }
        };

        Discovery() throws Exception {
            client.setSoTimeout(2000);
            value.bind(new InetSocketAddress("127.0.0.1", 0));
        }

        InetSocketAddress sender() {
            return (InetSocketAddress) client.getLocalSocketAddress();
        }

        InetSocketAddress address() {
            return (InetSocketAddress) context.channel().localAddress();
        }

        void receive(ByteBuf packet) throws Exception {
            receive(packet, sender());
        }

        void receive(ByteBuf packet, InetSocketAddress sender) throws Exception {
            ByteBuf encrypted = Unpooled.wrappedBuffer(encrypt(packet));
            context.executor().submit(() -> context.pipeline().fireChannelRead(
                    new io.netty.channel.socket.DatagramPacket(encrypted, address(), sender))).syncUninterruptibly();
            assertEquals(0, encrypted.refCnt());
        }

        @Override
        public void close() {
            value.close();
            client.close();
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}

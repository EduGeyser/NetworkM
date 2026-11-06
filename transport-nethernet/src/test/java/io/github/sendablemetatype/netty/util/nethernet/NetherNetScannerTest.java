package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetDiscovery;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class NetherNetScannerTest {
    private static final LanServerData.V7 PREVIEW_DATA = new LanServerData.V7("Dedicated Server", 2207, "1.26.60",
            "Bedrock level", 0, 10, 0, false, false, false, false, "24728cbd0c0b6df4", 4);

    @Test
    void discoveryAnswersWithTheV7PreviewResponse() throws Exception {
        ByteBuf response = discover(PREVIEW_DATA);
        try {
            int length = response.readIntLE();
            assertEquals(134, length);
            try (var stream = getClass().getResourceAsStream("/discovery/preview-1.26.60.21-v7.hex")) {
                assertNotNull(stream);
                assertEquals(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).strip(),
                        response.readCharSequence(length, StandardCharsets.US_ASCII).toString());
            }
        } finally {
            response.release();
        }
    }

    @Test
    void theScannerReadsTheStableV6Capture() throws Exception {
        String hex;
        try (var stream = getClass().getResourceAsStream("/discovery/stable-1.26.45.1-v6.hex")) {
            assertNotNull(stream);
            hex = new String(stream.readAllBytes(), StandardCharsets.US_ASCII).strip();
        }
        ByteBuf response = Unpooled.buffer().writeIntLE(hex.length());
        response.writeCharSequence(hex, StandardCharsets.US_ASCII);
        try {
            assertEquals(new LanServerData.V6("Dedicated Server", "Bedrock level", 0, 0, 10,
                    false, false, true, false, "3e8a5e7a932c5aa4", 2, 4), NetherNetScanner.readResponse(response));
        } finally {
            response.release();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"07", "0780", "078001", "07ffffffff0f", "078080808010", "07808080808000", "07000000",
            "06", "06000000"})
    void truncatedAndOverflowingFieldsAreRejected(String hex) {
        ByteBuf response = Unpooled.buffer().writeIntLE(hex.length());
        response.writeCharSequence(hex, StandardCharsets.US_ASCII);
        try {
            assertThrows(RuntimeException.class, () -> NetherNetScanner.readResponse(response));
        } finally {
            response.release();
        }
    }

    @Test
    void largestAdvertisementFitsUdpAndAnOversizedUpdatePreservesIt() throws Exception {
        // Version 1, name 3 + 32706, protocol 1, version 1, level 1, counts 3, flags 4, nonce 2, connection 1
        LanServerData.V7 largest = new LanServerData.V7("x".repeat(32706), 0, "", "", 0, 0, 0,
                false, false, true, false, "a", 4);
        LanServerData.V7 oversized = new LanServerData.V7("x".repeat(32707), 0, "", "", 0, 0, 0,
                false, false, true, false, "a", 4);
        ByteBuf response = discover(largest, discovery ->
                assertThrows(IllegalArgumentException.class, () -> discovery.setServerData(oversized)));
        try {
            assertEquals(4 + 2 * 32723, response.readableBytes());
            assertEquals(largest, NetherNetScanner.readResponse(response));
        } finally {
            response.release();
        }
    }

    private static ByteBuf discover(LanServerData.V7 data) throws Exception {
        return discover(data, ignored -> {});
    }

    private static ByteBuf discover(LanServerData.V7 data, Consumer<NetherNetDiscovery> afterUpdate) throws Exception {
        AtomicReference<Channel> channel = new AtomicReference<>();
        NetherNetDiscovery discovery = new NetherNetDiscovery(1) {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) {
                channel.set(ctx.channel());
            }
        };
        try (DatagramSocket socket = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0))) {
            socket.setSoTimeout(2000);
            discovery.setServerData(data);
            afterUpdate.accept(discovery);
            discovery.bind(new InetSocketAddress("127.0.0.1", 0));
            ByteBuf request = Unpooled.buffer().writeShortLE(NetherNetConstants.ID_DISCOVERY_REQUEST)
                    .writeLongLE(2).writeZero(8);
            byte[] encrypted;
            try {
                encrypted = NetherNetConstants.encryptDiscoveryPacket(request);
            } finally {
                request.release();
            }
            socket.send(new DatagramPacket(encrypted, encrypted.length, channel.get().localAddress()));
            DatagramPacket received = new DatagramPacket(new byte[65535], 65535);
            socket.receive(received);
            ByteBuf packet = Unpooled.wrappedBuffer(received.getData(), received.getOffset(), received.getLength());
            ByteBuf response;
            try {
                response = NetherNetConstants.decryptDiscoveryPacket(packet);
            } finally {
                packet.release();
            }
            assertNotNull(response);
            assertEquals(NetherNetConstants.ID_DISCOVERY_RESPONSE, response.readUnsignedShortLE());
            response.skipBytes(16);
            return response;
        } finally {
            discovery.close();
            Channel bound = channel.get();
            if (bound != null) {
                bound.eventLoop().terminationFuture().awaitUninterruptibly(2, TimeUnit.SECONDS);
            }
        }
    }
}

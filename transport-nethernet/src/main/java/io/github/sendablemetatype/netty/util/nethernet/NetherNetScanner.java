package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.codec.NetherNetServerDataCodec;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetDiscovery;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A simple scanner example for discovering NetherNet servers on the local network.
 */
public class NetherNetScanner {
    public static void main(String[] args) throws Exception {
        long myNetworkId = ThreadLocalRandom.current().nextLong();
        NetherNetDiscovery discovery = new NetherNetDiscovery(myNetworkId);

        discovery.bind(new InetSocketAddress("::", 0));

        System.out.println("Scanning for NetherNet servers on port 7551...");

        InetSocketAddress broadcastTarget = new InetSocketAddress("255.255.255.255", NetherNetConstants.DISCOVERY_PORT);

        discovery.sendDiscoveryRequest(broadcastTarget, (senderId, payload) -> {
            try {
                LanServerData data = readResponse(payload);
                System.out.println("--------------------------------");
                System.out.println("Found Server: " + Long.toUnsignedString(senderId));
                System.out.println("Data Version: " + data.dataVersion());
                switch (data) {
                    case LanServerData.V7 v7 -> {
                        System.out.println("MOTD: " + v7.serverName());
                        System.out.println("Level: " + v7.levelName());
                        System.out.println("Version: " + v7.version() + " (protocol " + v7.protocol() + ")");
                        System.out.println("Players: " + v7.playerCount() + "/" + v7.maxPlayerCount());
                        System.out.println("Game Mode: " + v7.gameType());
                        System.out.println("Editor World: " + v7.editorWorld());
                        System.out.println("Hardcore: " + v7.hardcore());
                        System.out.println("Accepts Online Auth: " + v7.onlineAuth());
                        System.out.println("Accepts Self-Signed Auth: " + v7.selfSignedAuth());
                        System.out.println("Nonce: " + v7.nonce());
                    }
                    case LanServerData.V6 v6 -> {
                        System.out.println("MOTD: " + v6.serverName());
                        System.out.println("Level: " + v6.levelName());
                        System.out.println("Players: " + v6.playerCount() + "/" + v6.maxPlayerCount());
                        System.out.println("Game Mode: " + v6.gameType());
                        System.out.println("Editor World: " + v6.editorWorld());
                        System.out.println("Hardcore: " + v6.hardcore());
                        System.out.println("Accepts Online Auth: " + v6.onlineAuth());
                        System.out.println("Accepts Self-Signed Auth: " + v6.selfSignedAuth());
                        System.out.println("Nonce: " + v6.nonce());
                    }
                }
                System.out.println("--------------------------------");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                payload.release();
            }
        });

        Thread.sleep(10000);
        discovery.close();
    }

    /**
     * Reads a discovery response body: a little-endian length, then that many hex characters
     * encoding the server data.
     */
    static LanServerData readResponse(ByteBuf payload) {
        if (!payload.isReadable(4)) {
            throw new IllegalArgumentException("Missing discovery response length");
        }
        int length = payload.readIntLE();
        if (length < 0 || length != payload.readableBytes()) {
            throw new IllegalArgumentException("Invalid discovery response length");
        }
        String hex = payload.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        ByteBuf data = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hex));
        try {
            return NetherNetServerDataCodec.decode(data);
        } finally {
            data.release();
        }
    }
}

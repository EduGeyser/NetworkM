/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
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
import io.github.sendablemetatype.netty.util.address.IpRangeSet;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A server on a real libwebrtc engine behind a PROXY protocol version 2 relay in front of its shared
 * ICE port (ruling G-6, item 2), over real UDP: media only through the relay, the client the relay's
 * header declares as the child's address (P10 / G-3), and how long a relay port stays with a client.
 * Ported from upstream's {@code RelayedJoinTest}.
 * <p>
 * libwebrtc gathers no loopback candidates, so server, relay and client share one IPv4 interface
 * address of this host instead.
 */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
@Timeout(120)
class NetherNetProxyRelayNativeTest {
    private static final long WAIT_SECONDS = 20;
    /** Where signaling met the client; the relayed address must win over it. */
    private static final InetSocketAddress SIGNALED = new InetSocketAddress("198.51.100.20", 40000);
    private static final byte[] PING = {0, 'p', 'i', 'n', 'g'};

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final NetherNetEngine engine = WebRtcEngine.builder().factories(2).build();
    private final BlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    private final BlockingQueue<NetherNetChildChannel> active = new LinkedBlockingQueue<>();
    private final List<Peer> clients = new ArrayList<>();
    private InetAddress lan;
    private TestProxyRelay relay;
    private FakeJoinSource source;
    private Channel server;

    @BeforeEach
    void setUp() throws Exception {
        lan = lanAddress();
        assumeTrue(lan != null, "No IPv4 interface address to run the relay on");
        // The header declares a client elsewhere, on the client's own port, so the child's address can only
        // have come from the header
        relay = new TestProxyRelay(lan, client -> new InetSocketAddress("203.0.113.7", client.getPort()));
        server = new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                .option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES, new TrustedProxies(
                        IpRangeSet.parse(List.of(lan.getHostAddress())), IpRangeSet.empty()))
                .option(NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES, List.of(relay.address()))
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) {
                        NetherNetChildChannel child = (NetherNetChildChannel) channel;
                        children.add(child);
                        child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                active.add(child);
                            }

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object message) {
                                ctx.writeAndFlush(message);
                            }
                        });
                    }
                })
                .bind(new InetSocketAddress(lan, 0)).sync().channel();
        relay.forwardTo((InetSocketAddress) server.localAddress());
        source = new FakeJoinSource(false);
        ((NetherNetServerChannel) server).addSource(source, EndpointHarness.ADMIT_ALL);
    }

    @AfterEach
    void tearDown() throws Exception {
        clients.forEach(Peer::close);
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        if (relay != null) {
            relay.close();
        }
        engine.close().toCompletableFuture().get(20, TimeUnit.SECONDS);
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aJoinRunsThroughTheRelayAndReportsTheClientItsHeaderDeclares() throws Exception {
        Client client = join(1);
        NetherNetChildChannel child = active.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(child, "the client connected through the relay");

        client.reliable().send(ByteBuffer.wrap(PING), failure -> { });
        assertArrayEquals(PING, client.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS), "the server's echo");
        assertTrue(relay.up.get() > 0 && relay.down.get() > 0, "media went both ways through the relay");

        SourcedAddress reported = await(() -> child.reportedAddress().source() == AddressSource.ICE_PROXY_PROTOCOL
                ? child.reportedAddress() : null);
        TestProxyRelay.Flow flow = relay.flowDeclaring(reported.address());
        assertNotNull(flow, "the child reports the client the header declared: " + reported);
        assertEquals("203.0.113.7", reported.address().getAddress().getHostAddress());
        assertEquals(flow.port(), reported.via(), "through the relay port that brought it");
        assertEquals(reported.address(), child.remoteAddress());
    }

    /**
     * A relay port stays with its client while the server's session lives. Once the relay reuses the port
     * for another client, the old session no longer hears from its own client and ends (after about 13 s on
     * the machine this was written on), and the port passes to the new client at once, long before the
     * route's own idle expiry (120 s in webrtc-java).
     */
    @Test
    void aRelayPortPassesToTheNextClientOnlyOnceTheOldSessionEnds() throws Exception {
        join(1);
        NetherNetChildChannel first = active.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(first);
        SourcedAddress firstReported = await(() -> first.reportedAddress().source() == AddressSource.ICE_PROXY_PROTOCOL
                ? first.reportedAddress() : null);
        TestProxyRelay.Flow port = relay.flowDeclaring(firstReported.address());
        assertNotNull(port);

        // The relay reuses the first client's port for the next one, whose checks the server hears but
        // must not answer through a port that still routes a live session
        relay.handOverNextClient(port);
        Client second = join(2);
        assertNull(active.poll(2, TimeUnit.SECONDS), "the port still belongs to the first client");

        NetherNetChildChannel next = active.poll(45, TimeUnit.SECONDS);
        assertNotNull(next, "the second client took the port over");
        assertFalse(first.isOpen(), "only once the first session was gone");
        second.reliable().send(ByteBuffer.wrap(PING), failure -> { });
        assertArrayEquals(PING, second.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        SourcedAddress reported = await(() -> next.reportedAddress().source() == AddressSource.ICE_PROXY_PROTOCOL
                ? next.reportedAddress() : null);
        assertEquals(port.port(), reported.via(), "through the same relay port");
        assertFalse(reported.address().equals(firstReported.address()), "as the second client");
    }

    /** Offers from a real client gathering on the interface address, and applies the server's answer. */
    private Client join(long connectionId) throws Exception {
        Client client = new Client();
        Peer peer = engine.createPeer(new PeerConfig(PeerConfig.Role.OFFERER, List.of(),
                new CandidatePolicy(false, List.of(lan), null), null, null, null), client);
        clients.add(peer);
        client.reliable.complete(peer.openChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL)));
        peer.openChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        peer.offer().toCompletableFuture().get(10, TimeUnit.SECONDS);
        String offer = peer.gatheredLocalDescription().toCompletableFuture().get(10, TimeUnit.SECONDS);

        String answer = source.offer(connectionId, offer, new SourcedAddress(SIGNALED, AddressSource.SIGNALING, null),
                null).answer();
        List<String> candidates = answer.lines().filter(line -> line.startsWith("a=candidate:")).toList();
        InetSocketAddress front = relay.address();
        assertEquals(1, candidates.size(), "only the relay: " + answer);
        assertTrue(candidates.getFirst().endsWith(" " + front.getAddress().getHostAddress() + " " + front.getPort()
                + " typ srflx raddr 0.0.0.0 rport 0"), candidates.getFirst());

        peer.applyAnswer(answer).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertNotNull(children.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        return client;
    }

    private static <T> T await(Supplier<T> condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        T value;
        while ((value = condition.get()) == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertNotNull(value, "timed out");
        return value;
    }

    /** An up, non-virtual IPv4 interface address, or null. */
    private static InetAddress lanAddress() throws SocketException {
        for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!nic.isUp() || nic.isLoopback() || nic.isVirtual()) {
                continue;
            }
            for (InetAddress address : Collections.list(nic.getInetAddresses())) {
                if (address instanceof Inet4Address && !address.isLinkLocalAddress()) {
                    return address;
                }
            }
        }
        return null;
    }

    /** A client peer's channel and what reached it. */
    private static final class Client implements PeerListener {
        final CompletableFuture<DataLink> reliable = new CompletableFuture<>();
        final BlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();
        final CompletableFuture<Void> open = new CompletableFuture<>();

        /** The reliable channel, once open. */
        DataLink reliable() throws Exception {
            open.get(WAIT_SECONDS, TimeUnit.SECONDS);
            return reliable.get();
        }

        @Override
        public void onChannelOpen(DataLink link) {
            if (link.parameters().label().equals(NetherNetConstants.RELIABLE_CHANNEL_LABEL)) {
                open.complete(null);
            }
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            byte[] bytes = new byte[message.remaining()];
            message.get(bytes);
            messages.add(bytes);
        }

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

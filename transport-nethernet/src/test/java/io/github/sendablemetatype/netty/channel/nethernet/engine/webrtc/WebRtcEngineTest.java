package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EndpointConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint.BindFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint.Sockets;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig.Role;
import io.github.sendablemetatype.webrtc.IceUdpEndpoint;
import io.github.sendablemetatype.webrtc.PeerConnectionObserver;
import io.github.sendablemetatype.webrtc.PortAllocatorConfig;
import io.github.sendablemetatype.webrtc.RTCBundlePolicy;
import io.github.sendablemetatype.webrtc.RTCConfiguration;
import io.github.sendablemetatype.webrtc.RTCDataChannelSendObserver;
import io.github.sendablemetatype.webrtc.RTCTcpCandidatePolicy;
import io.github.sendablemetatype.webrtc.logging.Logging.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.BindException;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Timeout(10)
class WebRtcEngineTest {
    private final List<String> log = Collections.synchronizedList(new ArrayList<>());

    @Test
    void aPeerConfigurationMapsToLibwebrtcSettings() throws Exception {
        WebRtcEngine engine = engine(1, () -> {
            throw new AssertionError("No factory needed");
        });
        InetAddress address = InetAddress.getByName("192.0.2.10");
        PeerConfig config = new PeerConfig(Role.ANSWERER,
                List.of(new IceServer(List.of("turn:relay.example:3478"), "user", "secret")),
                new CandidatePolicy(false, List.of(address), new CandidatePolicy.PortRange(40000, 40100)),
                null, new DtlsIdentity("CERT", "KEY"), null);

        RTCConfiguration configuration = engine.configuration(config);

        assertEquals(RTCBundlePolicy.MAX_BUNDLE, configuration.bundlePolicy);
        assertEquals(RTCTcpCandidatePolicy.DISABLED, configuration.tcpCandidatePolicy);
        assertEquals(List.of("turn:relay.example:3478"), configuration.iceServers.getFirst().urls);
        assertEquals("secret", configuration.iceServers.getFirst().password);
        PortAllocatorConfig allocator = configuration.portAllocatorConfig;
        assertTrue(allocator.isTcpDisabled());
        assertFalse(allocator.isIpv6Enabled());
        assertFalse(allocator.isFlagEnabled(PortAllocatorConfig.PORTALLOCATOR_ENABLE_IPV6_ON_WIFI));
        assertTrue(allocator.isFlagEnabled(PortAllocatorConfig.PORTALLOCATOR_DISABLE_LINK_LOCAL_NETWORKS));
        assertTrue(allocator.isFlagEnabled(PortAllocatorConfig.PORTALLOCATOR_ENABLE_ANY_ADDRESS_PORTS),
                "a peer with ports of its own keeps any-address ports");
        assertEquals(List.of(address), allocator.allowedAddresses);
        assertEquals(40000, allocator.minPort);
        assertEquals(40100, allocator.maxPort);
        assertEquals("KEY", configuration.certificates.getFirst().getPrivateKey());
        assertEquals("CERT", configuration.certificates.getFirst().getCertificate());
        assertNull(configuration.iceEndpoint);
        engine.close().toCompletableFuture().join();
    }

    @Test
    void aPeerOnASharedEndpointGathersWithoutAnyAddressPorts() throws Exception {
        CandidatePolicy policy = new CandidatePolicy(true, List.of(), new CandidatePolicy.PortRange(40000, 40100));

        PortAllocatorConfig allocator = WebRtcEngine.allocator(WebRtcEngine.DEFAULT_PORT_ALLOCATOR_FLAGS, policy, true);

        assertFalse(allocator.isFlagEnabled(PortAllocatorConfig.PORTALLOCATOR_ENABLE_ANY_ADDRESS_PORTS));
        assertTrue(allocator.isFlagEnabled(PortAllocatorConfig.PORTALLOCATOR_ENABLE_SHARED_SOCKET));
        assertTrue(allocator.isIpv6Enabled());
        assertEquals(0, allocator.minPort, "the endpoint owns the port");
        assertEquals(0, allocator.maxPort);
    }

    @Test
    void factoriesStartWithTheirFirstPeerAndTakePeersInTurn() throws Exception {
        CountDownLatch created = new CountDownLatch(3);
        AtomicInteger ids = new AtomicInteger();
        WebRtcEngine engine = engine(2, () -> new RecordingFactory(ids.incrementAndGet(), log, created));
        assertTrue(log.isEmpty());

        for (int i = 0; i < 3; i++) {
            engine.createPeer(answerer(), new FakeNative.Listener());
        }

        assertTrue(created.await(5, TimeUnit.SECONDS));
        assertEquals(2, ids.get());
        Map<String, Long> peersPerFactory = log.stream().filter(event -> event.startsWith("peer on"))
                .collect(Collectors.groupingBy(event -> event, Collectors.counting()));
        assertEquals(List.of(1L, 2L), peersPerFactory.values().stream().sorted().toList());
        engine.close().toCompletableFuture().join();
    }

    @Test
    void closeReleasesTheFactoriesOnlyAfterEveryPeerTerminated() throws Exception {
        CountDownLatch created = new CountDownLatch(1);
        RecordingFactory[] factory = new RecordingFactory[1];
        WebRtcEngine engine = engine(1, () -> factory[0] = new RecordingFactory(1, log, created));
        FakeNative.Listener listener = new FakeNative.Listener();
        Peer peer = engine.createPeer(answerer(), listener);
        assertTrue(created.await(5, TimeUnit.SECONDS));
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), log);
        ((WebRtcPeer) peer).onRemoteChannel(channel);
        DataLink link = listener.remoteLinks.getFirst();
        CompletableFuture<Throwable> sent = new CompletableFuture<>();
        link.send(ByteBuffer.allocate(1), sent::complete);

        CompletableFuture<Void> closed = engine.close().toCompletableFuture();

        assertThrows(IllegalStateException.class, () -> engine.createPeer(answerer(), listener));
        assertFalse(closed.isDone());
        awaitLog("close peer");
        assertFalse(factory[0].disposed.await(200, TimeUnit.MILLISECONDS), "Disposed under a closing peer");

        RTCDataChannelSendObserver pending = channel.send(0);
        pending.onFailure("[INVALID_STATE] Send operation was discarded before completion");
        closed.get(5, TimeUnit.SECONDS);

        assertTrue(peer.close().toCompletableFuture().isDone());
        List<String> order = List.copyOf(log);
        assertTrue(order.indexOf("dispose ReliableDataChannel") > order.indexOf("close peer"), order.toString());
        assertTrue(order.indexOf("dispose factory") > order.indexOf("dispose ReliableDataChannel"), order.toString());
    }

    @Test
    void closeWaitsForAnEndpointBeingOpened() throws Exception {
        WebRtcEngine engine = engine(1, () -> {
            throw new AssertionError("No factory needed");
        });
        int port;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            port = socket.getLocalPort();
        }

        // Without natives the creation fails; with them it may succeed. Either way it ends before the close.
        CompletableFuture<IceEndpoint> opening = engine.openEndpoint(new EndpointConfig(port, List.of())).toCompletableFuture();
        boolean openedFirst = engine.close().toCompletableFuture().thenApply(ignored -> opening.isDone()).get(5, TimeUnit.SECONDS);

        assertTrue(openedFirst, "The engine closed while an endpoint was still being created");
        assertTrue(engine.openEndpoint(new EndpointConfig(port, List.of())).toCompletableFuture().isCompletedExceptionally());
    }

    /** Needs natives. Another socket takes the endpoint's port on an address libwebrtc gathers on. */
    @Test
    @Timeout(30)
    @EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
    void aFailedBindLogsOneWarningForRepeatedFailuresOnAnAddress() throws Exception {
        Logger endpointLog = Logger.getLogger(WebRtcIceEndpoint.class.getName());
        Level endpointLevel = endpointLog.getLevel();
        List<LogRecord> records = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        endpointLog.setLevel(Level.ALL);
        endpointLog.addHandler(capture);
        WebRtcEngine engine = WebRtcEngine.builder().build();
        try {
            InetAddress address = gatheredAddress(engine);
            String host = address.getHostAddress();

            int port = freeUdpPort();
            IceEndpoint second = engine.openEndpoint(new EndpointConfig(port, List.of(address)))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            try (DatagramSocket taken = new DatagramSocket(null)) {
                taken.setReuseAddress(false);
                taken.bind(new InetSocketAddress(address, port));
                gather(engine, second);
                gather(engine, second);
                // The engine's endpoint thread logs both failures in order, so the second comes last
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (logged(records, Level.FINE, host).isEmpty()) {
                    assertTrue(System.nanoTime() < deadline, "No repeated failure for " + host + " in " + records);
                    Thread.sleep(5);
                }
            }

            assertEquals(1, logged(records, Level.WARNING, host).size(), records::toString);
        } finally {
            engine.close().toCompletableFuture().get(10, TimeUnit.SECONDS);
            endpointLog.removeHandler(capture);
            endpointLog.setLevel(endpointLevel);
        }
    }

    /** Needs natives. Another socket holds the endpoint's port on its one address, then lets it go. */
    @Test
    @Timeout(30)
    @EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
    void socketsShowAFailedBindUntilALaterPeerOpensThePortThere() throws Exception {
        WebRtcEngine engine = WebRtcEngine.builder().build();
        try {
            InetAddress address = gatheredAddress(engine);
            int port = freeUdpPort();
            InetSocketAddress local = new InetSocketAddress(address, port);
            IceEndpoint endpoint = engine.openEndpoint(new EndpointConfig(port, List.of(address)))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(Sockets.NONE, sockets(endpoint), "nothing is bound before a peer gathers");

            try (DatagramSocket taken = new DatagramSocket(null)) {
                taken.setReuseAddress(false);
                taken.bind(local);
                gather(engine, endpoint);
                assertEquals(new Sockets(List.of(), List.of(new BindFailure(local, "Address already in use"))),
                        sockets(endpoint), "the reason alone, without the address");
            }
            gather(engine, endpoint);
            assertEquals(new Sockets(List.of(local), List.of()), sockets(endpoint));

            endpoint.close().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThrows(ExecutionException.class, () -> sockets(endpoint), "a closed endpoint has no sockets to tell");
        } finally {
            engine.close().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aBindFailureKeepsOnlyTheSystemsReason() throws Exception {
        InetSocketAddress ipv4 = new InetSocketAddress(InetAddress.getByName("192.0.2.5"), 19132);
        assertEquals("Address already in use", WebRtcIceEndpoint.reason(new IceUdpEndpoint.BindFailure(ipv4,
                new BindException("Cannot bind 192.0.2.5 port 19132: Address already in use"))));
        InetSocketAddress scoped = new InetSocketAddress(
                Inet6Address.getByAddress(null, InetAddress.getByName("fe80::1").getAddress(), 7), 19132);
        assertEquals("Permission denied", WebRtcIceEndpoint.reason(new IceUdpEndpoint.BindFailure(scoped,
                new BindException("Cannot bind fe80:0:0:0:0:0:0:1%7 port 19132: Permission denied"))));
        assertEquals("Socket error 10013", WebRtcIceEndpoint.reason(new IceUdpEndpoint.BindFailure(ipv4,
                new BindException("Socket error 10013"))), "a message in another form stays whole");
    }

    @Test
    void remoteChannelParametersComeFromTheGetters() {
        assertEquals(ChannelParameters.reliable("ReliableDataChannel"),
                NativeLink.parametersOf("ReliableDataChannel", true, true, 0, 0, false, ""));
        assertEquals(ChannelParameters.unreliable("UnreliableDataChannel"),
                NativeLink.parametersOf("UnreliableDataChannel", false, false, 0, 0, false, null));
        assertEquals(new ChannelParameters("timed", false, OptionalInt.empty(), OptionalInt.of(150), true, "p"),
                NativeLink.parametersOf("timed", false, false, 0, 150, true, "p"));
        assertEquals(new ChannelParameters("limited", true, OptionalInt.of(3), OptionalInt.empty(), false, ""),
                NativeLink.parametersOf("limited", true, false, 3, 0, false, ""));
    }

    @Test
    void relaysMustBeAddressesOrRanges() {
        WebRtcIceEndpoint.requireRelay("203.0.113.7");
        WebRtcIceEndpoint.requireRelay("2001:db8::/32");
        WebRtcIceEndpoint.requireRelay("10.0.0.0/8");
        assertThrows(IllegalArgumentException.class, () -> WebRtcIceEndpoint.requireRelay("relay.example"));
        assertThrows(IllegalArgumentException.class, () -> WebRtcIceEndpoint.requireRelay("10.0.0.0/33"));
        assertThrows(IllegalArgumentException.class, () -> WebRtcIceEndpoint.requireRelay("10.0.0.0/x"));
    }

    private void awaitLog(String event) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!log.contains(event)) {
            assertTrue(System.nanoTime() < deadline, "No " + event + " in " + log);
            Thread.sleep(5);
        }
    }

    /** Gathers once with a peer of its own on the endpoint, which binds the port where it can. */
    private static void gather(WebRtcEngine engine, IceEndpoint endpoint) throws Exception {
        Peer peer = engine.createPeer(new PeerConfig(Role.OFFERER, List.of(), CandidatePolicy.DEFAULT, endpoint, null,
                null), new FakeNative.Listener());
        try {
            // An offer needs a data section
            peer.openChannel(ChannelParameters.reliable("probe"));
            peer.offer().thenCompose(offer -> peer.gatheredLocalDescription()).toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
        } finally {
            peer.close().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    /** An address libwebrtc gathers on, IPv4 if there is one; skips the test if there is none. */
    private static InetAddress gatheredAddress(WebRtcEngine engine) throws Exception {
        IceEndpoint probe = engine.openEndpoint(new EndpointConfig(freeUdpPort(), List.of())).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        gather(engine, probe);
        List<InetSocketAddress> bound = sockets(probe).bound();
        assumeFalse(bound.isEmpty(), "libwebrtc gathered on no address");
        return bound.stream().map(InetSocketAddress::getAddress)
                .filter(Inet4Address.class::isInstance).findFirst().orElse(bound.getFirst().getAddress());
    }

    private static Sockets sockets(IceEndpoint endpoint) throws Exception {
        return endpoint.sockets().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static List<LogRecord> logged(List<LogRecord> records, Level level, String text) {
        return records.stream().filter(record -> record.getLevel() == level && record.getMessage().contains(text))
                .toList();
    }

    private static int freeUdpPort() throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static PeerConfig answerer() {
        return new PeerConfig(Role.ANSWERER, List.of(), CandidatePolicy.DEFAULT, null, null, null);
    }

    private static WebRtcEngine engine(int factories, Supplier<NativePeer.Factory> factory) {
        return new WebRtcEngine(factories, factory, WebRtcEngine.DEFAULT_PORT_ALLOCATOR_FLAGS, Severity.NONE);
    }

    private static final class RecordingFactory implements NativePeer.Factory {
        private final int id;
        private final List<String> log;
        private final CountDownLatch created;
        final CountDownLatch disposed = new CountDownLatch(1);

        RecordingFactory(int id, List<String> log, CountDownLatch created) {
            this.id = id;
            this.log = log;
            this.created = created;
            log.add("factory " + id);
        }

        @Override
        public NativePeer createPeer(RTCConfiguration configuration, PeerConnectionObserver observer) {
            log.add("peer on " + id);
            created.countDown();
            return new FakeNative.Peer(log);
        }

        @Override
        public void dispose() {
            log.add("dispose factory");
            disposed.countDown();
        }
    }
}

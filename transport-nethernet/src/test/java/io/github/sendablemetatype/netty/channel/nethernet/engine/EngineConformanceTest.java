package io.github.sendablemetatype.netty.channel.nethernet.engine;

import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig.Role;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What every {@link NetherNetEngine} owes the transport, tested through the
 * engine interface only, with an offering and an answering peer of one engine.
 *
 * <p>A subclass supplies the engine. Tests that need packets on real sockets
 * (addresses in path events, metric values, the shared endpoint) are skipped
 * unless {@link #realNetwork()} says the engine has them; everything else is
 * what a fake engine must do too.
 */
@Timeout(60)
public abstract class EngineConformanceTest {
    protected static final String RELIABLE = "ReliableDataChannel";
    protected static final String UNRELIABLE = "UnreliableDataChannel";
    private static final long WAIT_SECONDS = 20;

    private NetherNetEngine engine;

    /** A new engine for one test; the suite closes it. */
    protected abstract NetherNetEngine newEngine();

    /** Whether the engine moves packets over real sockets, which some tests need. */
    protected abstract boolean realNetwork();

    @BeforeEach
    void openEngine() {
        engine = newEngine();
    }

    @AfterEach
    void closeEngine() throws Exception {
        await(engine.close());
    }

    @Test
    void trickledNegotiationOpensBothLinks() throws Exception {
        Pair pair = connect(true, answererConfig());

        assertEquals(List.of(ChannelParameters.reliable(RELIABLE), ChannelParameters.unreliable(UNRELIABLE)),
                pair.server.remoteLinks.stream().map(DataLink::parameters)
                        .sorted(Comparator.comparing(ChannelParameters::label)).toList());
        assertEquals(0, pair.client.earlyCandidates.get(), "A local candidate came before the offer was returned");
        assertEquals(0, pair.server.earlyCandidates.get(), "A local candidate came before the answer was returned");
        assertFalse(pair.client.localCandidates.isEmpty());
        assertFalse(pair.server.localCandidates.isEmpty());
    }

    @Test
    void fullDescriptionNegotiationOpensBothLinks() throws Exception {
        Pair pair = connect(false, answererConfig());

        assertEquals(2, pair.server.remoteLinks.size());
        assertTrue(pair.client.gatheringComplete.isDone());
        assertTrue(pair.server.gatheringComplete.isDone());
    }

    @Test
    void messagesFlowBothWaysOnBothLinksAndEverySendCompletes() throws Exception {
        Pair pair = connect(true, answererConfig());
        List<Throwable> results = Collections.synchronizedList(new ArrayList<>());

        for (Side from : List.of(pair.client, pair.server)) {
            Side to = from == pair.client ? pair.server : pair.client;
            byte[] reliable = (from.name + " reliable").getBytes(StandardCharsets.UTF_8);
            ByteBuffer window = ByteBuffer.allocate(reliable.length + 4).position(2);
            window.put(reliable).flip().position(2);
            from.links.get(RELIABLE).send(window, results::add);
            assertEquals(new Received(RELIABLE, reliable), to.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS));

            // No retransmission on this link; a few copies make loss on one host irrelevant.
            byte[] unreliable = (from.name + " unreliable").getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 3; i++) {
                from.links.get(UNRELIABLE).send(ByteBuffer.wrap(unreliable), results::add);
            }
            assertEquals(new Received(UNRELIABLE, unreliable), to.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        }

        awaitCondition(() -> results.size() == 8, "Not every send completed");
        // A copy that keeps the nulls of successful sends; List.copyOf rejects them.
        assertEquals(Collections.nCopies(8, null), new ArrayList<>(results));
    }

    @Test
    void queuedBytesDrainAndTheWatermarkFires() throws Exception {
        Pair pair = connect(true, answererConfig());
        DataLink link = pair.client.links.get(RELIABLE);
        link.lowWatermark(64 * 1024);
        int messages = 64;
        byte[] payload = new byte[64 * 1024];

        for (int i = 0; i < messages; i++) {
            link.send(ByteBuffer.wrap(payload), ignored -> {
            });
        }
        long queuedAfterBurst = link.queuedBytes();
        for (int i = 0; i < messages; i++) {
            assertNotNull(pair.server.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS), "Message " + i + " never arrived");
        }

        awaitCondition(() -> link.queuedBytes() == 0, "Queued bytes did not drain");
        if (queuedAfterBurst > 64 * 1024) {
            assertTrue(pair.client.queueLow.tryAcquire(WAIT_SECONDS, TimeUnit.SECONDS), "No queue low event");
        }
        if (realNetwork()) {
            assertTrue(queuedAfterBurst > 0, "A 4 MiB burst was never queued");
        }
    }

    @Test
    void aWatermarkSetWhileTheQueueIsAboveItFiresOnTheNextDrop() throws Exception {
        Pair pair = connect(true, answererConfig());
        DataLink link = pair.client.links.get(RELIABLE);
        long level = 64 * 1024;
        // No queue rises above this level, so the burst itself fires nothing.
        link.lowWatermark(Long.MAX_VALUE);
        for (int i = 0; i < 64; i++) {
            link.send(ByteBuffer.wrap(new byte[64 * 1024]), ignored -> {
            });
        }

        link.lowWatermark(level);
        // Read after the level was set: a drop this reading has not seen yet is held to the new level.
        assumeTrue(link.queuedBytes() > level, "The queue drained before the level was set");
        assertTrue(pair.client.queueLow.tryAcquire(WAIT_SECONDS, TimeUnit.SECONDS), "No queue low event");
    }

    @Test
    void theCloseStageWaitsForASendCompletionStillRunning() throws Exception {
        Pair pair = connect(true, answererConfig());
        DataLink link = pair.server.links.get(RELIABLE);
        CompletableFuture<Void> running = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<Void> closing;
        // A thread of its own: the completion may run inside send.
        Thread sender = Thread.ofVirtual().start(() -> link.send(ByteBuffer.allocate(1), ignored -> {
            running.complete(null);
            release.join();
        }));
        try {
            running.get(WAIT_SECONDS, TimeUnit.SECONDS);
            closing = pair.server.peer.close().toCompletableFuture();
            assertThrows(TimeoutException.class, () -> closing.get(200, TimeUnit.MILLISECONDS),
                    "The close stage completed while a send completion ran");
        } finally {
            release.complete(null);
            sender.join();
        }
        await(closing);
    }

    @Test
    void aRemoteCloseEndsTheLinksWithARemoteCause() throws Exception {
        Pair pair = connect(true, answererConfig());

        await(pair.client.peer.close());

        Closed closed = pair.server.channelClosed(RELIABLE);
        // libwebrtc aborts the SCTP association when a peer closes; the
        // stream resets of its data channels may arrive first.
        assertTrue(EnumSet.of(CloseCause.REMOTE_CLOSED, CloseCause.REMOTE_ABORTED).contains(closed.cause()), closed.toString());
    }

    @Test
    void aRefusedRemoteLinkIsClosed() throws Exception {
        Side client = new Side("client");
        Side server = new Side("server");
        server.refused.add(UNRELIABLE);
        Peer offerer = engine.createPeer(config(Role.OFFERER), client);
        Peer answerer = engine.createPeer(answererConfig(), server);
        client.links.put(RELIABLE, offerer.openChannel(ChannelParameters.reliable(RELIABLE)));
        client.links.put(UNRELIABLE, offerer.openChannel(ChannelParameters.unreliable(UNRELIABLE)));
        negotiateTrickled(client, offerer, server, answerer);

        assertTrue(EnumSet.of(CloseCause.REMOTE_CLOSED, CloseCause.REMOTE_ABORTED)
                .contains(client.channelClosed(UNRELIABLE).cause()));
        await(server.opened(RELIABLE));
        assertNull(server.links.get(UNRELIABLE));
    }

    @Test
    void aLocalCloseSettlesSendsFirstAndSilencesTheListener() throws Exception {
        Pair pair = connect(true, answererConfig());
        DataLink link = pair.server.links.get(RELIABLE);
        AtomicInteger settled = new AtomicInteger();
        int sends = 32;
        for (int i = 0; i < sends; i++) {
            link.send(ByteBuffer.allocate(16 * 1024), ignored -> settled.incrementAndGet());
        }

        CompletionStage<Void> closing = pair.server.peer.close();
        assertSame(closing, pair.server.peer.close());
        await(closing.thenRun(() -> {
            pair.server.terminated = true;
            pair.server.settledAtTermination = settled.get();
        }));

        assertEquals(sends, pair.server.settledAtTermination);
        CompletableFuture<Throwable> late = new CompletableFuture<>();
        link.send(ByteBuffer.allocate(1), late::complete);
        assertNotNull(late.get(WAIT_SECONDS, TimeUnit.SECONDS), "A send on a closed peer succeeded");
        assertEquals(TransportMetrics.NONE, await(pair.server.peer.metrics()));
        await(pair.client.peer.close());
        Thread.sleep(200);
        assertEquals(0, pair.server.lateCallbacks.get(), "A listener callback followed the close stage");
        assertFalse(pair.server.closed.isDone(), "onClosed fired after a local close");
    }

    @Test
    void aPeerClosedFromItsOwnCallbackTerminates() throws Exception {
        Side client = new Side("client");
        Side server = new Side("server");
        CompletableFuture<CompletionStage<Void>> closedInCallback = new CompletableFuture<>();
        Peer offerer = engine.createPeer(config(Role.OFFERER), client);
        Peer answerer = engine.createPeer(answererConfig(), server);
        server.onOpen = side -> closedInCallback.complete(answerer.close());
        client.links.put(RELIABLE, offerer.openChannel(ChannelParameters.reliable(RELIABLE)));
        client.links.put(UNRELIABLE, offerer.openChannel(ChannelParameters.unreliable(UNRELIABLE)));

        negotiateTrickled(client, offerer, server, answerer);

        await(closedInCallback.get(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void anUnreadableOfferFailsTheNegotiation() throws Exception {
        Side server = new Side("server");
        Peer answerer = engine.createPeer(answererConfig(), server);

        CompletableFuture<String> answer = answerer.answer("this is no session description").toCompletableFuture();

        assertThrows(ExecutionException.class, () -> answer.get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(CloseCause.NEGOTIATION_FAILED, server.closed.get(WAIT_SECONDS, TimeUnit.SECONDS));
        await(answerer.close());
    }

    @Test
    void closingTheEngineClosesItsPeers() throws Exception {
        Pair pair = connect(true, answererConfig());

        await(engine.close());

        assertTrue(pair.client.peer.close().toCompletableFuture().isDone());
        assertTrue(pair.server.peer.close().toCompletableFuture().isDone());
        // Idempotent: a later close finds the shutdown done.
        assertTrue(engine.close().toCompletableFuture().isDone());
        assertThrows(IllegalStateException.class, () -> engine.createPeer(answererConfig(), new Side("late")));
    }

    @Test
    void pathEventsCarryTheRealAddresses() throws Exception {
        assumeTrue(realNetwork(), "Needs real sockets");
        Pair pair = connect(true, answererConfig());

        awaitCondition(() -> {
            TransportPath client = pair.client.lastPath;
            TransportPath server = pair.server.lastPath;
            return client != null && server != null && client.local() != null
                    && client.local().equals(server.remote()) && server.local() != null
                    && server.local().equals(client.remote());
        }, "The two ends do not see the same pair: " + pair.client.lastPath + " / " + pair.server.lastPath);
    }

    @Test
    void metricsArePresentOnceConnected() throws Exception {
        assumeTrue(realNetwork(), "Needs real sockets");
        Pair pair = connect(true, answererConfig());

        awaitCondition(() -> {
            TransportMetrics metrics = await(pair.client.peer.metrics());
            return metrics.iceRoundTripTime().isPresent() && metrics.sctp().isPresent();
        }, "No ICE round-trip time or SCTP metrics");
        assertTrue(await(pair.server.peer.metrics()).sctp().orElseThrow().packetsReceived() > 0);
    }

    @Test
    void peersShareOneEndpointPort() throws Exception {
        assumeTrue(realNetwork(), "Needs real sockets");
        // The engine closes the endpoint with itself.
        IceEndpoint endpoint = await(engine.openEndpoint(new EndpointConfig(freeUdpPort(), List.of())));
        Pair first = connect(true, endpointConfig(endpoint, null, null));
        Pair second = connect(true, endpointConfig(endpoint, null, null));

        for (Pair pair : List.of(first, second)) {
            await(pair.server.gatheringComplete);
            for (String candidate : pair.server.localCandidates) {
                assertEquals(endpoint.port(), port(candidate), candidate);
            }
            awaitCondition(() -> pair.server.lastPath != null && pair.server.lastPath.local() != null, "No path");
            assertEquals(endpoint.port(), pair.server.lastPath.local().getPort());
        }
        byte[] message = "second".getBytes(StandardCharsets.UTF_8);
        second.client.links.get(RELIABLE).send(ByteBuffer.wrap(message), ignored -> {
        });
        assertEquals(new Received(RELIABLE, message), second.server.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertTrue(first.server.messages.isEmpty());
        assertTrue(await(endpoint.stats()).sessions() >= 2);
    }

    @Test
    void aHeldCheckIsTakenByThePeerCreatedForIt() throws Exception {
        assumeTrue(realNetwork(), "Needs real sockets");
        IceEndpoint endpoint = await(engine.openEndpoint(new EndpointConfig(freeUdpPort(), List.of())));
        DtlsIdentity identity = TestIdentity.generate();
        IceCredentials credentials = new IceCredentials("held" + Long.toHexString(System.nanoTime()),
                "heldcheckpassword" + Long.toHexString(System.nanoTime()));
        try (ExecutorService handlers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<String> hosts = endpointHosts(endpoint);

            Side client = new Side("client");
            Peer offerer = engine.createPeer(config(Role.OFFERER), client);
            client.links.put(RELIABLE, offerer.openChannel(ChannelParameters.reliable(RELIABLE)));
            client.links.put(UNRELIABLE, offerer.openChannel(ChannelParameters.unreliable(UNRELIABLE)));
            String offer = await(offerer.offer());

            // The answer a server with these credentials and this identity
            // gives, from a peer that never touches the endpoint, as an
            // assisted join's client would compute it.
            Peer template = engine.createPeer(new PeerConfig(Role.ANSWERER, List.of(), CandidatePolicy.DEFAULT,
                    null, identity, credentials), new Side("template"));
            String answer = await(template.answer(offer));
            await(template.close());

            Side server = new Side("server");
            CompletableFuture<IceEndpoint.HeldCheck> held = new CompletableFuture<>();
            endpoint.onHeldCheck(check -> {
                if (held.complete(check)) {
                    Peer created = engine.createPeer(endpointConfig(endpoint, identity, credentials), server);
                    server.peer = created;
                    created.answer(offer);
                }
            }, handlers);

            await(offerer.applyAnswer(answer));
            String mid = mid(offer);
            for (String host : hosts) {
                offerer.addRemoteCandidate("candidate:1 1 udp 2122260223 " + host + " " + endpoint.port() + " typ host", mid);
            }

            IceEndpoint.HeldCheck check = held.get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertEquals(credentials.ufrag(), check.localUfrag());
            await(client.opened(RELIABLE));
            await(server.opened(RELIABLE));
            // Peers go before the handler's executor.
            await(engine.close());
        }
    }

    // Helpers

    private Pair connect(boolean trickle, PeerConfig serverConfig) throws Exception {
        Side client = new Side("client");
        Side server = new Side("server");
        client.peer = engine.createPeer(config(Role.OFFERER), client);
        server.peer = engine.createPeer(serverConfig, server);
        client.links.put(RELIABLE, client.peer.openChannel(ChannelParameters.reliable(RELIABLE)));
        client.links.put(UNRELIABLE, client.peer.openChannel(ChannelParameters.unreliable(UNRELIABLE)));
        if (trickle) {
            negotiateTrickled(client, client.peer, server, server.peer);
        } else {
            client.description = client.peer.offer().toCompletableFuture();
            await(client.description);
            String offer = await(client.peer.gatheredLocalDescription());
            assertTrue(offer.contains("a=end-of-candidates"), offer);
            server.description = server.peer.answer(offer).toCompletableFuture();
            await(server.description);
            String answer = await(server.peer.gatheredLocalDescription());
            assertTrue(answer.contains("a=end-of-candidates"), answer);
            await(client.peer.applyAnswer(answer));
        }
        for (String label : List.of(RELIABLE, UNRELIABLE)) {
            await(client.opened(label));
            await(server.opened(label));
        }
        return new Pair(client, server);
    }

    private static void negotiateTrickled(Side client, Peer offerer, Side server, Peer answerer) throws Exception {
        client.description = offerer.offer().toCompletableFuture();
        String offer = await(client.description);
        // The client's candidates may reach the server before its remote description.
        client.forwardTo(answerer);
        server.description = answerer.answer(offer).toCompletableFuture();
        String answer = await(server.description);
        server.forwardTo(offerer);
        await(offerer.applyAnswer(answer));
    }

    /** The host addresses peers gather on the endpoint, as a server would publish them. */
    private List<String> endpointHosts(IceEndpoint endpoint) throws Exception {
        Pair probe = connect(true, endpointConfig(endpoint, null, null));
        await(probe.server.gatheringComplete);
        List<String> hosts = probe.server.localCandidates.stream()
                .map(candidate -> candidate.split(" ")[4])
                .distinct()
                .toList();
        await(probe.client.peer.close());
        await(probe.server.peer.close());
        assertFalse(hosts.isEmpty(), "No host candidates on the endpoint");
        return hosts;
    }

    private static PeerConfig config(Role role) {
        return new PeerConfig(role, List.of(), CandidatePolicy.DEFAULT, null, null, null);
    }

    private static PeerConfig answererConfig() {
        return config(Role.ANSWERER);
    }

    private static PeerConfig endpointConfig(IceEndpoint endpoint, DtlsIdentity identity, IceCredentials credentials) {
        return new PeerConfig(Role.ANSWERER, List.of(), CandidatePolicy.DEFAULT, endpoint, identity, credentials);
    }

    private static int port(String candidate) {
        return Integer.parseInt(candidate.split(" ")[5]);
    }

    private static String mid(String sdp) {
        return sdp.lines().filter(line -> line.startsWith("a=mid:")).findFirst().orElseThrow().substring(6);
    }

    private static int freeUdpPort() throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("A stage did not complete", e);
        }
    }

    private static void awaitCondition(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, message);
            Thread.sleep(20);
        }
    }

    private record Pair(Side client, Side server) {
    }

    private record Received(String label, byte[] bytes) {
        @Override
        public boolean equals(Object other) {
            return other instanceof Received(String otherLabel, byte[] otherBytes)
                    && label.equals(otherLabel) && Arrays.equals(bytes, otherBytes);
        }

        @Override
        public int hashCode() {
            return label.hashCode() * 31 + Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return label + ":" + new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private record Closed(String label, CloseCause cause) {
    }

    /** One peer's listener: records its events and forwards its candidates once told where. */
    private static final class Side implements PeerListener {
        final String name;
        volatile Peer peer;
        volatile CompletableFuture<String> description;
        final AtomicInteger earlyCandidates = new AtomicInteger();
        final List<String> localCandidates = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<Void> gatheringComplete = new CompletableFuture<>();
        final Map<String, DataLink> links = new ConcurrentHashMap<>();
        final Map<String, CompletableFuture<DataLink>> opened = new ConcurrentHashMap<>();
        final List<DataLink> remoteLinks = Collections.synchronizedList(new ArrayList<>());
        final Set<String> refused = ConcurrentHashMap.newKeySet();
        final BlockingQueue<Received> messages = new LinkedBlockingQueue<>();
        final Semaphore queueLow = new Semaphore(0);
        final BlockingQueue<Closed> channelsClosed = new LinkedBlockingQueue<>();
        volatile TransportPath lastPath;
        final CompletableFuture<CloseCause> closed = new CompletableFuture<>();
        volatile boolean terminated;
        volatile int settledAtTermination;
        final AtomicInteger lateCallbacks = new AtomicInteger();
        volatile Consumer<Side> onOpen = side -> {
        };

        // Guarded by this.
        private Peer forwardTarget;
        private final List<String[]> pending = new ArrayList<>();

        Side(String name) {
            this.name = name;
        }

        CompletableFuture<DataLink> opened(String label) {
            return opened.computeIfAbsent(label, ignored -> new CompletableFuture<>());
        }

        Closed channelClosed(String label) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (true) {
                Closed closed = channelsClosed.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                assertNotNull(closed, name + " saw no close of " + label);
                if (closed.label().equals(label)) {
                    return closed;
                }
            }
        }

        synchronized void forwardTo(Peer target) {
            forwardTarget = target;
            for (String[] candidate : pending) {
                target.addRemoteCandidate(candidate[0], candidate[1]);
            }
            pending.clear();
        }

        private void callback() {
            if (terminated) {
                lateCallbacks.incrementAndGet();
            }
        }

        @Override
        public void onLocalCandidate(String candidate, String mid) {
            callback();
            CompletableFuture<String> returned = description;
            if (returned != null && !returned.isDone()) {
                earlyCandidates.incrementAndGet();
            }
            localCandidates.add(candidate);
            synchronized (this) {
                if (forwardTarget == null) {
                    pending.add(new String[]{candidate, mid});
                    return;
                }
                forwardTarget.addRemoteCandidate(candidate, mid);
            }
        }

        @Override
        public void onGatheringComplete() {
            callback();
            gatheringComplete.complete(null);
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            callback();
            remoteLinks.add(link);
            String label = link.parameters().label();
            if (refused.contains(label)) {
                return false;
            }
            links.put(label, link);
            return true;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            callback();
            opened(link.parameters().label()).complete(link);
            onOpen.accept(this);
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            callback();
            byte[] bytes = new byte[message.remaining()];
            message.duplicate().get(bytes);
            messages.add(new Received(link.parameters().label(), bytes));
        }

        @Override
        public void onQueueLow(DataLink link) {
            callback();
            queueLow.release();
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
            callback();
            channelsClosed.add(new Closed(link.parameters().label(), cause));
        }

        @Override
        public void onPathSelected(TransportPath path) {
            callback();
            lastPath = path;
        }

        @Override
        public void onStateChange(PeerState state) {
            callback();
        }

        @Override
        public void onIceStateChange(IceState state) {
            callback();
        }

        @Override
        public void onClosed(CloseCause cause) {
            callback();
            closed.complete(cause);
        }
    }

    /** A fresh ECDSA P-256 certificate with a SHA-256 signature, made with the JDK's keytool. */
    static final class TestIdentity {
        private TestIdentity() {
        }

        static DtlsIdentity generate() throws Exception {
            Path directory = Files.createTempDirectory("nethernet-identity");
            Path store = directory.resolve("identity.p12");
            char[] password = "conformance".toCharArray();
            boolean windows = System.getProperty("os.name").startsWith("Windows");
            Path keytool = Path.of(System.getProperty("java.home"), "bin", windows ? "keytool.exe" : "keytool");
            Process process = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "dtls",
                    "-keyalg", "EC", "-groupname", "secp256r1", "-sigalg", "SHA256withECDSA",
                    "-dname", "CN=NetherNet conformance", "-validity", "30", "-storetype", "PKCS12",
                    "-keystore", store.toString(), "-storepass", new String(password))
                    .redirectErrorStream(true)
                    .start();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            process.getInputStream().transferTo(output);
            assertEquals(0, process.waitFor(), output.toString(StandardCharsets.UTF_8));

            KeyStore keys = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(store)) {
                keys.load(in, password);
            }
            PrivateKey key = (PrivateKey) keys.getKey("dtls", password);
            Certificate certificate = keys.getCertificate("dtls");
            Files.delete(store);
            Files.delete(directory);
            return new DtlsIdentity(pem("CERTIFICATE", certificate.getEncoded()), pem("PRIVATE KEY", key.getEncoded()));
        }

        private static String pem(String type, byte[] der) {
            String body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
            return "-----BEGIN " + type + "-----\n" + body + "\n-----END " + type + "-----\n";
        }
    }
}

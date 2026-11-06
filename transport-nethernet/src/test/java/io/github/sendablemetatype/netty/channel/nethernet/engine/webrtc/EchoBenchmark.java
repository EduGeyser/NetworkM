package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig.Role;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Measures a NetherNet echo through libwebrtc on one machine, to compare
 * webrtc-java builds: the round-trip time of one message at a time, and how
 * many messages per second come back with a window of them in flight.
 *
 * <p>A client engine and a server engine, with one webrtc-java factory each,
 * connect peer pairs over the host's own addresses and open NetherNet's two
 * data channels. The server sends every message on the reliable channel
 * straight back from its callback. With a window, the client sends the next
 * message from its callback too, so no thread of this tool is in the loop.
 * The output names the thread the data channel events arrived on, so a run
 * shows which delivery it measured.
 *
 * <p>Not a test: it runs only through the {@code echoBenchmark} task, which
 * needs natives, for example {@code ./gradlew :transport-nethernet:echoBenchmark
 * -PwebrtcNativePlatform=windows-x86_64}. Settings go in
 * {@code --args="pings=50000 pairs=1,4"}; see {@link Settings} for all of them.
 */
public final class EchoBenchmark {
    private static final String RELIABLE = "ReliableDataChannel";
    private static final String UNRELIABLE = "UnreliableDataChannel";
    private static final long WAIT_SECONDS = 30;

    private EchoBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.parse(args);
        int pairCount = Arrays.stream(settings.pairs).max().orElse(1);

        NetherNetEngine clientEngine = WebRtcEngine.builder().factories(1).build();
        NetherNetEngine serverEngine = WebRtcEngine.builder().factories(1).build();
        List<Pair> pairs = new ArrayList<>();
        try {
            for (int i = 0; i < pairCount; i++) {
                pairs.add(Pair.connect(clientEngine, serverEngine, settings.size));
            }
            Pair first = pairs.getFirst();
            first.latency(settings.warmup);
            first.window(settings.warmup, settings.window);

            System.out.println("NetherNet echo through libwebrtc");
            System.out.println("machine:  " + machine());
            System.out.println("natives:  " + System.getProperty("echoBenchmark.natives", "?"));
            System.out.println("delivery: client " + first.client.delivery() + ", server " + first.server.delivery());
            System.out.println("path:     " + first.client.path);
            System.out.println("settings: " + settings);

            for (int round = 1; round <= settings.rounds; round++) {
                long[] times = first.latency(settings.pings);
                Arrays.sort(times);
                System.out.printf(Locale.ROOT, "round %d  round trip (us): mean %.1f  p50 %.1f  p90 %.1f  p99 %.1f"
                                + "  max %.1f%n", round, Arrays.stream(times).average().orElse(0) / 1e3,
                        percentile(times, 50), percentile(times, 90), percentile(times, 99),
                        times[times.length - 1] / 1e3);
                for (int count : settings.pairs) {
                    double perSecond = windowed(pairs.subList(0, count), settings.messages, settings.window);
                    System.out.printf(Locale.ROOT, "round %d  echoed, %d pair%s, window %d: %.0f messages/s%n",
                            round, count, count == 1 ? "" : "s", settings.window, perSecond);
                }
            }
            long failed = pairs.stream()
                    .mapToLong(pair -> pair.client.failedSends.get() + pair.server.failedSends.get())
                    .sum();
            if (failed > 0) {
                System.out.println("failed sends: " + failed);
            }
        } finally {
            for (Pair pair : pairs) {
                pair.close();
            }
            await(clientEngine.close());
            await(serverEngine.close());
        }
    }

    /** Messages per second the pairs echo together, each with a window of messages in flight. */
    private static double windowed(List<Pair> pairs, int messagesPerPair, int window) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(pairs.size());
        long start = System.nanoTime();
        for (Pair pair : pairs) {
            pair.startWindow(messagesPerPair, window, done);
        }
        if (!done.await(WAIT_SECONDS * 10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Echoes stopped coming back");
        }
        long elapsed = System.nanoTime() - start;
        return (double) messagesPerPair * pairs.size() / elapsed * 1e9;
    }

    private static double percentile(long[] sorted, int percent) {
        int index = (int) Math.ceil(percent / 100.0 * sorted.length) - 1;
        return sorted[Math.clamp(index, 0, sorted.length - 1)] / 1e3;
    }

    private static String machine() {
        Runtime runtime = Runtime.getRuntime();
        return System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch") + ", " + cpu() + ", " + runtime.availableProcessors()
                + " logical processors, Java " + System.getProperty("java.runtime.version") + " ("
                + System.getProperty("java.vm.vendor") + ")";
    }

    private static String cpu() {
        String windows = System.getenv("PROCESSOR_IDENTIFIER");
        if (windows != null) {
            return windows;
        }
        try {
            return Files.readAllLines(Path.of("/proc/cpuinfo")).stream()
                    .filter(line -> line.startsWith("model name"))
                    .map(line -> line.substring(line.indexOf(':') + 1).trim())
                    .findFirst().orElse("unknown CPU");
        } catch (IOException | RuntimeException e) {
            return "unknown CPU";
        }
    }

    private static PeerConfig config(Role role) {
        return new PeerConfig(role, List.of(), CandidatePolicy.DEFAULT, null, null, null);
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * The settings, as {@code name=value} arguments.
     *
     * @param size bytes per message ({@code size}, default 256)
     * @param warmup round trips and echoed messages before measuring ({@code warmup}, default 5000)
     * @param pings round trips measured one at a time per round ({@code pings}, default 20000)
     * @param messages echoed messages per pair and round with a window ({@code messages}, default 200000)
     * @param window messages each pair keeps in flight ({@code window}, default 64)
     * @param rounds how often both measurements repeat ({@code rounds}, default 3)
     * @param pairs the numbers of pairs that echo at once, one window measurement each
     *        ({@code pairs}, default 1,8); all pairs share the two engines' factories
     */
    record Settings(int size, int warmup, int pings, int messages, int window, int rounds, int[] pairs) {

        static Settings parse(String[] args) {
            Map<String, String> values = new LinkedHashMap<>(Map.of("size", "256", "warmup", "5000",
                    "pings", "20000", "messages", "200000", "window", "64", "rounds", "3", "pairs", "1,8"));
            for (String arg : args) {
                int equals = arg.indexOf('=');
                if (equals < 0 || !values.containsKey(arg.substring(0, equals))) {
                    throw new IllegalArgumentException("Unknown setting: " + arg + "; known: " + values.keySet());
                }
                values.put(arg.substring(0, equals), arg.substring(equals + 1));
            }
            int[] pairs = Arrays.stream(values.get("pairs").split(",")).mapToInt(Integer::parseInt).toArray();
            return new Settings(Integer.parseInt(values.get("size")), Integer.parseInt(values.get("warmup")),
                    Integer.parseInt(values.get("pings")), Integer.parseInt(values.get("messages")),
                    Integer.parseInt(values.get("window")), Integer.parseInt(values.get("rounds")), pairs);
        }

        @Override
        public String toString() {
            return "size=" + size + " B, warmup=" + warmup + ", pings=" + pings + ", messages=" + messages
                    + ", window=" + window + ", rounds=" + rounds + ", pairs=" + Arrays.toString(pairs);
        }
    }

    /** A connected client and server peer. */
    private record Pair(Peer offerer, Peer answerer, Side client, Side server) {

        static Pair connect(NetherNetEngine clientEngine, NetherNetEngine serverEngine, int size) throws Exception {
            Side client = new Side(false, size);
            Side server = new Side(true, size);
            Peer offerer = clientEngine.createPeer(config(Role.OFFERER), client);
            Peer answerer = serverEngine.createPeer(config(Role.ANSWERER), server);
            offerer.openChannel(ChannelParameters.reliable(RELIABLE));
            offerer.openChannel(ChannelParameters.unreliable(UNRELIABLE));
            String offer = await(offerer.offer());
            client.forwardTo(answerer);
            String answer = await(answerer.answer(offer));
            server.forwardTo(offerer);
            await(offerer.applyAnswer(answer));
            await(client.reliable);
            await(server.reliable);
            return new Pair(offerer, answerer, client, server);
        }

        /** Round trips of one message at a time, in nanoseconds. */
        long[] latency(int count) throws Exception {
            DataLink link = await(client.reliable);
            client.waiter = Thread.currentThread();
            long[] times = new long[count];
            for (int i = 0; i < count; i++) {
                long target = client.received.get() + 1;
                long start = System.nanoTime();
                link.send(client.message, client.sendResult);
                while (client.received.get() < target) {
                    LockSupport.park(this);
                }
                times[i] = System.nanoTime() - start;
            }
            client.waiter = null;
            return times;
        }

        void window(int messages, int window) throws Exception {
            CountDownLatch done = new CountDownLatch(1);
            startWindow(messages, window, done);
            if (!done.await(WAIT_SECONDS * 10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Echoes stopped coming back");
            }
        }

        void startWindow(int messages, int window, CountDownLatch done) throws InterruptedException {
            DataLink link;
            try {
                link = await(client.reliable);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            client.startWindow(link, messages, done);
            for (int i = 0; i < Math.min(window, messages); i++) {
                client.sendNext();
            }
        }

        void close() throws Exception {
            await(offerer.close());
            await(answerer.close());
        }
    }

    /**
     * One side's listener. The server echoes; the client counts echoes, wakes
     * the measuring thread, and with a window sends the next message.
     */
    private static final class Side implements PeerListener {
        private final boolean server;
        final ByteBuffer message;
        final CompletableFuture<DataLink> reliable = new CompletableFuture<>();
        final AtomicLong received = new AtomicLong();
        final AtomicLong failedSends = new AtomicLong();
        final Consumer<Throwable> sendResult = failure -> {
            if (failure != null) {
                failedSends.incrementAndGet();
            }
        };
        volatile Thread waiter;
        volatile TransportPath path;
        private volatile Thread peerThread;
        private volatile Thread linkThread;

        // The window: set up before its first send. Echoes are counted on the
        // receiving thread only; sends also go out from the measuring thread.
        private volatile DataLink windowLink;
        private volatile CountDownLatch windowDone;
        private volatile long windowMessages;
        private volatile long windowReceived;
        private final AtomicLong windowSent = new AtomicLong();

        // Guarded by this.
        private Peer forwardTarget;
        private final List<String[]> pending = new ArrayList<>();

        Side(boolean server, int size) {
            this.server = server;
            // Never changed after this, so threads may send it at once: sends only read it.
            message = ByteBuffer.allocateDirect(size);
            for (int i = 0; i < size; i++) {
                message.put(i, (byte) i);
            }
        }

        /** Where the data channel's messages arrived, next to the peer connection's events. */
        String delivery() {
            Thread link = linkThread;
            Thread peer = peerThread;
            if (link == null || peer == null) {
                return "unknown";
            }
            return link == peer ? "on the signaling thread" : "off the signaling thread";
        }

        synchronized void forwardTo(Peer target) {
            forwardTarget = target;
            for (String[] candidate : pending) {
                target.addRemoteCandidate(candidate[0], candidate[1]);
            }
            pending.clear();
        }

        void startWindow(DataLink link, int messages, CountDownLatch done) {
            windowMessages = messages;
            windowReceived = 0;
            windowSent.set(0);
            windowDone = done;
            windowLink = link;
        }

        /** Sends the window's next message, unless all of them went out. */
        void sendNext() {
            DataLink link = windowLink;
            if (link != null && windowSent.getAndIncrement() < windowMessages) {
                link.send(message, sendResult);
            }
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            if (linkThread == null) {
                linkThread = Thread.currentThread();
            }
            if (server) {
                // The engine copies the bytes before send returns, while the message is still valid.
                link.send(message, sendResult);
                return;
            }
            received.incrementAndGet();
            Thread waiting = waiter;
            if (waiting != null) {
                LockSupport.unpark(waiting);
            }
            if (windowLink != null) {
                long count = windowReceived + 1;
                windowReceived = count;
                if (count == windowMessages) {
                    windowLink = null;
                    windowDone.countDown();
                } else {
                    sendNext();
                }
            }
        }

        @Override
        public synchronized void onLocalCandidate(String candidate, String mid) {
            if (forwardTarget == null) {
                pending.add(new String[]{candidate, mid});
            } else {
                forwardTarget.addRemoteCandidate(candidate, mid);
            }
        }

        @Override
        public void onGatheringComplete() {
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            return true;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            if (link.parameters().label().equals(RELIABLE)) {
                reliable.complete(link);
            }
        }

        @Override
        public void onTextMessage(DataLink link, ByteBuffer message) {
        }

        @Override
        public void onQueueLow(DataLink link) {
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
        }

        @Override
        public void onPathSelected(TransportPath path) {
            this.path = path;
        }

        @Override
        public void onStateChange(PeerState state) {
            if (peerThread == null) {
                peerThread = Thread.currentThread();
            }
        }

        @Override
        public void onIceStateChange(IceState state) {
        }

        @Override
        public void onClosed(CloseCause cause) {
        }
    }
}

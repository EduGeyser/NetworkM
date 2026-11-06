package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Binding, closing and the health a bound source reports. */
@Timeout(10)
class HttpSignalingLifecycleTest {
    private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final List<EventLoopGroup> acceptGroups = new ArrayList<>();
    private final List<NetherNetHttpSignaling> listeners = new ArrayList<>();

    @AfterEach
    void tearDown() {
        listeners.forEach(NetherNetHttpSignaling::close);
        acceptGroups.forEach(group -> group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly());
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void healthFollowsBindStartAndClose() throws Exception {
        NetherNetHttpSignaling signaling = listener(newAcceptGroup());
        List<SignalingState> seen = new CopyOnWriteArrayList<>();
        signaling.addHealthListener(health -> seen.add(health.state()));
        assertEquals(SignalingState.NEW, signaling.health().state());

        signaling.bind(new InetSocketAddress("127.0.0.1", 0));
        signaling.start(new RecordingSink());
        signaling.close();

        assertEquals(List.of(SignalingState.STARTING, SignalingState.CONNECTED, SignalingState.CLOSED), seen);
        assertTrue(signaling.iceServers().isEmpty());
        assertThrows(IllegalStateException.class, () -> signaling.start(new RecordingSink()));
    }

    @Test
    void failedBindClosesWithTheFailureAndKeepsTheWorker() throws Exception {
        EventLoopGroup accept = newAcceptGroup();
        NetherNetHttpSignaling signaling = listener(accept);
        try (ServerSocket occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress("127.0.0.1", 0));
            assertThrows(ConnectException.class, () -> signaling.bind(occupied.getLocalSocketAddress()));
        }

        assertTrue(accept.terminationFuture().await(2, TimeUnit.SECONDS));
        assertNull(signaling.boundAddress());
        assertEquals(SignalingState.CLOSED, signaling.health().state());
        assertInstanceOf(ConnectException.class, signaling.health().lastFailure());
        assertFalse(worker.isShuttingDown());
        assertEquals(42, worker.submit(() -> 42).get(2, TimeUnit.SECONDS).intValue());
    }

    @Test
    void secondBindKeepsTheFirstListener() throws Exception {
        EventLoopGroup accept = newAcceptGroup();
        AtomicInteger creations = new AtomicInteger();
        NetherNetHttpSignaling signaling = NetherNetHttpSignaling.builder(worker).acceptGroupFactory(() -> {
            creations.incrementAndGet();
            return accept;
        }).build();
        listeners.add(signaling);
        signaling.start(new RecordingSink());
        signaling.bind(new InetSocketAddress("127.0.0.1", 0));
        InetSocketAddress original = signaling.boundAddress();

        assertThrows(ConnectException.class, () -> signaling.bind(new InetSocketAddress("127.0.0.1", 0)));

        assertEquals(original, signaling.boundAddress());
        assertEquals(1, creations.get());
        assertEquals(200, probe(original));
    }

    @Test
    void closedListenerCannotBeBoundAgain() throws Exception {
        EventLoopGroup accept = newAcceptGroup();
        NetherNetHttpSignaling signaling = listener(accept);
        signaling.bind(new InetSocketAddress("127.0.0.1", 0));

        signaling.close();

        assertThrows(ConnectException.class, () -> signaling.bind(new InetSocketAddress("127.0.0.1", 0)));
        assertTrue(accept.terminationFuture().await(2, TimeUnit.SECONDS));
        assertNull(signaling.boundAddress());
        assertFalse(worker.isShuttingDown());
    }

    @Test
    void bindingNeverAsksTheTlsSelector() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        NetherNetHttpSignaling signaling = NetherNetHttpSignaling.builder(worker).acceptGroupFactory(this::newAcceptGroup)
                .tls(name -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("no certificate");
                }).build();
        listeners.add(signaling);
        signaling.start(new RecordingSink());

        signaling.bind(new InetSocketAddress("127.0.0.1", 0));

        assertEquals(200, probe(signaling.boundAddress()));
        assertEquals(0, calls.get());
    }

    @Test
    void closeEndsOpenConnectionsWithoutStoppingTheWorker() throws Exception {
        EventLoopGroup accept = newAcceptGroup();
        NetherNetHttpSignaling signaling = listener(accept);
        signaling.start(new RecordingSink());
        signaling.bind(new InetSocketAddress("127.0.0.1", 0));
        InetSocketAddress address = signaling.boundAddress();
        try (Socket idle = new Socket()) {
            idle.connect(address, 2000);
            idle.setSoTimeout(2000);
            // A later connection completing a request shows the idle one reached the worker too
            assertEquals(200, probe(address));

            signaling.close();

            assertEquals(-1, idle.getInputStream().read());
            assertTrue(accept.terminationFuture().await(2, TimeUnit.SECONDS));
            assertFalse(worker.isShuttingDown());
        }
    }

    @Test
    void closeAbandonsAJoinWaitingForItsAnswer() throws Exception {
        NetherNetHttpSignaling signaling = listener(newAcceptGroup());
        RecordingSink sink = new RecordingSink();
        signaling.start(sink);
        signaling.bind(new InetSocketAddress("127.0.0.1", 0));
        try (Socket socket = SocketHttp.open(signaling.boundAddress())) {
            SocketHttp.write(socket, SocketHttp.post("42", "v=0\r\n"));
            Join join = sink.next();

            signaling.close();

            join.reply().abandoned().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void interruptedBindRestoresTheInterruptAndCleansUp() throws Exception {
        EventLoopGroup accept = newAcceptGroup();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch acquired = new CountDownLatch(1);
        block(accept, blocked, resume);
        NetherNetHttpSignaling signaling = NetherNetHttpSignaling.builder(worker).acceptGroupFactory(() -> {
            acquired.countDown();
            return accept;
        }).build();
        listeners.add(signaling);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread binder = new Thread(() -> {
            try {
                signaling.bind(new InetSocketAddress("127.0.0.1", 0));
            } catch (Throwable error) {
                failure.set(error);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        }, "test-http-bind");
        try {
            assertTrue(blocked.await(2, TimeUnit.SECONDS));
            binder.start();
            assertTrue(acquired.await(2, TimeUnit.SECONDS));
            binder.interrupt();
            binder.join(2000);

            assertFalse(binder.isAlive());
            assertInstanceOf(ConnectException.class, failure.get());
            assertInstanceOf(InterruptedException.class, failure.get().getCause());
            assertTrue(interrupted.get());
        } finally {
            resume.countDown();
            binder.interrupt();
            binder.join(2000);
        }
        assertTrue(accept.terminationFuture().await(2, TimeUnit.SECONDS));
        assertFalse(worker.isShuttingDown());
    }

    @Test
    void closeDuringBindDoesNotWaitForTheAcceptLoop() throws Exception {
        EventLoopGroup accept = newAcceptGroup();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch acquired = new CountDownLatch(1);
        block(accept, blocked, resume);
        NetherNetHttpSignaling signaling = NetherNetHttpSignaling.builder(worker).acceptGroupFactory(() -> {
            acquired.countDown();
            return accept;
        }).build();
        listeners.add(signaling);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread binder = new Thread(() -> {
            try {
                signaling.bind(new InetSocketAddress("127.0.0.1", 0));
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "test-http-bind-close-race");
        try {
            assertTrue(blocked.await(2, TimeUnit.SECONDS));
            binder.start();
            assertTrue(acquired.await(2, TimeUnit.SECONDS));

            worker.submit(signaling::close).get(2, TimeUnit.SECONDS);

            assertFalse(worker.isShuttingDown());
            resume.countDown();
            binder.join(2000);
            assertFalse(binder.isAlive());
            assertInstanceOf(ConnectException.class, failure.get());
        } finally {
            resume.countDown();
            binder.interrupt();
            binder.join(2000);
        }
        assertTrue(accept.terminationFuture().await(2, TimeUnit.SECONDS));
        assertNull(signaling.boundAddress());
        assertSame(SignalingState.CLOSED, signaling.health().state());
    }

    private static int probe(InetSocketAddress address) throws Exception {
        try (Socket socket = SocketHttp.open(address)) {
            SocketHttp.write(socket, SocketHttp.get("/v1/join", "Connection: close\r\n"));
            return SocketHttp.read(socket).status();
        }
    }

    private static void block(EventLoopGroup group, CountDownLatch blocked, CountDownLatch resume) {
        group.execute(() -> {
            blocked.countDown();
            try {
                resume.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private EventLoopGroup newAcceptGroup() {
        EventLoopGroup accept = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        acceptGroups.add(accept);
        return accept;
    }

    private NetherNetHttpSignaling listener(EventLoopGroup accept) {
        NetherNetHttpSignaling signaling = NetherNetHttpSignaling.builder(worker).acceptGroupFactory(() -> accept).build();
        listeners.add(signaling);
        return signaling;
    }
}

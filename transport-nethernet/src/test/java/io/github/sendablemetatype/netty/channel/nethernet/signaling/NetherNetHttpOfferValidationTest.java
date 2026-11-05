package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.OfferPolicy;
import io.github.sendablemetatype.netty.util.nethernet.TrustedIssuer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class NetherNetHttpOfferValidationTest {
    private final MultiThreadIoEventLoopGroup worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final ManualExecutor executor = new ManualExecutor();
    private final AtomicInteger executorCreations = new AtomicInteger();
    private final NetherNetHttpSignaling signaling = new NetherNetHttpSignaling(() -> null, worker, () -> worker, () -> {
        executorCreations.incrementAndGet();
        return executor;
    });
    private final List<EmbeddedChannel> channels = new ArrayList<>();
    private final List<OfferCheckResult> admitted = new ArrayList<>();

    private EmbeddedChannel offer(String sdp) {
        signaling.setNewConnectionHandler((connection, network, payload) -> {
            admitted.add(signaling.offerCheckOf(connection));
            signaling.sendSignal(network, NetherNetConstants.buildSignalConnectResponse(connection, "answer"));
        });
        EmbeddedChannel channel = new EmbeddedChannel() {
            @Override protected SocketAddress remoteAddress0() { return new InetSocketAddress("127.0.0.1", 1234); }
        };
        channels.add(channel);
        channel.freezeTime();
        signaling.initConnection(channel);
        String request = "POST /v1/join/peer HTTP/1.1\r\nHost: localhost\r\nContent-Length: "
                + sdp.getBytes(StandardCharsets.UTF_8).length + "\r\n\r\n" + sdp;
        channel.writeInbound(Unpooled.copiedBuffer(request, StandardCharsets.UTF_8));
        channel.runPendingTasks();
        return channel;
    }

    @AfterEach
    void close() {
        signaling.close();
        channels.forEach(EmbeddedChannel::finishAndReleaseAll);
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void defaultPolicyRefusesOffersWithoutAnAssertion() throws Exception {
        EmbeddedChannel channel = offer("v=0\r\n");
        assertTrue(admitted.isEmpty());
        executor.finishNext();
        channel.runPendingTasks();
        assertTrue(response(channel).startsWith("HTTP/1.1 401"));
        assertTrue(admitted.isEmpty());
    }

    @Test
    void admissionWaitsForTheCheckAndReceivesItsResult() throws Exception {
        signaling.setOfferCheck(retailCheck());
        EmbeddedChannel channel = offer(retailOffer());
        assertTrue(admitted.isEmpty());
        assertNull(channel.readOutbound());
        executor.finishNext();
        assertTrue(admitted.isEmpty(), "The check must marshal admission back to the HTTP loop");
        channel.runPendingTasks();
        assertEquals(1, admitted.size());
        assertEquals(OfferCheckResult.Level.ATTESTED, admitted.getFirst().level());
        assertTrue(response(channel).startsWith("HTTP/1.1 200"));
    }

    @Test
    void educationOffersAreAdmittedByDefault() throws Exception {
        signaling.setOfferCheck(retailCheck());
        EmbeddedChannel channel = offer(educationOffer());
        executor.finishNext();
        channel.runPendingTasks();
        assertEquals(1, admitted.size());
        assertEquals(OfferCheckResult.Level.KEY_PROVEN, admitted.getFirst().level());
        assertTrue(response(channel).startsWith("HTTP/1.1 200"));
    }

    @Test
    void policyRequiringAttestationRefusesSelfSignedTokens() throws Exception {
        signaling.setOfferCheck(retailCheck());
        signaling.setOfferPolicy(OfferPolicy.REQUIRE_ATTESTED);
        EmbeddedChannel channel = offer(educationOffer());
        executor.finishNext();
        channel.runPendingTasks();
        assertTrue(admitted.isEmpty());
        assertTrue(response(channel).startsWith("HTTP/1.1 401"));
    }

    @Test
    void unreachableTrustSourceAnswers500OnlyWhenAttestationIsRequired() throws Exception {
        // Port 1 refuses the connection, so the key set fetch fails at once
        signaling.setOfferCheck(new OfferCheck(List.of(TrustedIssuer.jwks(URI.create("https://127.0.0.1:1/keys"),
                TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE))));
        EmbeddedChannel admittedChannel = offer(retailOffer());
        executor.finishNext();
        admittedChannel.runPendingTasks();
        assertTrue(response(admittedChannel).startsWith("HTTP/1.1 200"), "the proven key is enough by default");

        signaling.setOfferPolicy(OfferPolicy.REQUIRE_ATTESTED);
        EmbeddedChannel refused = offer(retailOffer());
        executor.finishNext();
        refused.runPendingTasks();
        assertTrue(response(refused).startsWith("HTTP/1.1 500"));
        assertEquals(1, admitted.size());
    }

    @Test
    void saturatedCheckQueueRefusesWithoutBlockingTheIoLoop() throws Exception {
        signaling.setOfferCheck(retailCheck());
        offer(retailOffer());
        EmbeddedChannel excess = offer(retailOffer());
        assertTrue(response(excess).startsWith("HTTP/1.1 503"));
        assertEquals(1, executor.tasks.size());
        assertTrue(admitted.isEmpty());
    }

    @Test
    void negotiationDeadlineCancelsTheQueuedCheck() throws Exception {
        signaling.setOfferCheck(retailCheck());
        EmbeddedChannel channel = offer(retailOffer());
        channel.advanceTimeBy(16, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertTrue(response(channel).startsWith("HTTP/1.1 502"));
        assertTrue(((Future<?>) executor.tasks.peek()).isCancelled());
        executor.finishNext();
        channel.runPendingTasks();
        assertTrue(admitted.isEmpty());
    }

    @Test
    void disconnectPreventsALateResultFromAdmittingAConnection() throws Exception {
        try (ServerSocket keyServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            keyServer.setSoTimeout(5000);
            // A key set fetch that hangs until the client has left; failing it then still yields a
            // proven key, which the default policy would admit
            signaling.setOfferCheck(new OfferCheck(List.of(TrustedIssuer.jwks(
                    URI.create("https://127.0.0.1:" + keyServer.getLocalPort() + "/keys"),
                    TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE))));
            EmbeddedChannel channel = offer(retailOffer());
            Thread check = executor.startNext();
            try (Socket ignored = keyServer.accept()) {
                channel.close();
            }
            check.join(5000);
            assertFalse(check.isAlive());
            channel.runPendingTasks();
            assertTrue(admitted.isEmpty());
            assertNull(channel.readOutbound());
        }
    }

    @Test
    void listenerCloseCancelsTheCheckAndShutsDownItsExecutor() throws Exception {
        signaling.setOfferCheck(retailCheck());
        EmbeddedChannel channel = offer(retailOffer());
        Runnable queued = executor.tasks.remove();
        signaling.close();
        queued.run();
        channel.runPendingTasks();
        assertTrue(executor.isShutdown());
        assertTrue(admitted.isEmpty());
        assertTrue(response(channel).startsWith("HTTP/1.1 503"));
    }

    private static String response(EmbeddedChannel channel) {
        channel.runPendingTasks();
        StringBuilder response = new StringBuilder();
        ByteBuf bytes;
        while ((bytes = channel.readOutbound()) != null) {
            try { response.append(bytes.toString(StandardCharsets.UTF_8)); }
            finally { bytes.release(); }
        }
        return response.toString();
    }

    private static final class ManualExecutor extends AbstractExecutorService {
        final LinkedBlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>(1);
        boolean shutdown;

        @Override public void execute(Runnable command) {
            if (shutdown || !tasks.offer(command)) throw new RejectedExecutionException();
        }
        Thread startNext() {
            Thread thread = new Thread(tasks.remove(), "test-offer-validator");
            thread.start();
            return thread;
        }
        void finishNext() throws InterruptedException {
            Thread thread = startNext();
            thread.join(2000);
            assertFalse(thread.isAlive());
        }
        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> remaining = new ArrayList<>();
            tasks.drainTo(remaining);
            return remaining;
        }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown && tasks.isEmpty(); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return isTerminated(); }
    }
}

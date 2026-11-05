package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class SignalingSupervisorTest {
    private final EmbeddedChannel loop = new EmbeddedChannel();
    private final List<Boolean> tokenRequests = new ArrayList<>();
    private final List<String> openedWith = new ArrayList<>();
    private final List<CompletableFuture<Void>> opens = new ArrayList<>();
    private final List<SignalingHealth> published = new ArrayList<>();
    private int aborts;
    private double random = 1.0;

    private final SignalingSupervisor supervisor = new SignalingSupervisor(loop.eventLoop(),
            refresh -> {
                tokenRequests.add(refresh);
                return CompletableFuture.completedFuture("MCToken " + tokenRequests.size());
            },
            new SignalingSupervisor.Connector() {
                @Override
                public CompletionStage<?> open(String token) {
                    openedWith.add(token);
                    CompletableFuture<Void> open = new CompletableFuture<>();
                    opens.add(open);
                    return open;
                }

                @Override
                public void abort() {
                    aborts++;
                }
            },
            () -> random);

    {
        loop.freezeTime();
        supervisor.addListener(published::add);
    }

    @AfterEach
    void tearDown() {
        supervisor.close();
        run();
        loop.finishAndReleaseAll();
    }

    @Test
    void startReturnsAtOnceAndConnectsInTheBackground() {
        assertEquals(SignalingState.NEW, supervisor.health().state());
        CompletableFuture<Void> connected = supervisor.nextConnection();

        run();

        assertEquals(SignalingState.STARTING, supervisor.health().state());
        assertFalse(connected.isDone());
        succeed(0);
        assertTrue(connected.isDone());
        assertEquals(SignalingState.CONNECTED, supervisor.health().state());
        CompletableFuture<Void> again = supervisor.nextConnection();
        run();
        assertTrue(again.isDone(), "A connected supervisor answers at once");
        assertEquals(1, opens.size());
    }

    @Test
    void everyAttemptAsksForATokenAndARejectedTokenForcesARefresh() {
        supervisor.start();
        run();
        fail(0, new SignalingSupervisor.TokenRejectedException("401", null));
        advance(supervisor.health().retryIn());
        fail(1, new IOException("reset"));
        advance(supervisor.health().retryIn());
        succeed(2);

        assertEquals(List.of(false, true, false), tokenRequests);
        assertEquals(List.of("MCToken 1", "MCToken 2", "MCToken 3"), openedWith);
    }

    @Test
    void failedAttemptsBackOffExponentiallyUpToTheCapWithJitter() {
        supervisor.start();
        run();
        List<Duration> delays = new ArrayList<>();
        for (int attempt = 0; attempt < 8; attempt++) {
            fail(attempt, new IOException("down"));
            Duration delay = supervisor.health().retryIn();
            delays.add(delay);
            assertEquals(attempt + 1, supervisor.health().failedAttempts());

            loop.advanceTimeBy(delay.toMillis() - 1, TimeUnit.MILLISECONDS);
            run();
            assertEquals(attempt + 1, opens.size(), "No attempt before the delay elapsed");
            advance(Duration.ofMillis(1));
            assertEquals(attempt + 2, opens.size());
        }
        assertEquals(List.of(2L, 4L, 8L, 16L, 32L, 64L, 120L, 120L), delays.stream().map(Duration::toSeconds).toList());

        random = 0.0;
        fail(8, new IOException("down"));
        assertEquals(Duration.ofSeconds(60), supervisor.health().retryIn(), "Jitter keeps at least half the delay");
    }

    @Test
    void healthReportsEveryTransition() {
        supervisor.start();
        run();
        IOException refused = new IOException("refused");
        fail(0, refused);
        advance(supervisor.health().retryIn());
        succeed(1);
        loop.advanceTimeBy(SignalingSupervisor.STABLE_AFTER.toSeconds(), TimeUnit.SECONDS);
        IOException lost = new IOException("1001");
        supervisor.lost(lost);
        run();
        succeed(2);
        supervisor.close();
        run();

        assertEquals(List.of(
                new SignalingHealth(SignalingState.STARTING, 0, null, null),
                new SignalingHealth(SignalingState.STARTING, 1, refused, Duration.ofSeconds(2)),
                new SignalingHealth(SignalingState.CONNECTED, 0, null, null),
                new SignalingHealth(SignalingState.RECONNECTING, 0, lost, Duration.ZERO),
                new SignalingHealth(SignalingState.CONNECTED, 0, null, null),
                new SignalingHealth(SignalingState.CLOSED, 0, null, null)), published);
    }

    @Test
    void aConnectionLostSoonAfterItCameUpBacksOff() {
        supervisor.start();
        run();
        succeed(0);
        loop.advanceTimeBy(10, TimeUnit.SECONDS);

        supervisor.lost(new IOException("closed"));
        run();

        assertEquals(SignalingState.RECONNECTING, supervisor.health().state());
        assertEquals(Duration.ofSeconds(2), supervisor.health().retryIn());
        assertEquals(1, opens.size());
    }

    @Test
    void aLossWhileConnectingIsIgnoredInFavorOfTheAttemptsResult() {
        supervisor.start();
        run();

        supervisor.lost(new IOException("closed"));
        run();

        assertEquals(new SignalingHealth(SignalingState.STARTING, 0, null, null), supervisor.health());
    }

    @Test
    void manualReconnectReplacesAWorkingConnection() {
        supervisor.start();
        run();
        succeed(0);

        CompletableFuture<Void> reconnected = supervisor.reconnect();
        run();

        assertEquals(1, aborts);
        assertEquals(SignalingState.RECONNECTING, supervisor.health().state());
        assertEquals(2, opens.size());
        assertFalse(reconnected.isDone());
        succeed(1);
        assertTrue(reconnected.isDone());
        assertEquals(SignalingState.CONNECTED, supervisor.health().state());
    }

    @Test
    void manualReconnectJoinsARunningAttemptAndSkipsABackoffWait() {
        supervisor.start();
        run();
        CompletableFuture<Void> joined = supervisor.reconnect();
        run();
        assertEquals(1, opens.size(), "A running attempt is not restarted");
        IOException down = new IOException("down");
        fail(0, down);
        assertSame(down, assertThrows(CompletionException.class, joined::join).getCause());

        CompletableFuture<Void> skipped = supervisor.reconnect();
        run();

        assertEquals(2, opens.size(), "The backoff wait is skipped");
        succeed(1);
        assertTrue(skipped.isDone());
        loop.advanceTimeBy(1, TimeUnit.MINUTES);
        run();
        assertEquals(2, opens.size(), "The cancelled backoff timer does not start another attempt");
    }

    @Test
    void anAttemptThatNeverCompletesTimesOutAndIsAborted() {
        supervisor.start();
        run();

        advance(SignalingSupervisor.ATTEMPT_TIMEOUT);

        assertInstanceOf(TimeoutException.class, supervisor.health().lastFailure());
        assertEquals(1, aborts);
        opens.getFirst().complete(null);
        run();
        assertEquals(SignalingState.STARTING, supervisor.health().state(), "A late success of a timed out attempt is ignored");
    }

    @Test
    void closeFailsWaitersAndStopsRetrying() {
        supervisor.start();
        run();
        fail(0, new IOException("down"));
        CompletableFuture<Void> waiting = supervisor.nextConnection();
        run();

        supervisor.close();
        run();
        loop.advanceTimeBy(10, TimeUnit.MINUTES);
        run();

        assertInstanceOf(ClosedChannelException.class, assertThrows(CompletionException.class, waiting::join).getCause());
        assertEquals(SignalingState.CLOSED, supervisor.health().state());
        assertEquals(1, opens.size());
        CompletableFuture<Void> afterClose = supervisor.reconnect();
        run();
        assertTrue(afterClose.isCompletedExceptionally());
    }

    private void succeed(int attempt) {
        opens.get(attempt).complete(null);
        run();
    }

    private void fail(int attempt, Throwable cause) {
        opens.get(attempt).completeExceptionally(cause);
        run();
    }

    private void advance(Duration duration) {
        loop.advanceTimeBy(duration.toNanos(), TimeUnit.NANOSECONDS);
        run();
    }

    private void run() {
        for (int i = 0; i < 4; i++) {
            loop.runPendingTasks();
        }
    }
}

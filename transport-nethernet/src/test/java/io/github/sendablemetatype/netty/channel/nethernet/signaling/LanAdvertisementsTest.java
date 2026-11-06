package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertisement;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanDiscoveryRequest;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Which advertiser replies become the fallback, with the advertiser's calls and the deadline driven by hand. */
class LanAdvertisementsTest {
    private static final Duration DEADLINE = Duration.ofMillis(100);
    private static final LanDiscoveryRequest REQUEST =
            new LanDiscoveryRequest(new InetSocketAddress("192.168.1.20", 7551), "20");

    private final Queue<Runnable> calls = new ArrayDeque<>();
    private final EmbeddedChannel loop = new EmbeddedChannel();
    private final LanAdvertisements advertisements = new LanAdvertisements(calls::add);

    LanAdvertisementsTest() {
        loop.freezeTime();
    }

    @AfterEach
    void tearDown() {
        loop.finishAndReleaseAll();
    }

    private static LanServerData.V7 data(int players) {
        return new LanServerData.V7("Server", 2225, "1.26.62", "World", players, 10, 0, false, false, true, false,
                "abc", 4);
    }

    private CompletableFuture<Optional<byte[]>> ask() {
        return advertisements.answer(REQUEST, loop.eventLoop());
    }

    private void passDeadline() {
        loop.advanceTimeBy(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        loop.runScheduledPendingTasks();
    }

    /** What the advertiser's reply to {@code data} encodes to, through a fresh instance. */
    private static byte[] payload(LanServerData.V7 data) {
        LanAdvertisements fixed = new LanAdvertisements(Runnable::run);
        fixed.fix(data);
        return fixed.answer(REQUEST, null).join().orElseThrow();
    }

    @Test
    void aReplyPastItsDeadlineDoesNotBecomeTheFallback() {
        int[] players = {0};
        advertisements.ask(request -> LanAdvertisement.advertise(data(++players[0])), DEADLINE);
        CompletableFuture<Optional<byte[]>> first = ask();
        calls.remove().run();
        assertArrayEquals(payload(data(1)), first.join().orElseThrow());

        CompletableFuture<Optional<byte[]>> late = ask();
        passDeadline();
        assertArrayEquals(payload(data(1)), late.join().orElseThrow(), "the deadline sends the fallback");
        calls.remove().run();

        CompletableFuture<Optional<byte[]>> next = ask();
        passDeadline();
        assertArrayEquals(payload(data(1)), next.join().orElseThrow(), "still the reply that came in time");
    }

    @Test
    void aCallToTheReplacedAdvertiserDoesNotBecomeTheNewOnesFallback() {
        advertisements.ask(request -> LanAdvertisement.advertise(data(1)), DEADLINE);
        CompletableFuture<Optional<byte[]>> replaced = ask();
        advertisements.ask(request -> LanAdvertisement.advertise(data(2)), DEADLINE);
        calls.remove().run();
        assertTrue(replaced.isDone());

        CompletableFuture<Optional<byte[]>> current = ask();
        passDeadline();
        assertEquals(Optional.empty(), current.join(), "the new advertiser produced nothing yet");
        assertFalse(calls.isEmpty());
    }
}

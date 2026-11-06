package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProviderCallsTest {
    private static final Duration TIMEOUT = Duration.ofMillis(100);

    // The executor runs each call inline, so the answer arrives before the loop could schedule the
    // deadline: as on a loop that is running late.
    @Test
    void anAnswerPastItsDeadlineGetsTheFallbackEvenWhenItBeatsTheLoop() throws Exception {
        AtomicLong now = new AtomicLong();
        ProviderCalls calls = new ProviderCalls("test provider",
                InternalLoggerFactory.getInstance(ProviderCallsTest.class), TIMEOUT, Runnable::run, now::get);

        String late = calls.call(() -> {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(150));
            return "late";
        }, () -> "fallback", ImmediateEventExecutor.INSTANCE).get(1, TimeUnit.SECONDS);
        String onTime = calls.call(() -> {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(50));
            return "on time";
        }, () -> "fallback", ImmediateEventExecutor.INSTANCE).get(1, TimeUnit.SECONDS);

        assertEquals("fallback", late);
        assertEquals("on time", onTime);
    }
}

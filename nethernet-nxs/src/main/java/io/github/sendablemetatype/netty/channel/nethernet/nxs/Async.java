package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Small helpers for the client's asynchronous steps; every continuation runs on the lifecycle executor. */
final class Async {
    private Async() {
    }

    /** A future's wrapper, or an unchecked wrapper of an I/O failure, says nothing about what failed. */
    static Throwable unwrap(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException
                || failure instanceof UncheckedIOException) && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    /** Completes on the executor after the delay. */
    static CompletableFuture<Void> delay(ScheduledExecutorService executor, Duration delay) {
        CompletableFuture<Void> elapsed = new CompletableFuture<>();
        executor.schedule(() -> elapsed.complete(null), Math.max(0, delay.toNanos()), TimeUnit.NANOSECONDS);
        return elapsed;
    }

    /**
     * The stage's result on the executor, or a {@link TimeoutException} naming what did not finish in time.
     */
    static <T> CompletableFuture<T> within(CompletionStage<T> stage, Duration limit, ScheduledExecutorService executor,
                                           String what) {
        CompletableFuture<T> result = new CompletableFuture<>();
        ScheduledFuture<?> timer = executor.schedule(
                () -> result.completeExceptionally(new TimeoutException(what + " did not finish in time")),
                limit.toNanos(), TimeUnit.NANOSECONDS);
        stage.whenCompleteAsync((value, failure) -> {
            timer.cancel(false);
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(unwrap(failure));
            }
        }, executor);
        return result;
    }

    /** A step that starts asynchronous work and may fail before it does. */
    @FunctionalInterface
    interface Step<T> {
        CompletionStage<T> run() throws Exception;
    }

    /** Runs a step that may throw, as a stage. */
    static <T> CompletableFuture<T> attempt(Step<T> step) {
        try {
            return step.run().toCompletableFuture();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    static CompletableFuture<Void> done() {
        return CompletableFuture.completedFuture(null);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Calls a host's provider for requests anyone on the network can send (HTTP status requests, LAN
 * discovery requests): off the I/O threads, at most {@value #MAX_CONCURRENT_CALLS} calls at once,
 * each with a deadline. A call that throws, answers null, misses its deadline or finds no room
 * completes with the caller's fallback instead, so a slow or broken provider never stalls the
 * socket it answers on. The first failure warns, later ones log at debug, since whoever can reach
 * the port could otherwise drive the log volume.
 */
final class ProviderCalls {
    static final int MAX_CONCURRENT_CALLS = 64;

    private final String name;
    private final InternalLogger log;
    private final Duration timeout;
    private final Executor executor;
    private final LongSupplier nanos;
    private final Semaphore calls = new Semaphore(MAX_CONCURRENT_CALLS);
    // Racing a duplicate first warning is harmless; this only bounds the volume.
    private final AtomicBoolean warned = new AtomicBoolean();

    /**
     * @param name     names the provider in logs, such as {@code status provider}
     * @param log      where failures are logged
     * @param timeout  how long one call may take
     * @param executor runs the calls; not an I/O loop
     */
    ProviderCalls(String name, InternalLogger log, Duration timeout, Executor executor) {
        this(name, log, timeout, executor, System::nanoTime);
    }

    ProviderCalls(String name, InternalLogger log, Duration timeout, Executor executor, LongSupplier nanos) {
        this.name = name;
        this.log = log;
        this.timeout = timeout;
        this.executor = executor;
        this.nanos = nanos;
    }

    /**
     * Makes one call.
     *
     * @param call     asks the provider and turns its answer into a reply; may block
     * @param fallback the reply when the call does not deliver one, asked on whichever thread
     *                 gives up on the call
     * @param loop     runs the deadline
     * @return the reply; completes on the executor or the loop
     */
    <T> CompletableFuture<T> call(Callable<T> call, Supplier<T> fallback, EventExecutor loop) {
        if (!calls.tryAcquire()) {
            failed("The {} already has {} calls running", MAX_CONCURRENT_CALLS);
            return CompletableFuture.completedFuture(fallback.get());
        }
        CompletableFuture<T> reply = new CompletableFuture<>();
        long started = nanos.getAsLong();
        try {
            executor.execute(() -> {
                try {
                    T answer = Objects.requireNonNull(call.call(), "the answer");
                    // The scheduled deadline runs on the loop, which may itself be late, so an answer
                    // past its deadline can get here first; it still counts as late.
                    if (nanos.getAsLong() - started > timeout.toNanos()) {
                        if (reply.complete(fallback.get())) {
                            failed("The {} did not answer within {} ms", timeout.toMillis());
                        }
                    } else {
                        reply.complete(answer);
                    }
                } catch (Throwable e) {
                    // Throwable: an Error escaping here would leave the request to its deadline
                    if (!reply.isDone()) {
                        failed("The {} failed: {}", e.toString());
                        reply.complete(fallback.get());
                    }
                } finally {
                    // Released when the call returns, even past its deadline, so a provider that
                    // hangs holds at most the calls in flight and no more
                    calls.release();
                }
            });
        } catch (RejectedExecutionException e) {
            calls.release();
            return CompletableFuture.completedFuture(fallback.get());
        }
        if (!reply.isDone()) {
            ScheduledFuture<?> deadline = loop.schedule(() -> {
                if (!reply.isDone() && reply.complete(fallback.get())) {
                    failed("The {} did not answer within {} ms", timeout.toMillis());
                }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
            reply.whenComplete((done, failure) -> deadline.cancel(false));
        }
        return reply;
    }

    private void failed(String format, Object argument) {
        if (warned.compareAndSet(false, true)) {
            log.warn(format + " (further failures log at debug)", name, argument);
        } else {
            log.debug(format, name, argument);
        }
    }
}

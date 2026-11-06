package io.github.sendablemetatype.netty.channel.nethernet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/** An identity check executor that holds one check at a time until the test runs it. */
final class ManualCheckExecutor extends AbstractExecutorService {
    final LinkedBlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>(1);
    private volatile boolean shutdown;

    @Override
    public void execute(Runnable command) {
        if (shutdown || !tasks.offer(command)) {
            throw new RejectedExecutionException();
        }
    }

    void runNext() {
        tasks.remove().run();
    }

    /** Waits until a check is queued, and returns it without taking it. */
    Future<?> awaitQueued() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (tasks.peek() instanceof Future<?> queued) {
                return queued;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("No identity check was queued");
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        List<Runnable> remaining = new ArrayList<>();
        tasks.drainTo(remaining);
        return remaining;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && tasks.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return isTerminated();
    }
}

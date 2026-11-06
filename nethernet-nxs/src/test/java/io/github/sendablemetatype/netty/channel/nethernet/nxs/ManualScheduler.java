package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/**
 * A single-threaded executor on virtual time for the client's lifecycle: tasks run only on the test thread, timers
 * fire only when the test advances the clock, and work other threads hand over (HTTP replies, socket events) waits
 * in a queue until the test runs it. Nothing sleeps.
 */
final class ManualScheduler extends AbstractExecutorService implements ScheduledExecutorService, ProviderClock {
    /** How long to wait for work from another thread, such as a loopback HTTP reply, before giving up. */
    private static final long EXTERNAL_WAIT_MILLIS = 20_000;

    private final LinkedBlockingQueue<Runnable> ready = new LinkedBlockingQueue<>();
    private final PriorityQueue<Timer> timers = new PriorityQueue<>();
    private final long startMillis = System.currentTimeMillis();
    private volatile long nanos = 1_000_000_000_000L;
    private long sequence;
    private volatile boolean shutdown;

    @Override
    public long millis() {
        return startMillis + TimeUnit.NANOSECONDS.toMillis(nanos - 1_000_000_000_000L);
    }

    @Override
    public long nanos() {
        return nanos;
    }

    /** Runs everything ready now, including timers that are due. */
    void runReady() {
        while (true) {
            Runnable task = ready.poll();
            if (task == null) {
                Timer due;
                synchronized (timers) {
                    due = timers.isEmpty() || timers.peek().at > nanos ? null : timers.poll();
                }
                if (due == null) {
                    return;
                }
                due.fire();
                continue;
            }
            task.run();
        }
    }

    /** Moves time forward, firing timers in order, with the work they release run at their own time. */
    void advance(Duration duration) {
        long until = nanos + duration.toNanos();
        runReady();
        while (true) {
            Timer next;
            synchronized (timers) {
                next = timers.isEmpty() || timers.peek().at > until ? null : timers.poll();
            }
            if (next == null) {
                break;
            }
            nanos = Math.max(nanos, next.at);
            next.fire();
            runReady();
        }
        nanos = until;
        runReady();
    }

    /** Runs work until the stage completes, waiting for other threads only as long as they take. */
    <T> T await(CompletionStage<T> stage) throws Exception {
        CompletableFuture<T> future = stage.toCompletableFuture();
        until(future::isDone);
        try {
            return future.get();
        } catch (ExecutionException failure) {
            throw failure.getCause() instanceof Exception cause ? cause : failure;
        }
    }

    /**
     * Runs work until the condition holds, failing when nothing arrives for a long time. The condition may also turn
     * true on another thread, such as the stub receiving a frame, so it is checked between short waits.
     */
    void until(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + EXTERNAL_WAIT_MILLIS;
        while (true) {
            runReady();
            if (condition.getAsBoolean()) {
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Nothing happened for " + EXTERNAL_WAIT_MILLIS + " ms");
            }
            Runnable task = ready.poll(10, TimeUnit.MILLISECONDS);
            if (task != null) {
                task.run();
                deadline = System.currentTimeMillis() + EXTERNAL_WAIT_MILLIS;
            }
        }
    }

    /**
     * Runs work until the condition holds. When nothing arrives for a moment, the work is waiting on one of its own
     * timers, such as an in-place retry delay, so the clock moves to that timer, at most a second at a time.
     */
    void settle(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + EXTERNAL_WAIT_MILLIS;
        while (true) {
            runReady();
            if (condition.getAsBoolean()) {
                return;
            }
            Runnable task = ready.poll(QUIET_MILLIS, TimeUnit.MILLISECONDS);
            if (task != null) {
                task.run();
                continue;
            }
            Timer next;
            synchronized (timers) {
                next = timers.peek();
            }
            if (next == null || System.currentTimeMillis() > deadline) {
                throw new AssertionError("Nothing happened and no timer is due");
            }
            advance(Duration.ofNanos(Math.max(1, Math.min(next.at - nanos, TimeUnit.SECONDS.toNanos(1)))));
        }
    }

    /**
     * Runs work until none has arrived from another thread for {@code millis}, so replies already on their way, such
     * as a pong, land before the clock moves on.
     */
    void quiet(long millis) throws InterruptedException {
        while (true) {
            runReady();
            Runnable task = ready.poll(millis, TimeUnit.MILLISECONDS);
            if (task == null) {
                return;
            }
            task.run();
        }
    }

    /** How long without work from other threads means the work waits on a timer; loopback replies take far less. */
    private static final long QUIET_MILLIS = 300;

    /** The failure a stage completes with. */
    Throwable failure(CompletionStage<?> stage) throws InterruptedException {
        CompletableFuture<?> future = stage.toCompletableFuture();
        until(future::isDone);
        try {
            future.join();
        } catch (CompletionException | CancellationException failure) {
            return Async.unwrap(failure);
        }
        throw new AssertionError("Expected a failure");
    }

    @Override
    public void execute(Runnable command) {
        ready.add(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return add(new Timer(Executors.callable(command), nanos + unit.toNanos(delay), 0));
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return add(new Timer(callable, nanos + unit.toNanos(delay), 0));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return add(new Timer(Executors.callable(command), nanos + unit.toNanos(initialDelay), unit.toNanos(period)));
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return add(new Timer(Executors.callable(command), nanos + unit.toNanos(initialDelay), unit.toNanos(delay)));
    }

    @SuppressWarnings("unchecked")
    private <V> ScheduledFuture<V> add(Timer timer) {
        synchronized (timers) {
            timers.add(timer);
        }
        return (ScheduledFuture<V>) timer;
    }

    /** The timers still waiting, for checks that nothing was left behind. */
    int pendingTimers() {
        synchronized (timers) {
            return (int) timers.stream().filter(timer -> !timer.isCancelled()).count();
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        return new ArrayList<>();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return shutdown;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    private final class Timer extends CompletableFuture<Object> implements ScheduledFuture<Object> {
        private final Callable<?> task;
        private final long period;
        private final long order = sequence++;
        private long at;

        Timer(Callable<?> task, long at, long period) {
            this.task = task;
            this.at = at;
            this.period = period;
        }

        void fire() {
            if (isCancelled()) {
                return;
            }
            try {
                Object result = task.call();
                if (period == 0) {
                    complete(result);
                } else {
                    at = nanos + period;
                    add(this);
                }
            } catch (Throwable failure) {
                completeExceptionally(failure);
            }
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(at - nanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            Timer timer = (Timer) other;
            int byTime = Long.compare(at, timer.at);
            return byTime != 0 ? byTime : Long.compare(order, timer.order);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            synchronized (timers) {
                timers.remove(this);
            }
            return super.cancel(mayInterruptIfRunning);
        }
    }
}

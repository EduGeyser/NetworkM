package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;

/**
 * Keeps one connection to a signaling service up: connects in the background, asks for a token
 * before every attempt, retries failed attempts with exponential backoff and jitter, replaces a
 * lost connection, and reports each change as a {@link SignalingHealth}.
 *
 * <p>All state lives on one executor. Public methods may be called from any thread; they post
 * their work to the executor and never block.
 */
final class SignalingSupervisor {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(SignalingSupervisor.class);

    /** Deadline for one attempt: getting a token, opening the socket and its credential exchange. */
    static final Duration ATTEMPT_TIMEOUT = Duration.ofSeconds(30);
    /** A connection that lived this long counts as stable: losing it retries at once and resets backoff. */
    static final Duration STABLE_AFTER = Duration.ofSeconds(60);
    static final Duration BACKOFF_BASE = Duration.ofSeconds(2);
    static final Duration BACKOFF_MAX = Duration.ofMinutes(2);

    /** Opens and closes the connection the supervisor keeps up. Called on the executor only. */
    interface Connector {
        /**
         * Opens a new connection authorized by the token.
         *
         * @return completes once the connection is usable, or fails if it never becomes usable
         */
        CompletionStage<?> open(String token);

        /** Closes the current connection or the one being opened, without reporting it as lost. */
        void abort();
    }

    /** An attempt failed because the service refused the token; the next attempt asks for a fresh one. */
    static final class TokenRejectedException extends IOException {
        TokenRejectedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final EventExecutor executor;
    private final NetherNetXboxRpcSignaling.TokenSupplier tokens;
    private final Connector connector;
    private final DoubleSupplier random;
    private final List<Consumer<SignalingHealth>> listeners = new CopyOnWriteArrayList<>();

    private volatile SignalingHealth health = new SignalingHealth(SignalingState.NEW, 0, null, null);
    private volatile boolean closed;

    // Executor-confined.
    private int failedAttempts;
    /** Unsuccessful periods in a row (failed attempts and short-lived connections). Drives the backoff. */
    private int backoffLevel;
    private long connectedAtNanos;
    private boolean refreshToken;
    private long attemptId;
    private boolean attemptRunning;
    /** Completed by the attempt in progress, or by the next one when none is running. */
    private CompletableFuture<Void> outcome;
    /** The attempt's deadline while it runs, or the timer for the next attempt. */
    private ScheduledFuture<?> timer;

    SignalingSupervisor(EventExecutor executor, NetherNetXboxRpcSignaling.TokenSupplier tokens, Connector connector) {
        this(executor, tokens, connector, () -> ThreadLocalRandom.current().nextDouble());
    }

    SignalingSupervisor(EventExecutor executor, NetherNetXboxRpcSignaling.TokenSupplier tokens, Connector connector,
                        DoubleSupplier random) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.connector = Objects.requireNonNull(connector, "connector");
        this.random = Objects.requireNonNull(random, "random");
    }

    SignalingHealth health() {
        return health;
    }

    void addListener(Consumer<SignalingHealth> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    void removeListener(Consumer<SignalingHealth> listener) {
        listeners.remove(listener);
    }

    /** Starts connecting in the background. Does nothing once started. */
    void start() {
        post(() -> {
            if (health.state() == SignalingState.NEW) {
                attemptNow();
            }
        });
    }

    /**
     * Replaces the connection now: closes a working one, or skips the wait before the next attempt.
     * An attempt already running is not restarted.
     *
     * @return completes when that attempt succeeds, fails when it fails; retries continue either way
     */
    CompletableFuture<Void> reconnect() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        post(() -> {
            if (closed) {
                result.completeExceptionally(new ClosedChannelException());
                return;
            }
            CompletableFuture<Void> next = outcome();
            if (!attemptRunning) {
                if (health.state() == SignalingState.CONNECTED) {
                    connector.abort();
                    publish(SignalingState.RECONNECTING, null, Duration.ZERO);
                }
                attemptNow();
            }
            propagate(next, result);
        });
        return result;
    }

    /**
     * Starts connecting if not started yet.
     *
     * @return completes at once while connected, otherwise with the outcome of the next attempt
     */
    CompletableFuture<Void> nextConnection() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        post(() -> {
            if (closed) {
                result.completeExceptionally(new ClosedChannelException());
                return;
            }
            if (health.state() == SignalingState.CONNECTED && !attemptRunning) {
                result.complete(null);
                return;
            }
            CompletableFuture<Void> next = outcome();
            if (health.state() == SignalingState.NEW) {
                attemptNow();
            }
            propagate(next, result);
        });
        return result;
    }

    /**
     * Reports that the usable connection died. The connector must already have dropped it. Ignored
     * unless connected, so a failure while opening goes through the attempt's own result instead.
     */
    void lost(Throwable cause) {
        post(() -> {
            if (closed || health.state() != SignalingState.CONNECTED) {
                return;
            }
            Duration delay;
            if (executor.ticker().nanoTime() - connectedAtNanos >= STABLE_AFTER.toNanos()) {
                // Most losses are the service rotating its instances (close code 1001) after a long life.
                backoffLevel = 0;
                delay = Duration.ZERO;
            } else {
                // A connection that dies right after it came up must not turn into a reconnect loop.
                delay = backoff(++backoffLevel);
            }
            log.debug("Signaling connection lost, reconnecting in {} ms: {}", delay.toMillis(), describe(cause));
            publish(SignalingState.RECONNECTING, cause, delay);
            scheduleAttempt(delay);
        });
    }

    /** Stops for good: cancels timers, closes the connection and fails waiting callers. */
    void close() {
        closed = true;
        post(() -> {
            if (health.state() == SignalingState.CLOSED) {
                return;
            }
            cancelTimer();
            attemptId++;
            attemptRunning = false;
            connector.abort();
            if (outcome != null) {
                outcome.completeExceptionally(new ClosedChannelException());
                outcome = null;
            }
            publish(SignalingState.CLOSED, null, null);
        });
    }

    private void attemptNow() {
        cancelTimer();
        if (closed) {
            return;
        }
        if (health.state() == SignalingState.NEW) {
            publish(SignalingState.STARTING, null, null);
        }
        long id = ++attemptId;
        attemptRunning = true;
        boolean refresh = refreshToken;
        refreshToken = false;
        timer = executor.schedule(() -> attemptFailed(id, new TimeoutException(
                "No usable signaling connection within " + ATTEMPT_TIMEOUT.toSeconds() + " s")),
                ATTEMPT_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);

        CompletionStage<String> token;
        try {
            token = Objects.requireNonNull(tokens.token(refresh), "The token supplier returned no stage");
        } catch (RuntimeException e) {
            attemptFailed(id, e);
            return;
        }
        token.whenComplete((value, error) -> post(() -> {
            if (!isCurrentAttempt(id)) {
                return;
            }
            if (error != null || value == null) {
                attemptFailed(id, error != null ? unwrap(error)
                        : new IllegalStateException("The token supplier returned no token"));
                return;
            }
            CompletionStage<?> open;
            try {
                open = Objects.requireNonNull(connector.open(value), "The connector returned no stage");
            } catch (RuntimeException e) {
                attemptFailed(id, e);
                return;
            }
            open.whenComplete((ignored, openError) -> post(() -> {
                if (openError != null) {
                    attemptFailed(id, unwrap(openError));
                } else {
                    attemptSucceeded(id);
                }
            }));
        }));
    }

    private void attemptSucceeded(long id) {
        if (!isCurrentAttempt(id)) {
            return;
        }
        attemptRunning = false;
        cancelTimer();
        failedAttempts = 0;
        connectedAtNanos = executor.ticker().nanoTime();
        publish(SignalingState.CONNECTED, null, null);
        CompletableFuture<Void> done = outcome;
        outcome = null;
        if (done != null) {
            done.complete(null);
        }
    }

    private void attemptFailed(long id, Throwable cause) {
        if (!isCurrentAttempt(id)) {
            return;
        }
        attemptRunning = false;
        cancelTimer();
        connector.abort();
        failedAttempts++;
        if (cause instanceof TokenRejectedException) {
            refreshToken = true;
        }
        Duration delay = backoff(++backoffLevel);
        log.debug("Signaling connection attempt {} failed, retrying in {} ms: {}",
                failedAttempts, delay.toMillis(), describe(cause));
        // Keeps STARTING before the first success and RECONNECTING after it.
        publish(health.state(), cause, delay);
        scheduleAttempt(delay);
        CompletableFuture<Void> done = outcome;
        outcome = null;
        if (done != null) {
            done.completeExceptionally(cause);
        }
    }

    private boolean isCurrentAttempt(long id) {
        return !closed && attemptRunning && id == attemptId;
    }

    private void scheduleAttempt(Duration delay) {
        cancelTimer();
        if (delay.isZero()) {
            attemptNow();
        } else {
            timer = executor.schedule(this::attemptNow, delay.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** Doubles from {@link #BACKOFF_BASE} per level up to {@link #BACKOFF_MAX}, then keeps 50 to 100 percent of it. */
    private Duration backoff(int level) {
        long cap = BACKOFF_MAX.toMillis();
        long full = BACKOFF_BASE.toMillis() << Math.min(Math.max(level - 1, 0), 20);
        long millis = Math.min(cap, full);
        return Duration.ofMillis((long) (millis * (0.5 + 0.5 * random.getAsDouble())));
    }

    private CompletableFuture<Void> outcome() {
        if (outcome == null) {
            outcome = new CompletableFuture<>();
        }
        return outcome;
    }

    private void cancelTimer() {
        if (timer != null) {
            timer.cancel(false);
            timer = null;
        }
    }

    private void publish(SignalingState state, Throwable cause, Duration retryIn) {
        SignalingHealth next = new SignalingHealth(state,
                state == SignalingState.CONNECTED ? 0 : failedAttempts, cause, retryIn);
        health = next;
        for (Consumer<SignalingHealth> listener : listeners) {
            try {
                listener.accept(next);
            } catch (RuntimeException e) {
                log.warn("A signaling health listener failed", e);
            }
        }
    }

    private void post(Runnable task) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            // The executor shut down with the signaling; only a close can still be pending then.
            if (!closed) {
                log.warn("Signaling executor rejected a task", e);
            }
        }
    }

    private static void propagate(CompletableFuture<Void> source, CompletableFuture<Void> target) {
        source.whenComplete((ignored, error) -> {
            if (error != null) {
                target.completeExceptionally(error);
            } else {
                target.complete(null);
            }
        });
    }

    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    private static String describe(Throwable cause) {
        return cause == null ? "no cause" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}

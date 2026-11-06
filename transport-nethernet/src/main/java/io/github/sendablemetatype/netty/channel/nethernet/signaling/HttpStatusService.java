package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusProvider;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusRequest;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs the {@link StatusProvider} for status requests: off the I/O threads, at most
 * {@value #MAX_CONCURRENT_CALLS} calls at once, each with a deadline. A call that fails, misses
 * its deadline or finds no room is answered with the last document served for the same
 * {@code Host}, or with an empty {@code 200}; never with an error status, since a failed status
 * request costs the client NetherNet (ruling E-6).
 */
final class HttpStatusService {
    static final int MAX_CONCURRENT_CALLS = 64;
    /** Hosts whose last document is kept. The Host header is the client's say, so this is bounded. */
    static final int REMEMBERED_HOSTS = 256;

    /** What a status request is answered with. */
    sealed interface Reply {
        /** A {@code 200} with this JSON document. */
        record Document(String json) implements Reply {
        }

        /** A {@code 200} with no body. */
        record Empty() implements Reply {
        }

        /** A {@code 404}: no NetherNet for this request. */
        record Declined() implements Reply {
        }
    }

    private final StatusProvider provider;
    private final Duration timeout;
    private final Executor executor;
    private final Semaphore calls = new Semaphore(MAX_CONCURRENT_CALLS);
    private final Map<String, String> lastDocuments = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > REMEMBERED_HOSTS;
        }
    };
    // Racing a duplicate first warning is harmless; this only bounds the volume from an open port
    private final AtomicBoolean warned = new AtomicBoolean();

    HttpStatusService(StatusProvider provider, Duration timeout, Executor executor) {
        this.provider = provider;
        this.timeout = timeout;
        this.executor = executor;
    }

    /**
     * Asks the provider about one request.
     *
     * @param request the request
     * @param loop    the connection's loop, which runs the deadline
     * @return the reply; completes on the provider's thread or the loop
     */
    CompletableFuture<Reply> query(StatusRequest request, EventExecutor loop) {
        String host = hostKey(request.host());
        if (!calls.tryAcquire()) {
            failed("{} status provider calls are already running", MAX_CONCURRENT_CALLS);
            return CompletableFuture.completedFuture(fallback(host));
        }
        CompletableFuture<Reply> reply = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    StatusAnswer answer = provider.status(request);
                    switch (answer) {
                        case StatusAnswer.Serve(var status) -> {
                            String json = status.toJson();
                            // Only the document that is served: an answer past its deadline was
                            // never sent and may be older than the one sent since (E-6)
                            if (reply.complete(new Reply.Document(json))) {
                                remember(host, json);
                            }
                        }
                        case StatusAnswer.Decline _ -> reply.complete(new Reply.Declined());
                        case null -> throw new NullPointerException("The status provider answered null");
                    }
                } catch (Throwable e) {
                    // Throwable: an Error escaping here would leave the request to its deadline
                    if (!reply.isDone()) {
                        failed("The status provider failed: {}", e.toString());
                        reply.complete(fallback(host));
                    }
                } finally {
                    // Released when the call returns, even past its deadline, so a provider that
                    // hangs holds at most the 64 calls and no more
                    calls.release();
                }
            });
        } catch (RejectedExecutionException e) {
            calls.release();
            return CompletableFuture.completedFuture(fallback(host));
        }
        if (!reply.isDone()) {
            ScheduledFuture<?> deadline = loop.schedule(() -> {
                if (reply.complete(fallback(host))) {
                    failed("The status provider did not answer within {} ms", timeout.toMillis());
                }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
            reply.whenComplete((done, failure) -> deadline.cancel(false));
        }
        return reply;
    }

    private Reply fallback(String host) {
        String json;
        synchronized (lastDocuments) {
            json = lastDocuments.get(host);
        }
        return json == null ? new Reply.Empty() : new Reply.Document(json);
    }

    private void remember(String host, String json) {
        synchronized (lastDocuments) {
            lastDocuments.put(host, json);
        }
    }

    private void failed(String format, Object argument) {
        // The endpoint is unauthenticated, so per request warnings would let anyone drive log volume
        if (warned.compareAndSet(false, true)) {
            NetherNetHttpSignaling.log.warn(format + " (further failures log at debug)", argument);
        } else {
            NetherNetHttpSignaling.log.debug(format, argument);
        }
    }

    private static String hostKey(String host) {
        return host == null ? "" : host.toLowerCase(Locale.ROOT);
    }
}

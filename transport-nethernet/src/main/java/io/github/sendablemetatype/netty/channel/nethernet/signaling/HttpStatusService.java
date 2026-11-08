package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusProvider;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusRequest;
import io.netty.util.concurrent.EventExecutor;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs the {@link StatusProvider} for status requests through {@link ProviderCalls}: off the I/O
 * threads, bounded, each with a deadline. A call that fails, misses its deadline or finds no room
 * is answered with the last shared document served for the same {@code Host}, or with an empty
 * {@code 200}; never with an error status, since a failed status request costs the client
 * NetherNet. A document counts as served once its response was written, which the request
 * handler reports with {@link #served}. A document the provider served
 * {@linkplain StatusAnswer#serveToRequester to its requester alone} is never repeated: it may
 * carry what the provider chose for that requester, and a fallback goes to whoever else names
 * the Host.
 */
final class HttpStatusService {
    /** Hosts whose last document is kept. The Host header is the client's say, so this is bounded. */
    static final int REMEMBERED_HOSTS = 256;

    /** What a status request is answered with. */
    sealed interface Reply {
        /**
         * A {@code 200} with this JSON document.
         *
         * @param sequence orders the provider's answers, so an older document never replaces a
         *                 newer one; a fallback carries the sequence of the answer it repeats
         * @param shared   whether a fallback may repeat it to other requests for the same Host
         */
        record Document(String json, long sequence, boolean shared) implements Reply {
        }

        /** A {@code 200} with no body. */
        record Empty() implements Reply {
        }

        /** A {@code 404}: no NetherNet for this request. */
        record Declined() implements Reply {
        }
    }

    private final StatusProvider provider;
    private final ProviderCalls calls;
    private final AtomicLong answers = new AtomicLong();
    private final Map<String, Reply.Document> lastDocuments = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Reply.Document> eldest) {
            return size() > REMEMBERED_HOSTS;
        }
    };

    HttpStatusService(StatusProvider provider, Duration timeout, Executor executor) {
        this.provider = provider;
        this.calls = new ProviderCalls("status provider", NetherNetHttpSignaling.log, timeout, executor);
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
        return calls.call(() -> switch (provider.status(request)) {
            case StatusAnswer.Serve(var status, var shared) ->
                    new Reply.Document(status.toJson(), answers.incrementAndGet(), shared);
            case StatusAnswer.Decline _ -> new Reply.Declined();
            case null -> throw new NullPointerException("The status provider answered null");
        }, () -> fallback(host), loop);
    }

    /**
     * Records a document whose response was written, the only kind a fallback may repeat: one
     * for a client that left, or past its deadline, never reached anybody. A document for its
     * requester alone is not recorded.
     *
     * @param request  the request it answered
     * @param document the document as {@link #query} gave it
     */
    void served(StatusRequest request, Reply.Document document) {
        if (!document.shared()) {
            return;
        }
        String host = hostKey(request.host());
        synchronized (lastDocuments) {
            // Writes on different connections finish in any order, and a fallback repeats an
            // older answer
            Reply.Document last = lastDocuments.get(host);
            if (last == null || document.sequence() > last.sequence()) {
                lastDocuments.put(host, document);
            }
        }
    }

    private Reply fallback(String host) {
        Reply.Document last;
        synchronized (lastDocuments) {
            last = lastDocuments.get(host);
        }
        return last == null ? new Reply.Empty() : last;
    }

    private static String hostKey(String host) {
        return host == null ? "" : host.toLowerCase(Locale.ROOT);
    }
}

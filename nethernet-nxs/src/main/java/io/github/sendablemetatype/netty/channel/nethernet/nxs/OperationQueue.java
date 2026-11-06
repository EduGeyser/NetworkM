package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Runs the client's operations one at a time without blocking a thread: each starts on the lifecycle executor once
 * the previous one's stage has completed, so signed requests go out strictly in sequence even though every wait in
 * between is asynchronous.
 */
final class OperationQueue {
    private final Executor executor;
    /** Touched only on the executor. */
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

    OperationQueue(Executor executor) {
        this.executor = executor;
    }

    /**
     * @param work Started on the executor; its stage may complete on any thread
     * @return Completes with the work's result, on the executor
     */
    <T> CompletableFuture<T> submit(Async.Step<T> work) {
        CompletableFuture<T> result = new CompletableFuture<>();
        executor.execute(() -> {
            CompletableFuture<Void> previous = tail, done = new CompletableFuture<>();
            tail = done;
            previous.whenCompleteAsync((ignored, alsoIgnored) -> {
                Async.attempt(work).whenCompleteAsync((value, failure) -> {
                    done.complete(null);
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(Async.unwrap(failure));
                    }
                }, executor);
            }, executor);
        });
        return result;
    }
}

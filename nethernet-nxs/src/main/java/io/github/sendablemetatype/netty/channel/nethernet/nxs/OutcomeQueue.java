/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ProviderException;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Outcome reports. Each second the queue collects new outcomes into the saved state; it sends a batch once the oldest
 * unsent outcome has waited ten seconds or a full batch of 100 is ready, independently of heartbeat timing. Every
 * outcome is valid when queued, since its types enforce the schema, so a rejected batch can only be the provider's
 * doing; a definitive rejection drops that batch rather than blocking later reports. At most 1000 wait.
 */
final class OutcomeQueue {
    static final long BATCH_NANOS = TimeUnit.SECONDS.toNanos(10);
    static final int BATCH_EVENTS = 100;
    static final int MAX_PENDING_EVENTS = 1000;

    private final ClientRuntime runtime;
    private final Exchanger exchanger;
    /** Game outcomes from application threads, until the next collection. */
    private final ArrayBlockingQueue<Wire.OutcomeEvent> game = new ArrayBlockingQueue<>(256);
    private long nextNanos;
    /** When the oldest unsent outcome is due to be sent, or 0 while none waits. */
    private long dueNanos;
    /** Consecutive failures, which lengthen the wait before the next attempt. */
    private int failures;

    OutcomeQueue(ClientRuntime runtime, Exchanger exchanger) {
        this.runtime = runtime;
        this.exchanger = exchanger;
    }

    /** Takes a game outcome from any thread without blocking. */
    boolean offer(Wire.OutcomeEvent event) {
        return game.offer(event);
    }

    boolean due() {
        return runtime.clock.nanos() - nextNanos >= 0;
    }

    void clear() {
        game.clear();
    }

    void failed(Throwable failure) {
        failures++;
        nextNanos = runtime.clock.nanos() + ProviderFailures.retryDelayNanos(failure, failures);
        if (!runtime.fenced) {
            runtime.log.failed(ProviderLog.Operation.EVENTS, ProviderFailures.describe(failure));
        }
    }

    /**
     * Collects new outcomes into the saved state, then sends a batch when one is due.
     *
     * @param immediately Send what is pending without waiting, as drain and shutdown do
     * @return Whether outcomes are still pending
     */
    CompletableFuture<Boolean> flush(boolean immediately) {
        return Async.attempt(() -> {
            List<Wire.OutcomeEvent> pending = new ArrayList<>(runtime.keeper.state().pendingEvents());
            int room = Math.min(BATCH_EVENTS, MAX_PENDING_EVENTS - pending.size());
            List<Wire.OutcomeEvent> fresh = new ArrayList<>();
            if (room > 0) {
                List<TransportOutcome> observed = runtime.transport.pollOutcomes(room);
                if (observed.size() > room) {
                    throw new IllegalStateException("Transport returned more outcomes than requested");
                }
                for (TransportOutcome outcome : observed) {
                    fresh.add(new Wire.OutcomeEvent(outcome.ticketId(), outcome.stage(), outcome.occurredAt(),
                            outcome.reason(), outcome.remote()));
                    // Assisted failures were already reported by the control carrier
                    if (outcome.stage() == OutcomeStage.FAILED && !outcome.reason().startsWith("assisted_")) {
                        runtime.log.playerConnectionFailed();
                    }
                }
                game.drainTo(fresh, room - fresh.size());
            }
            if (!fresh.isEmpty()) {
                // Saved in the round that collects them; only the send waits for the batch
                pending.addAll(fresh);
                runtime.keeper.save(runtime.keeper.state().withPendingEvents(pending));
            }
            if (pending.isEmpty()) {
                dueNanos = 0;
                return CompletableFuture.completedFuture(false);
            }
            long now = runtime.clock.nanos();
            if (dueNanos == 0) {
                dueNanos = now + BATCH_NANOS;
            }
            if (!immediately && pending.size() < BATCH_EVENTS && now - dueNanos < 0) {
                return CompletableFuture.completedFuture(true);
            }
            List<Wire.OutcomeEvent> batch = List.copyOf(pending.subList(0, Math.min(BATCH_EVENTS, pending.size())));
            boolean endpoints = runtime.discovery != null && runtime.discovery.advertises(Wire.CONNECTIVITY);
            return exchanger.signed("outcomes", WireCodec.outcomes(batch, endpoints))
                    .thenApply(ignored -> true)
                    .exceptionallyCompose(failure -> {
                        // Any other 4xx had no effect and never will, so it must not block later reports
                        if (ProviderFailures.refusal(failure)
                                .filter(refused -> !runtime.fenced && refused.status() / 100 == 4
                                        && refused.status() != 429)
                                .isEmpty()) {
                            return CompletableFuture.failedFuture(failure);
                        }
                        runtime.log.detail(ProviderLog.Operation.EVENTS, "Dropped " + batch.size()
                                + " rejected reports: " + ((ProviderException) Async.unwrap(failure)).code());
                        return CompletableFuture.completedFuture(true);
                    })
                    .thenApply(ignored -> {
                        // Dropped by position, once the exchange is done, so no view spans the request
                        List<Wire.OutcomeEvent> remaining = new ArrayList<>(runtime.keeper.state().pendingEvents());
                        remaining.subList(0, Math.min(batch.size(), remaining.size())).clear();
                        try {
                            runtime.keeper.save(runtime.keeper.state().withPendingEvents(remaining));
                        } catch (java.io.IOException failure) {
                            throw new java.io.UncheckedIOException(failure);
                        }
                        // Any left over start the next batch
                        dueNanos = 0;
                        failures = 0;
                        runtime.log.recovered(ProviderLog.Operation.EVENTS);
                        return !remaining.isEmpty();
                    });
        });
    }

    /** Sends everything pending at once, a batch at a time, up to what the saved state keeps. */
    CompletableFuture<Void> flushAll() {
        return flushAll(0);
    }

    private CompletableFuture<Void> flushAll(int batch) {
        if (batch >= MAX_PENDING_EVENTS / BATCH_EVENTS) {
            return Async.done();
        }
        return flush(true).thenCompose(more -> more ? flushAll(batch + 1) : Async.done());
    }
}

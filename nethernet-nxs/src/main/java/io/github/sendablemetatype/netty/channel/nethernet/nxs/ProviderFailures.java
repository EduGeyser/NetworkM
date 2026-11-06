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

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** How the client describes failures to the operator and how long it waits after them. */
final class ProviderFailures {
    static final String RATE_LIMITED = "rate_limited", CAPACITY_REACHED = "capacity_reached",
            RECOVERY_REQUIRED = "recovery_required", RECOVERY_UNAVAILABLE = "recovery_unavailable",
            GENERATION_FENCED = "generation_fenced", REGISTRATION_GONE = "registration_gone";
    /** Failed check-ins and reports wait this long, doubling with each further failure up to the cap. */
    static final long RETRY_NANOS = TimeUnit.SECONDS.toNanos(10);
    static final long MAX_BACKOFF_NANOS = TimeUnit.MINUTES.toNanos(2);

    private ProviderFailures() {
    }

    /**
     * The provider's words, or the failure's type. Network and file errors add their message, such as the host that
     * did not resolve or why TLS failed, and their cause's; other messages can quote a response, so they are left
     * out.
     */
    static String describe(Throwable failure) {
        failure = Async.unwrap(failure);
        if (failure instanceof ProviderException) {
            return failure.getMessage();
        }
        String described = described(failure);
        return failure.getCause() instanceof IOException cause && cause != failure
                ? described + " (" + described(cause) + ")"
                : described;
    }

    /** The type, and for an IOException its message on one line of at most 200 characters. */
    private static String described(Throwable failure) {
        String type = failure.getClass().getSimpleName();
        String message = failure instanceof IOException ? failure.getMessage() : null;
        if (message == null || message.isBlank()) {
            return type;
        }
        String line = message.replaceAll("[\\p{Cntrl}\\s]+", " ").strip();
        return type + ": " + (line.length() > 200 ? line.substring(0, 197) + "..." : line);
    }

    /** The provider's refusal, if this failure is one. */
    static Optional<ProviderException> refusal(Throwable failure) {
        return Async.unwrap(failure) instanceof ProviderException refused ? Optional.of(refused) : Optional.empty();
    }

    static boolean refusedWith(Throwable failure, String code) {
        return refusal(failure).filter(refused -> code.equals(refused.code())).isPresent();
    }

    /** How long to wait before registering again, when the provider refused only for now and said for how long. */
    static Optional<Duration> registrationRetry(Throwable failure) {
        return refusal(failure)
                .filter(refused -> RATE_LIMITED.equals(refused.code()) || CAPACITY_REACHED.equals(refused.code()))
                .flatMap(ProviderException::retryAfter)
                .map(wait -> wait.compareTo(Duration.ofNanos(RETRY_NANOS)) < 0 ? Duration.ofNanos(RETRY_NANOS) : wait);
    }

    /**
     * How long to wait after the given number of consecutive failures: at least 10 seconds and no sooner than the
     * provider's Retry-After when it sent one, otherwise 10 seconds doubling to 2 minutes, plus up to a tenth as
     * jitter.
     */
    static long retryDelayNanos(Throwable failure, int failures) {
        Optional<Duration> retryAfter = refusal(failure).flatMap(ProviderException::retryAfter);
        long delay = retryAfter.isPresent()
                ? Math.max(RETRY_NANOS, retryAfter.get().toNanos())
                : Math.min(RETRY_NANOS << Math.min(Math.max(failures - 1, 0), 4), MAX_BACKOFF_NANOS);
        return delay + ThreadLocalRandom.current().nextLong(delay / 10 + 1);
    }
}

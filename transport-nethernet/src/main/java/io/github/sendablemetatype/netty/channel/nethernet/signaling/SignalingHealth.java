package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.time.Duration;
import java.util.Objects;

/**
 * A snapshot of a signaling source's health, as reported to health listeners.
 *
 * @param state          the current state
 * @param failedAttempts connection attempts that failed in a row since the last success
 * @param lastFailure    why the last attempt failed or the connection was lost; null when the
 *                       state change has no failure behind it
 * @param retryIn        when the next attempt starts; null when none is scheduled
 */
public record SignalingHealth(SignalingState state, int failedAttempts, Throwable lastFailure, Duration retryIn) {
    public SignalingHealth {
        Objects.requireNonNull(state, "state");
    }
}

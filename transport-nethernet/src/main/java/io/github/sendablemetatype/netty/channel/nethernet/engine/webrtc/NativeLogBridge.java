package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.webrtc.logging.LogSink;
import io.github.sendablemetatype.webrtc.logging.Logging;
import io.github.sendablemetatype.webrtc.logging.Logging.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Routes libwebrtc's native log into SLF4J. The native log is process wide,
 * so engines share one sink, registered at the lowest threshold any open
 * engine asks for; levels below that are the host's logging configuration.
 */
final class NativeLogBridge {
    /** The SLF4J logger native messages go to. */
    static final String LOGGER_NAME = "io.github.sendablemetatype.netty.nethernet.webrtc.native";

    /**
     * webrtc-java reads its native threshold from this property once, when it
     * loads the library; without it libwebrtc formats no message for sinks.
     */
    static final String SEVERITY_PROPERTY = "io.github.sendablemetatype.webrtc.logging.severity";

    private static final Logger NATIVE = LoggerFactory.getLogger(LOGGER_NAME);
    private static final LogSink SINK = NativeLogBridge::log;

    // Guarded by the class.
    private static final List<Severity> thresholds = new ArrayList<>();
    private static Severity installed = Severity.NONE;

    private NativeLogBridge() {
    }

    /**
     * Asks for native messages at or above the threshold. Call before the
     * engine first touches webrtc-java: if the host set no native threshold,
     * this sets one for the library to read when it loads.
     */
    static synchronized void acquire(Severity threshold) {
        if (threshold == Severity.NONE) {
            return;
        }
        if (System.getProperty(SEVERITY_PROPERTY) == null) {
            System.setProperty(SEVERITY_PROPERTY, threshold.name());
        }
        thresholds.add(threshold);
        update();
    }

    /** Withdraws a threshold given to {@link #acquire}. */
    static synchronized void release(Severity threshold) {
        if (thresholds.remove(threshold)) {
            update();
        }
    }

    private static void update() {
        Severity lowest = thresholds.stream().min(Enum::compareTo).orElse(Severity.NONE);
        if (lowest == installed) {
            return;
        }
        if (installed != Severity.NONE) {
            Logging.removeLogSink(SINK);
        }
        if (lowest != Severity.NONE) {
            Logging.addLogSink(lowest, SINK);
        }
        installed = lowest;
    }

    /**
     * libwebrtc's INFO is debugging detail and its VERBOSE diagnostic detail
     * (see {@link Severity}), so they map one level down. Errors and warnings
     * that report routine events ({@link RoutineNativeLine}) log at DEBUG.
     */
    static void log(Severity severity, String message) {
        String line = message.stripTrailing();
        if ((severity == Severity.ERROR || severity == Severity.WARNING) && RoutineNativeLine.matches(line)) {
            NATIVE.debug(line);
            return;
        }
        switch (severity) {
            case ERROR -> NATIVE.error(line);
            case WARNING -> NATIVE.warn(line);
            case INFO -> NATIVE.debug(line);
            case VERBOSE -> NATIVE.trace(line);
            case NONE -> {
            }
        }
    }
}

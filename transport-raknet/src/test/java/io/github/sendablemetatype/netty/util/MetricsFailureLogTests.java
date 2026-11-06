package io.github.sendablemetatype.netty.util;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.JdkLoggerFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsFailureLogTests {

    @Test
    void theFirstFailureOfEachHookWarnsAndLaterOnesLogAtDebug() {
        Logger backend = Logger.getLogger(MetricsFailureLogTests.class.getName());
        List<LogRecord> records = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Level previous = backend.getLevel();
        backend.setLevel(Level.ALL);
        backend.addHandler(capture);
        try {
            InternalLogger log = ((JdkLoggerFactory) JdkLoggerFactory.INSTANCE).newInstance(backend.getName());
            RuntimeException cause = new IllegalStateException("Metrics unavailable");

            MetricsFailureLog.failed(log, "MetricsFailureLogTests.first", "a", cause);
            MetricsFailureLog.failed(log, "MetricsFailureLogTests.first", "b", cause);
            MetricsFailureLog.failed(log, "MetricsFailureLogTests.second", "c", cause);

            // Netty's JDK logger writes debug as FINE
            assertEquals(List.of(Level.WARNING, Level.FINE, Level.WARNING),
                    records.stream().map(LogRecord::getLevel).toList());
            assertEquals(cause, records.getFirst().getThrown());
        } finally {
            backend.removeHandler(capture);
            backend.setLevel(previous);
        }
    }
}

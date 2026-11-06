package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.webrtc.logging.Logging;
import io.github.sendablemetatype.webrtc.logging.Logging.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Needs natives; the build sets webrtc-java's native threshold for the test JVM before any test loads them. */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
class NativeLogBridgeTest {

    @Test
    void nativeMessagesAtTheThresholdReachTheirLogger() {
        Logger backend = Logger.getLogger(NativeLogBridge.LOGGER_NAME);
        Level previous = backend.getLevel();
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
        backend.setLevel(Level.ALL);
        backend.addHandler(capture);
        NativeLogBridge.acquire(Severity.WARNING);
        try {
            Logging.info("below the threshold");
            Logging.warn("native warning for the bridge");

            List<LogRecord> bridged = records.stream()
                    .filter(record -> record.getMessage().contains("for the bridge")
                            || record.getMessage().contains("below the threshold"))
                    .toList();
            assertEquals(1, bridged.size(), bridged.toString());
            assertTrue(bridged.getFirst().getMessage().contains("native warning for the bridge"));
            assertEquals(Level.WARNING, bridged.getFirst().getLevel());
        } finally {
            NativeLogBridge.release(Severity.WARNING);
            backend.removeHandler(capture);
            backend.setLevel(previous);
        }
    }
}

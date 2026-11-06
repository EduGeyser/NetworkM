package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.webrtc.logging.Logging;
import io.github.sendablemetatype.webrtc.logging.Logging.Severity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

class NativeLogBridgeTest {
    private final Logger backend = Logger.getLogger(NativeLogBridge.LOGGER_NAME);
    private final List<LogRecord> records = new CopyOnWriteArrayList<>();
    private final Handler capture = new Handler() {
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
    private Level previous;

    @BeforeEach
    void capture() {
        previous = backend.getLevel();
        backend.setLevel(Level.ALL);
        backend.addHandler(capture);
    }

    @AfterEach
    void restore() {
        backend.removeHandler(capture);
        backend.setLevel(previous);
    }

    /** Needs natives; the build sets webrtc-java's native threshold for the test JVM before any test loads them. */
    @Test
    @EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
    void nativeMessagesAtTheThresholdReachTheirLogger() {
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
        }
    }

    @Test
    void routineErrorsLogAtDebugAndOthersKeepTheirLevel() {
        List<String> lines = List.of(
                "(dcsctp_transport.cc:561): DcSctpTransport9001->OnAborted("
                        + "error=PEER_REPORTED, message=User-Initiated Abort, reason=Close called).",
                "(dcsctp_transport.cc:561): DcSctpTransport9002->OnAborted("
                        + "error=TOO_MANY_RETRIES, message=Too many retransmissions).",
                "(turn_port.cc:1736): Port[9003:0:1:0:relay:Net[eth0:192.168.1.x/24:Ethernet:id=1]]: "
                        + "Received TURN create permission error response, id=4a6f, code=403, rtt=31");
        NativeLogBridge.log(Severity.ERROR, lines.get(0) + "\n");
        NativeLogBridge.log(Severity.ERROR, lines.get(1) + "\n");
        NativeLogBridge.log(Severity.WARNING, lines.get(2) + "\n");

        // Native tests in the same JVM may log through the bridge meanwhile
        List<LogRecord> bridged = records.stream().filter(record -> lines.contains(record.getMessage())).toList();
        assertEquals(lines, bridged.stream().map(LogRecord::getMessage).toList());
        // slf4j-jdk14 logs DEBUG as FINE
        assertEquals(List.of(Level.FINE, Level.SEVERE, Level.FINE), bridged.stream().map(LogRecord::getLevel).toList());
    }
}

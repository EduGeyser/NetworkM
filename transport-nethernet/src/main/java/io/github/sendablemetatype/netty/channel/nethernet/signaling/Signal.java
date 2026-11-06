package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One received NetherNet signal, in the wire form Xbox RPC and LAN carry:
 * {@code <type> <connection id> <payload>}.
 *
 * <p>The connection ID is opaque: it is matched and echoed exactly as the initiator wrote it, never
 * parsed as a number, so {@code 00042} stays {@code 00042}. Vanilla writes decimal uint64 values,
 * but that reading is secondhand (go-nethernet) and nothing here depends on it.
 *
 * @param type         the signal type, one of the {@code RTC_NEGOTIATION_*} constants for every
 *                     signal NetherNet acts on
 * @param connectionId the connection the signal belongs to, as written
 * @param payload      the description, candidate or error code; empty when the signal has none
 */
public record Signal(String type, String connectionId, String payload) {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(Signal.class);
    private static final int MAX_CONNECTION_ID_LENGTH = 64;

    public Signal {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(payload, "payload");
    }

    /**
     * Parses a received signal. A signal whose connection ID is missing or invalid (see
     * {@link #isValidConnectionId}) is dropped with a debug line that names neither the ID nor the
     * payload: the ID may hold control characters, and an offer carries the client's identity token.
     *
     * @param raw the signal as received
     * @return the signal, or empty when it is dropped
     */
    public static Optional<Signal> parse(String raw) {
        String[] parts = raw.split(" ", 3);
        String connectionId = parts.length > 1 ? parts[1] : "";
        if (!isValidConnectionId(connectionId)) {
            log.debug("Dropped a {} signal with an invalid connection ID ({} chars)", knownType(parts[0]),
                    connectionId.length());
            return Optional.empty();
        }
        return Optional.of(new Signal(parts[0], connectionId, parts.length > 2 ? parts[2] : ""));
    }

    /**
     * Whether a received connection ID is one NetherNet routes: 1 to {@value #MAX_CONNECTION_ID_LENGTH}
     * printable ASCII characters without spaces (0x21 to 0x7E). The ID goes into logs and back onto
     * the wire, so control characters and unbounded lengths are refused rather than passed along.
     *
     * @param connectionId the ID as received
     * @return true when signals with this ID may be routed
     */
    static boolean isValidConnectionId(String connectionId) {
        int length = connectionId.length();
        if (length == 0 || length > MAX_CONNECTION_ID_LENGTH) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            char c = connectionId.charAt(i);
            if (c < 0x21 || c > 0x7E) {
                return false;
            }
        }
        return true;
    }

    /**
     * A new connection ID for an attempt NetherNet starts or answers. Received IDs are opaque, but
     * other peers may parse IDs as numbers (go-nethernet does), so generated ones keep the form
     * vanilla writes: a nonzero unsigned 64-bit value in decimal.
     *
     * @return the new ID
     */
    public static String newConnectionId() {
        return Long.toUnsignedString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
    }

    // The type is as untrusted as the ID, so a log line names it only when it is one NetherNet knows.
    private static String knownType(String type) {
        return switch (type) {
            case NetherNetConstants.RTC_NEGOTIATION_CONNECT_REQUEST,
                 NetherNetConstants.RTC_NEGOTIATION_CONNECT_RESPONSE,
                 NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD,
                 NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR -> type;
            default -> "unknown";
        };
    }
}

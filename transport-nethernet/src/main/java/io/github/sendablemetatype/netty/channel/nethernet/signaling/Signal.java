package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Optional;

/**
 * One NetherNet signal, in the wire form Xbox RPC and LAN carry:
 * {@code <type> <connection id> <payload>}.
 *
 * <p>The connection ID is opaque: it is matched and echoed exactly as the initiator wrote it, never
 * parsed as a number, so {@code 00042} stays {@code 00042}. Vanilla writes decimal uint64 values,
 * but that reading is secondhand (go-nethernet) and nothing here depends on it.
 *
 * @param type         the signal type
 * @param connectionId the connection the signal belongs to, as written
 * @param payload      the description, candidate or error code; empty when the signal has none
 */
public record Signal(SignalType type, String connectionId, String payload) {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(Signal.class);
    private static final int MAX_CONNECTION_ID_LENGTH = 64;
    /** How much of an unknown type a log line shows. */
    private static final int MAX_LOGGED_TYPE_LENGTH = 32;
    /** Thread safe; one draw per join, so its cost does not matter. */
    private static final SecureRandom CONNECTION_IDS = new SecureRandom();

    /**
     * @throws IllegalArgumentException if the connection ID is not valid (see
     *                                  {@link #isValidConnectionId}); a custom signaling that builds
     *                                  signals itself gets the same guarantee as the wire paths
     */
    public Signal {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        requireValidConnectionId(connectionId);
    }

    /**
     * Parses a received signal. A signal of a type other than the four {@link SignalType}s is
     * dropped with a debug line that shows the type escaped and cut short, since it is remote
     * text. A signal whose connection ID is missing or invalid (see {@link #isValidConnectionId})
     * is dropped with a debug line that names neither the ID nor the payload: the ID may hold
     * control characters, and an offer carries the client's identity token.
     *
     * @param raw the signal as received
     * @return the signal, or empty when it is dropped
     */
    public static Optional<Signal> parse(String raw) {
        String[] parts = raw.split(" ", 3);
        Optional<SignalType> type = SignalType.fromWireName(parts[0]);
        if (type.isEmpty()) {
            log.debug("Dropped a signal of an unknown type \"{}\"", escaped(parts[0]));
            return Optional.empty();
        }
        String connectionId = parts.length > 1 ? parts[1] : "";
        if (!isValidConnectionId(connectionId)) {
            log.debug("Dropped a {} signal with an invalid connection ID ({} chars)", type.get().wireName(),
                    connectionId.length());
            return Optional.empty();
        }
        return Optional.of(new Signal(type.get(), connectionId, parts.length > 2 ? parts[2] : ""));
    }

    /**
     * @return the signal as the wire carries it; {@link #parse} reads it back
     */
    public String wire() {
        return type.wireName() + " " + connectionId + " " + payload;
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

    /** Refuses an invalid connection ID without echoing it, since it may hold control characters. */
    static void requireValidConnectionId(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        if (!isValidConnectionId(connectionId)) {
            throw new IllegalArgumentException("A connection ID is 1 to " + MAX_CONNECTION_ID_LENGTH
                    + " printable ASCII characters without spaces (got " + connectionId.length() + " chars)");
        }
    }

    /**
     * A new connection ID for an attempt NetherNet starts or answers. Received IDs are opaque, but
     * other peers may parse IDs as numbers (go-nethernet does), so generated ones keep the form
     * vanilla writes: a nonzero unsigned 64-bit value in decimal. The ID comes from a secure
     * random source, so a third party cannot predict the next one.
     *
     * @return the new ID
     */
    public static String newConnectionId() {
        return Long.toUnsignedString(CONNECTION_IDS.nextLong(1, Long.MAX_VALUE));
    }

    /**
     * Remote text fit for a log line: printable ASCII as it is, with a backslash before a backslash
     * or quote, every other character as a Java Unicode escape, and at most
     * {@value #MAX_LOGGED_TYPE_LENGTH} characters of the original.
     */
    static String escaped(String text) {
        int shown = Math.min(text.length(), MAX_LOGGED_TYPE_LENGTH);
        StringBuilder out = new StringBuilder(shown + 16);
        for (int i = 0; i < shown; i++) {
            char c = text.charAt(i);
            if (c == '\\' || c == '"') {
                out.append('\\').append(c);
            } else if (c >= 0x20 && c <= 0x7E) {
                out.append(c);
            } else {
                out.append(String.format("\\u%04x", (int) c));
            }
        }
        if (text.length() > shown) {
            out.append("...");
        }
        return out.toString();
    }
}

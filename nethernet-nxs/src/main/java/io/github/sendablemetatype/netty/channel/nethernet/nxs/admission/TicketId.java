package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * The correlation ID that outcome reports carry for one admitted join: 32 lowercase hex digits. For a stateless
 * ticket it is the first 16 bytes of SHA-256 over the ASCII answer ufrag; for an assisted join it is the join's ID.
 * It is never taken from an unauthenticated packet.
 *
 * @param value The ID as 32 lowercase hex digits
 */
public record TicketId(String value) {
    private static final Pattern FORMAT = Pattern.compile("[0-9a-f]{32}");

    public TicketId {
        if (value == null || !FORMAT.matcher(value).matches()) {
            throw new IllegalArgumentException("A ticket ID is 32 lowercase hex digits");
        }
    }

    /**
     * @param answerUfrag The admitted token, the host's ICE ufrag
     * @return The ticket ID the wire reference derives from it
     */
    public static TicketId ofToken(String answerUfrag) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(answerUfrag.getBytes(StandardCharsets.US_ASCII));
            return new TicketId(HexFormat.of().formatHex(digest, 0, 16));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    @Override
    public String toString() {
        return value;
    }
}

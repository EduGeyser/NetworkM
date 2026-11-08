package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;

import java.util.Objects;

/**
 * Why client signaling gave up on an offer before an answer arrived: it could not deliver it, no
 * answer came in time, or the server refused it without a signal of its own (over HTTP, a refusal
 * status or a {@code 2xx} without an SDP).
 *
 * @param failure   how the client channel reports it
 * @param errorCode the NetherNet error code that stands for it, as a {@code CONNECTERROR} signal
 *                  carries one
 * @param reason    what happened, for messages; remote text in it is cut short and reduced to
 *                  printable ASCII
 */
public record SignalingFailure(NetherConnectionFailure failure, int errorCode, String reason) {
    /** How much remote text is worth carrying into a reason. */
    static final int REMOTE_TEXT_LIMIT = 200;

    public SignalingFailure {
        Objects.requireNonNull(failure, "failure");
        Objects.requireNonNull(reason, "reason");
    }

    /**
     * Remote text, such as a server's error body, fit for a reason: the remote side controls it,
     * so it is cut short and reduced to printable ASCII before it can reach a log.
     *
     * @return {@code ": "} and the text, or empty when there is none
     */
    static String quote(String text) {
        if (text == null || text.isBlank()) {
            return "";
        }
        StringBuilder out = new StringBuilder(": ");
        for (int i = 0; i < text.length() && out.length() < REMOTE_TEXT_LIMIT + 2; i++) {
            char c = text.charAt(i);
            out.append(c >= ' ' && c <= '~' ? c : ' ');
        }
        return out.toString().stripTrailing();
    }
}

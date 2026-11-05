package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The few changes the adapter makes to session descriptions on their way into and out of libwebrtc. */
final class SdpEdits {
    private static final Pattern IDENTITY = Pattern.compile("(?m)^a=identity:[^\\r\\n]*\\r?\\n?");
    private static final Pattern UFRAG = Pattern.compile("(?m)^a=ice-ufrag:[^\\r\\n]*");
    private static final Pattern PASSWORD = Pattern.compile("(?m)^a=ice-pwd:[^\\r\\n]*");
    private static final String END_OF_CANDIDATES = "a=end-of-candidates";

    private SdpEdits() {
    }

    /**
     * Removes {@code a=identity} lines. The assertion belongs to signaling,
     * which validates it on the unchanged description; NetherNet's spec says to
     * strip it before the engine, so nothing depends on how libwebrtc treats it.
     */
    static String withoutIdentity(String sdp) {
        return IDENTITY.matcher(sdp).replaceAll("");
    }

    /**
     * Sets the ICE credentials of every media section. libwebrtc accepts a
     * local description whose credentials differ from the ones it created
     * unless the WebRTC-NoSdpMangleUfrag or WebRTC-NoSdpMangleReject field
     * trials say otherwise (pc/sdp_offer_answer.cc).
     */
    static String withCredentials(String sdp, IceCredentials credentials) {
        String ufrag = UFRAG.matcher(sdp).replaceAll(Matcher.quoteReplacement("a=ice-ufrag:" + credentials.ufrag()));
        return PASSWORD.matcher(ufrag).replaceAll(Matcher.quoteReplacement("a=ice-pwd:" + credentials.password()));
    }

    /**
     * Appends {@code a=end-of-candidates} to a description with every gathered
     * candidate. libwebrtc's local description collects the candidates but
     * never the terminator, which it only signals on the trickle path; the
     * NetherNet spec's example answer carries it.
     */
    static String withEndOfCandidates(String sdp) {
        if (sdp.contains(END_OF_CANDIDATES)) {
            return sdp;
        }
        return sdp.endsWith("\n") ? sdp + END_OF_CANDIDATES + "\r\n" : sdp + "\r\n" + END_OF_CANDIDATES + "\r\n";
    }

    /** A candidate line as libwebrtc parses it: without the {@code a=} prefix and line break. */
    static String candidateAttribute(String candidate) {
        String trimmed = candidate.strip();
        return trimmed.startsWith("a=") ? trimmed.substring(2) : trimmed;
    }
}

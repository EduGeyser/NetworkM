package io.github.sendablemetatype.netty.channel.nethernet.ice;

import java.util.ArrayList;
import java.util.List;

/** Line handling shared by the SDP rewrites. */
final class SdpText {
    private SdpText() {
    }

    /**
     * The lines of a description without their ends, blank lines left out: libwebrtc rejects a
     * description with an empty line anywhere, the end included ({@code api/webrtc_sdp.cc}).
     */
    static List<String> lines(String sdp) {
        List<String> lines = new ArrayList<>();
        for (String line : sdp.split("\r\n|\n")) {
            if (!line.isEmpty()) {
                lines.add(line);
            }
        }
        return lines;
    }

    /** The lines joined into a description, each ended by CRLF as SDP requires. */
    static String join(List<String> lines) {
        StringBuilder out = new StringBuilder();
        for (String line : lines) {
            out.append(line).append("\r\n");
        }
        return out.toString();
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.ice;

import java.util.Objects;

/**
 * A remote candidate ready for the engine, tagged with the m-section it belongs to. Maps one to
 * one onto webrtc-java's {@code new RTCIceCandidate(sdpMid, sdpMLineIndex, sdp)}.
 *
 * @param sdp           The candidate, {@code candidate:...}
 * @param sdpMid        The {@code a=mid} of its m-section, empty when the description named none,
 *                      in which case the engine goes by the index
 * @param sdpMLineIndex The index of its m-section, from 0
 */
public record RemoteCandidate(String sdp, String sdpMid, int sdpMLineIndex) {
    public RemoteCandidate {
        Objects.requireNonNull(sdp, "sdp");
        Objects.requireNonNull(sdpMid, "sdpMid");
    }
}

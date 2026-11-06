package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.webrtc.RTCDataChannel;
import io.github.sendablemetatype.webrtc.RTCDataChannelBuffer;
import io.github.sendablemetatype.webrtc.RTCDataChannelObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelSendObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelState;
import io.github.sendablemetatype.webrtc.RTCError;
import io.github.sendablemetatype.webrtc.RTCErrorDetailType;
import io.github.sendablemetatype.webrtc.RTCErrorType;

import java.nio.ByteBuffer;
import java.util.OptionalInt;

/**
 * The calls the adapter makes on a native data channel. A seam for tests: the
 * production implementation forwards to webrtc-java.
 */
interface NativeLink {

    /** Read without waiting for an engine thread: libwebrtc answers these off its proxy. */
    ChannelParameters parameters();

    /** Waits for the factory's network thread, unless called there. */
    RTCDataChannelState state();

    /**
     * Why the channel failed or closed, or null if libwebrtc reports nothing.
     * Waits for the factory's network thread, unless called there.
     */
    LinkError error();

    void registerObserver(RTCDataChannelObserver observer);

    void unregisterObserver();

    /** Copies the buffer's readable window before returning; the observer runs once. */
    void sendAsync(ByteBuffer data, RTCDataChannelSendObserver observer);

    /** Waits for the factory's network thread. */
    void close();

    /** Releases the Java side's reference to the native channel. */
    void dispose();

    /**
     * A data channel's error, as webrtc-java reports it.
     *
     * @param type the error's kind
     * @param detail the part of the connection it comes from
     * @param sctpCauseCode the SCTP cause code of an SCTP failure, or null
     * @param message libwebrtc's description, or null
     */
    record LinkError(RTCErrorType type, RTCErrorDetailType detail, Integer sctpCauseCode, String message) {
    }

    /** Forwards to a webrtc-java data channel. */
    record RtcLink(RTCDataChannel channel) implements NativeLink {

        @Override
        public ChannelParameters parameters() {
            return parametersOf(channel.getLabel(), channel.isOrdered(), channel.isReliable(),
                    channel.getMaxRetransmits(), channel.getMaxPacketLifeTime(), channel.isNegotiated(),
                    channel.getProtocol());
        }

        @Override
        public RTCDataChannelState state() {
            return channel.getState();
        }

        @Override
        public LinkError error() {
            RTCError error = channel.getError();
            return error == null ? null
                    : new LinkError(error.getType(), error.getDetail(), error.getSctpCauseCode(), error.getMessage());
        }

        @Override
        public void registerObserver(RTCDataChannelObserver observer) {
            channel.registerObserver(observer);
        }

        @Override
        public void unregisterObserver() {
            channel.unregisterObserver();
        }

        @Override
        public void sendAsync(ByteBuffer data, RTCDataChannelSendObserver observer) {
            channel.sendAsync(new RTCDataChannelBuffer(data, true), observer);
        }

        @Override
        public void close() {
            channel.close();
        }

        @Override
        public void dispose() {
            channel.dispose();
        }
    }

    /**
     * Builds a channel's parameters from webrtc-java's getters, which report
     * an absent limit as 0. libwebrtc calls a channel reliable when neither
     * limit is set, so a reliable channel has none. An unreliable one with
     * both reported as 0 is taken as {@code maxRetransmits} 0: the WebRTC
     * specification allows only one of the two, and NetherNet clients open
     * their unreliable channel with {@code maxRetransmits} 0.
     */
    static ChannelParameters parametersOf(String label, boolean ordered, boolean reliable, int maxRetransmits,
                                          int maxPacketLifeTimeMs, boolean negotiated, String protocol) {
        OptionalInt retransmits = OptionalInt.empty();
        OptionalInt lifeTime = OptionalInt.empty();
        if (!reliable) {
            if (maxPacketLifeTimeMs > 0) {
                lifeTime = OptionalInt.of(maxPacketLifeTimeMs);
                if (maxRetransmits > 0) {
                    retransmits = OptionalInt.of(maxRetransmits);
                }
            } else {
                retransmits = OptionalInt.of(maxRetransmits);
            }
        }
        return new ChannelParameters(label, ordered, retransmits, lifeTime, negotiated, protocol == null ? "" : protocol);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.EngineConformanceTest;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.webrtc.RTCDataChannelBuffer;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.ByteBuffer;

/** The conformance suite on libwebrtc; runs where the build provides webrtc-java natives. */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
class WebRtcEngineConformanceTest extends EngineConformanceTest {

    @Override
    protected NetherNetEngine newEngine() {
        return WebRtcEngine.builder().factories(2).build();
    }

    @Override
    protected boolean realNetwork() {
        return true;
    }

    /** Sends on the native channel, which waits for libwebrtc to take the message, so it goes before later sends. */
    @Override
    protected void sendText(DataLink link, byte[] message) throws Exception {
        NativeLink.RtcLink channel = (NativeLink.RtcLink) ((WebRtcLink) link).channel();
        channel.channel().send(new RTCDataChannelBuffer(ByteBuffer.wrap(message), false));
    }
}

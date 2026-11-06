package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.EngineConformanceTest;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

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
}

package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.nio.ByteBuffer;

/** The conformance suite on the fake engine's loopback mode. */
class FakeEngineConformanceTest extends EngineConformanceTest {

    @Override
    protected NetherNetEngine newEngine() {
        return FakeEngine.loopback();
    }

    @Override
    protected boolean realNetwork() {
        return false;
    }

    @Override
    protected void sendText(DataLink link, byte[] message) {
        ((FakeDataLink) link).sendText(ByteBuffer.wrap(message), ignored -> {
        });
    }
}

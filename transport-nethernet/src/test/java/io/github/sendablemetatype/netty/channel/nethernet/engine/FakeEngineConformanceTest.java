package io.github.sendablemetatype.netty.channel.nethernet.engine;

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
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeDataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.List;

/** A child channel on a fake peer, without a server or signaling. */
final class ChildFixture {
    static final InetSocketAddress REMOTE = new InetSocketAddress("127.0.0.1", 19132);
    static final InetSocketAddress LOCAL = new InetSocketAddress("127.0.0.1", 19133);
    static final PeerConfig ANSWERER = new PeerConfig(PeerConfig.Role.ANSWERER, List.of(),
            CandidatePolicy.DEFAULT, null, null, null);

    /** Signaling that goes nowhere. */
    static final NetherNetChildChannel.Negotiation NO_SIGNALING = new NetherNetChildChannel.Negotiation() {
        @Override
        public void localCandidate(String candidate, String mid) {
        }

        @Override
        public void failed(CloseCause cause) {
        }
    };

    final FakeEngine engine = new FakeEngine();
    final NetherNetChildChannel child;
    final FakePeer peer;
    FakeDataLink reliable;
    FakeDataLink unreliable;

    /** @param parent the server side, or null for a child that is only ever registered */
    ChildFixture(Channel parent) {
        child = new NetherNetChildChannel(parent, REMOTE, LOCAL, NO_SIGNALING);
        peer = (FakePeer) child.openPeer(engine, ANSWERER);
    }

    /** The client opens both data channels and the reliable one opens, which activates the child. */
    ChildFixture openLinks() {
        reliable = peer.remoteChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        unreliable = peer.remoteChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        peer.open(reliable);
        return this;
    }

    /** A segment arrives on the reliable data channel: the countdown header, then the payload. */
    void reliable(int... segment) {
        peer.message(reliable, bytes(segment));
    }

    void reliable(byte[] segment) {
        peer.message(reliable, segment);
    }

    /** A segment arrives on the unreliable data channel. */
    void unreliable(int... segment) {
        peer.message(unreliable, bytes(segment));
    }

    private static byte[] bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }
}

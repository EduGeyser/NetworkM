package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.webrtc.AddIceCandidateObserver;
import io.github.sendablemetatype.webrtc.CreateSessionDescriptionObserver;
import io.github.sendablemetatype.webrtc.PeerConnectionObserver;
import io.github.sendablemetatype.webrtc.RTCAnswerOptions;
import io.github.sendablemetatype.webrtc.RTCConfiguration;
import io.github.sendablemetatype.webrtc.RTCDataChannelInit;
import io.github.sendablemetatype.webrtc.RTCIceCandidate;
import io.github.sendablemetatype.webrtc.RTCOfferOptions;
import io.github.sendablemetatype.webrtc.RTCPeerConnection;
import io.github.sendablemetatype.webrtc.RTCSessionDescription;
import io.github.sendablemetatype.webrtc.RTCTransportMetrics;
import io.github.sendablemetatype.webrtc.PeerConnectionFactory;
import io.github.sendablemetatype.webrtc.SetSessionDescriptionObserver;

/**
 * The calls the adapter makes on a native peer connection. Every one of them
 * may wait for the factory's signaling or network thread, so the adapter makes
 * them only on its control thread. A seam for tests: the production
 * implementation forwards to webrtc-java.
 */
interface NativePeer {

    void setRemoteDescription(RTCSessionDescription description, SetSessionDescriptionObserver observer);

    void createOffer(CreateSessionDescriptionObserver observer);

    void createAnswer(CreateSessionDescriptionObserver observer);

    void setLocalDescription(RTCSessionDescription description, SetSessionDescriptionObserver observer);

    /** The local description with the candidates gathered so far, or null if there is none. */
    String localDescription();

    /** Chained behind pending description operations, as the standard requires. */
    void addIceCandidate(RTCIceCandidate candidate, AddIceCandidateObserver observer);

    NativeLink createDataChannel(ChannelParameters parameters);

    /** Null before the connection has a data channel transport, and after it is closed. */
    RTCTransportMetrics transportMetrics();

    void close();

    /** Creates native peers; one per pooled webrtc-java factory. */
    interface Factory {

        NativePeer createPeer(RTCConfiguration configuration, PeerConnectionObserver observer);

        void dispose();
    }

    /** Forwards to a webrtc-java factory. */
    record RtcFactory(PeerConnectionFactory factory) implements Factory {

        @Override
        public NativePeer createPeer(RTCConfiguration configuration, PeerConnectionObserver observer) {
            RTCPeerConnection connection = factory.createPeerConnection(configuration, observer);
            if (connection == null) {
                throw new IllegalStateException("webrtc-java created no peer connection");
            }
            return new RtcPeer(connection);
        }

        @Override
        public void dispose() {
            factory.dispose();
        }
    }

    /** Forwards to a webrtc-java peer connection. */
    record RtcPeer(RTCPeerConnection connection) implements NativePeer {

        @Override
        public void setRemoteDescription(RTCSessionDescription description, SetSessionDescriptionObserver observer) {
            connection.setRemoteDescription(description, observer);
        }

        @Override
        public void createOffer(CreateSessionDescriptionObserver observer) {
            connection.createOffer(new RTCOfferOptions(), observer);
        }

        @Override
        public void createAnswer(CreateSessionDescriptionObserver observer) {
            connection.createAnswer(new RTCAnswerOptions(), observer);
        }

        @Override
        public void setLocalDescription(RTCSessionDescription description, SetSessionDescriptionObserver observer) {
            connection.setLocalDescription(description, observer);
        }

        @Override
        public String localDescription() {
            RTCSessionDescription description = connection.getLocalDescription();
            return description == null ? null : description.sdp;
        }

        @Override
        public void addIceCandidate(RTCIceCandidate candidate, AddIceCandidateObserver observer) {
            connection.addIceCandidate(candidate, observer);
        }

        @Override
        public NativeLink createDataChannel(ChannelParameters parameters) {
            RTCDataChannelInit init = new RTCDataChannelInit();
            init.ordered = parameters.ordered();
            init.negotiated = parameters.negotiated();
            init.protocol = parameters.protocol();
            parameters.maxRetransmits().ifPresent(value -> init.maxRetransmits = value);
            parameters.maxPacketLifeTimeMs().ifPresent(value -> init.maxPacketLifeTime = value);
            return new NativeLink.RtcLink(connection.createDataChannel(parameters.label(), init));
        }

        @Override
        public RTCTransportMetrics transportMetrics() {
            return connection.getTransportMetrics();
        }

        @Override
        public void close() {
            connection.close();
        }
    }
}

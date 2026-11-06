package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.webrtc.AddIceCandidateObserver;
import io.github.sendablemetatype.webrtc.CreateSessionDescriptionObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelSendObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelState;
import io.github.sendablemetatype.webrtc.RTCIceCandidate;
import io.github.sendablemetatype.webrtc.RTCSessionDescription;
import io.github.sendablemetatype.webrtc.RTCTransportMetrics;
import io.github.sendablemetatype.webrtc.SetSessionDescriptionObserver;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/** Fakes of the adapter's native seams, recording every call in one shared log. */
final class FakeNative {
    private FakeNative() {
    }

    /** Runs tasks only when the test says so, to show what runs on the control thread. */
    static final class ManualExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        boolean shutdown;

        @Override
        public synchronized void execute(Runnable task) {
            if (shutdown) {
                throw new RejectedExecutionException("shut down");
            }
            tasks.add(task);
        }

        void runAll() {
            Runnable task;
            while ((task = poll()) != null) {
                task.run();
            }
        }

        synchronized int pending() {
            return tasks.size();
        }

        private synchronized Runnable poll() {
            return tasks.poll();
        }
    }

    enum Step { SET_REMOTE, CREATE_OFFER, CREATE_ANSWER, SET_LOCAL }

    static final class Peer implements NativePeer {
        final List<String> log;
        final List<RTCSessionDescription> remoteDescriptions = new ArrayList<>();
        final List<RTCSessionDescription> localDescriptions = new ArrayList<>();
        final List<RTCIceCandidate> candidates = new ArrayList<>();
        final List<Link> createdLinks = new ArrayList<>();
        SetSessionDescriptionObserver remote;
        CreateSessionDescriptionObserver created;
        SetSessionDescriptionObserver local;
        Step throwAt;
        String localDescription = "v=0\r\na=candidate:1 1 udp 1 192.0.2.1 5000 typ host\r\n";
        boolean throwOnRead;
        RTCTransportMetrics metrics;
        int closes;

        Peer(List<String> log) {
            this.log = log;
        }

        @Override
        public void setRemoteDescription(RTCSessionDescription description, SetSessionDescriptionObserver observer) {
            step(Step.SET_REMOTE);
            remoteDescriptions.add(description);
            remote = observer;
        }

        @Override
        public void createOffer(CreateSessionDescriptionObserver observer) {
            step(Step.CREATE_OFFER);
            created = observer;
        }

        @Override
        public void createAnswer(CreateSessionDescriptionObserver observer) {
            step(Step.CREATE_ANSWER);
            created = observer;
        }

        @Override
        public void setLocalDescription(RTCSessionDescription description, SetSessionDescriptionObserver observer) {
            step(Step.SET_LOCAL);
            localDescriptions.add(description);
            local = observer;
        }

        @Override
        public String localDescription() {
            if (throwOnRead) {
                throw new Error("native read failed");
            }
            return localDescription;
        }

        /** Parses the candidate first, as webrtc-java does; it reports an unparsable one with a plain Error. */
        @Override
        public void addIceCandidate(RTCIceCandidate candidate, AddIceCandidateObserver observer) {
            if (!candidate.sdp.startsWith("candidate:")) {
                throw new Error("Create ICE candidate failed: " + candidate.sdp);
            }
            record("candidate " + candidate.sdpMid + " " + candidate.sdp);
            candidates.add(candidate);
        }

        @Override
        public NativeLink createDataChannel(ChannelParameters parameters) {
            Link link = new Link(parameters, log);
            createdLinks.add(link);
            return link;
        }

        @Override
        public RTCTransportMetrics transportMetrics() {
            if (throwOnRead) {
                throw new Error("native read failed");
            }
            return metrics;
        }

        @Override
        public void close() {
            closes++;
            record("close peer");
        }

        /**
         * Throws as webrtc-java does: a description it cannot parse is a C++
         * exception, which reaches Java as a plain Error; creating a
         * description fails with a runtime exception, such as the one for a
         * disposed connection.
         */
        private void step(Step step) {
            record(step.name());
            if (throwAt == step) {
                switch (step) {
                    case SET_REMOTE, SET_LOCAL -> throw new Error("Create session description failed: native " + step);
                    case CREATE_OFFER, CREATE_ANSWER -> throw new IllegalStateException("native " + step + " failed");
                }
            }
        }

        private void record(String event) {
            synchronized (log) {
                log.add(event);
            }
        }
    }

    static final class Link implements NativeLink {
        final ChannelParameters parameters;
        final List<String> log;
        final List<RTCDataChannelSendObserver> sends = new ArrayList<>();
        volatile RTCDataChannelState state = RTCDataChannelState.CONNECTING;
        volatile LinkError error;
        volatile RTCDataChannelObserver observer;
        // What sendAsync throws: a RuntimeException or a plain Error, as webrtc-java does.
        Throwable sendFailure;

        Link(ChannelParameters parameters, List<String> log) {
            this.parameters = parameters;
            this.log = log;
        }

        @Override
        public ChannelParameters parameters() {
            return parameters;
        }

        @Override
        public RTCDataChannelState state() {
            return state;
        }

        @Override
        public LinkError error() {
            return error;
        }

        @Override
        public void registerObserver(RTCDataChannelObserver observer) {
            this.observer = observer;
        }

        @Override
        public void unregisterObserver() {
            record("unregister " + parameters.label());
            observer = null;
        }

        @Override
        public void sendAsync(ByteBuffer data, RTCDataChannelSendObserver observer) {
            switch (sendFailure) {
                case RuntimeException e -> throw e;
                case Error e -> throw e;
                case null, default -> {
                }
            }
            synchronized (sends) {
                sends.add(observer);
            }
        }

        @Override
        public void close() {
            record("close " + parameters.label());
        }

        @Override
        public void dispose() {
            record("dispose " + parameters.label());
        }

        void changeState(RTCDataChannelState newState) {
            state = newState;
            observer.onStateChange();
        }

        RTCDataChannelSendObserver send(int index) {
            synchronized (sends) {
                return sends.get(index);
            }
        }

        private void record(String event) {
            synchronized (log) {
                log.add(event);
            }
        }
    }

    /** Records every listener event as text in arrival order. */
    static final class Listener implements PeerListener {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final List<DataLink> remoteLinks = Collections.synchronizedList(new ArrayList<>());
        volatile boolean keepRemote = true;

        @Override
        public void onLocalCandidate(String candidate, String mid) {
            events.add("candidate " + mid + " " + candidate);
        }

        @Override
        public void onGatheringComplete() {
            events.add("gathering complete");
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            events.add("remote " + link.parameters().label());
            remoteLinks.add(link);
            return keepRemote;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            events.add("open " + link.parameters().label());
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            events.add("message " + link.parameters().label() + " " + message.remaining());
        }

        @Override
        public void onTextMessage(DataLink link, ByteBuffer message) {
            events.add("text " + link.parameters().label() + " " + message.remaining());
        }

        @Override
        public void onQueueLow(DataLink link) {
            events.add("queue low " + link.parameters().label());
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
            events.add("channel closed " + link.parameters().label() + " " + cause);
        }

        @Override
        public void onPathSelected(TransportPath path) {
            events.add("path " + path);
        }

        @Override
        public void onStateChange(PeerState state) {
            events.add("state " + state);
        }

        @Override
        public void onIceStateChange(IceState state) {
            events.add("ice " + state);
        }

        @Override
        public void onClosed(CloseCause cause) {
            events.add("closed " + cause);
        }

        List<String> snapshot() {
            synchronized (events) {
                return List.copyOf(events);
            }
        }
    }
}

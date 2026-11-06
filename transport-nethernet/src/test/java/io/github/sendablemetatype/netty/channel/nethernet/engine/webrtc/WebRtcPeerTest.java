package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLinkClosedException;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig.Role;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportMetrics;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.FakeNative.ManualExecutor;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.FakeNative.Step;
import io.github.sendablemetatype.webrtc.RTCDataChannelObserver;
import io.github.sendablemetatype.webrtc.RTCDataChannelBuffer;
import io.github.sendablemetatype.webrtc.RTCDataChannelState;
import io.github.sendablemetatype.webrtc.RTCErrorDetailType;
import io.github.sendablemetatype.webrtc.RTCErrorType;
import io.github.sendablemetatype.webrtc.RTCIceConnectionState;
import io.github.sendablemetatype.webrtc.RTCIceGatheringState;
import io.github.sendablemetatype.webrtc.RTCPeerConnectionState;
import io.github.sendablemetatype.webrtc.RTCSctpMetrics;
import io.github.sendablemetatype.webrtc.RTCSdpType;
import io.github.sendablemetatype.webrtc.RTCSessionDescription;
import io.github.sendablemetatype.webrtc.RTCTransportMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebRtcPeerTest {
    private static final String OFFER = "v=0\r\na=identity:eyJhbGciOiJFUzM4NCJ9\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n";
    private static final String ANSWER = "v=0\r\na=ice-ufrag:libw\r\na=ice-pwd:libwebrtcpassword000000\r\n";
    /** The error libwebrtc gives a data channel when a client's quit aborts the association. */
    private static final NativeLink.LinkError USER_INITIATED_ABORT = new NativeLink.LinkError(
            RTCErrorType.OPERATION_ERROR_WITH_DATA, RTCErrorDetailType.SCTP_FAILURE, 12,
            "User-Initiated Abort, reason=Close called");

    private final List<String> nativeLog = new ArrayList<>();
    private final ManualExecutor control = new ManualExecutor();
    private final FakeNative.Peer connection = new FakeNative.Peer(nativeLog);
    private final FakeNative.Listener listener = new FakeNative.Listener();

    @ParameterizedTest
    @CsvSource({"SET_REMOTE,SetRemoteDescription,REMOTE_DESCRIPTION_REJECTED",
            "CREATE_ANSWER,CreateAnswer,NEGOTIATION_FAILED", "SET_LOCAL,SetLocalDescription,NEGOTIATION_FAILED"})
    void anAnswerStepFailingLaterFailsTheStageAndReportsOnce(Step step, String operation, CloseCause cause) {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        CompletableFuture<String> stage = answerUpTo(peer, step).toCompletableFuture();

        switch (step) {
            case SET_REMOTE -> connection.remote.onFailure("[INVALID_PARAMETER] bad offer");
            case CREATE_ANSWER -> connection.created.onFailure("[INTERNAL_ERROR] no answer");
            default -> connection.local.onFailure("[INVALID_STATE] bad answer");
        }
        int operations = nativeLog.size();
        deliverLateSuccesses();
        control.runAll();

        assertFailedWith(stage, operation + " failed");
        assertEquals(List.of("closed " + cause), listener.snapshot());
        assertEquals(operations, nativeLog.size());
    }

    @ParameterizedTest
    @CsvSource({"SET_REMOTE,SetRemoteDescription,REMOTE_DESCRIPTION_REJECTED",
            "CREATE_ANSWER,CreateAnswer,NEGOTIATION_FAILED", "SET_LOCAL,SetLocalDescription,NEGOTIATION_FAILED"})
    void anAnswerStepThrowingFailsTheSameWay(Step step, String operation, CloseCause cause) {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        connection.throwAt = step;

        CompletableFuture<String> stage = answerUpTo(peer, step).toCompletableFuture();

        assertFailedWith(stage, operation + " failed");
        assertEquals(List.of("closed " + cause), listener.snapshot());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void offerAndAnswerStepsOfTheOffererFailAsNegotiation(boolean answerStep) {
        WebRtcPeer peer = peer(Role.OFFERER, null);
        CompletableFuture<String> offer = peer.offer().toCompletableFuture();
        control.runAll();
        if (!answerStep) {
            connection.created.onFailure("[INTERNAL_ERROR] no offer");
            assertFailedWith(offer, "CreateOffer failed");
        } else {
            connection.created.onSuccess(new RTCSessionDescription(RTCSdpType.OFFER, OFFER));
            control.runAll();
            connection.local.onSuccess();
            CompletableFuture<Void> applied = peer.applyAnswer(ANSWER).toCompletableFuture();
            control.runAll();
            connection.remote.onFailure("[INVALID_PARAMETER] bad answer");
            assertFailedWith(applied, "SetRemoteDescription failed");
        }
        // The answer is the remote side's input; the offer is ours.
        assertEquals(List.of("closed " + (answerStep ? CloseCause.REMOTE_DESCRIPTION_REJECTED
                : CloseCause.NEGOTIATION_FAILED)), listener.snapshot());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aPeerThatCannotBeCreatedFailsItsFirstStage(boolean nativesMissing) {
        WebRtcPeer peer = new WebRtcPeer(Role.ANSWERER, listener, control, null);
        peer.start(observer -> {
            if (nativesMissing) {
                throw new UnsatisfiedLinkError("no natives");
            }
            // webrtc-java's report of a factory that could not start.
            throw new Error("Start network thread failed");
        });
        control.runAll();

        CompletableFuture<String> stage = peer.answer(OFFER).toCompletableFuture();
        control.runAll();

        assertFailedWith(stage, "CreatePeerConnection failed");
        assertEquals(List.of("closed NEGOTIATION_FAILED"), listener.snapshot());
        assertTrue(nativeLog.isEmpty());
    }

    @Test
    void theOfferLosesItsIdentityAndTheAnswerGetsTheChosenCredentials() {
        IceCredentials credentials = new IceCredentials("held1234", "chosenpassword00000000000");
        WebRtcPeer peer = peer(Role.ANSWERER, credentials);

        CompletableFuture<String> stage = answerUpTo(peer, Step.SET_LOCAL).toCompletableFuture();
        connection.local.onSuccess();

        assertEquals("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n", connection.remoteDescriptions.getFirst().sdp);
        String expected = "v=0\r\na=ice-ufrag:held1234\r\na=ice-pwd:chosenpassword00000000000\r\n";
        assertEquals(expected, connection.localDescriptions.getFirst().sdp);
        assertEquals(expected, stage.join());
    }

    @Test
    void remoteCandidatesWaitForTheRemoteDescriptionAndKeepTheirOrderAndMid() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        peer.addRemoteCandidate("a=candidate:1 1 udp 1 192.0.2.1 5000 typ host\r\n", "data");
        peer.addRemoteCandidate("candidate:2 1 udp 1 192.0.2.2 5000 typ host", "data");
        control.runAll();
        assertTrue(nativeLog.isEmpty());

        peer.answer(OFFER);
        control.runAll();
        peer.addRemoteCandidate("candidate:3 1 udp 1 192.0.2.3 5000 typ host", "0");
        control.runAll();

        assertEquals(List.of("SET_REMOTE",
                "candidate data candidate:1 1 udp 1 192.0.2.1 5000 typ host",
                "candidate data candidate:2 1 udp 1 192.0.2.2 5000 typ host",
                "candidate 0 candidate:3 1 udp 1 192.0.2.3 5000 typ host"), nativeLog);
    }

    @Test
    void anUnparsableRemoteCandidateCostsOnlyItself() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        peer.addRemoteCandidate("candidate:1 1 udp 1 192.0.2.1 5000 typ host", "0");
        peer.addRemoteCandidate("no candidate", "0");
        peer.addRemoteCandidate("candidate:2 1 udp 1 192.0.2.2 5000 typ host", "0");

        peer.answer(OFFER);
        control.runAll();
        peer.addRemoteCandidate("still no candidate", "0");
        control.runAll();

        assertEquals(List.of("SET_REMOTE",
                "candidate 0 candidate:1 1 udp 1 192.0.2.1 5000 typ host",
                "candidate 0 candidate:2 1 udp 1 192.0.2.2 5000 typ host"), nativeLog);
        assertTrue(listener.snapshot().isEmpty());
    }

    @Test
    void localEventsWaitUntilTheAnswerWasReturned() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        CompletionStage<String> stage = answerUpTo(peer, Step.SET_LOCAL);
        stage.thenAccept(answer -> listener.events.add("answer returned"));

        peer.onLocalCandidate("candidate:1 1 udp 1 192.0.2.1 5000 typ host", "0");
        peer.onGatheringState(RTCIceGatheringState.COMPLETE);
        assertTrue(listener.snapshot().isEmpty());

        connection.local.onSuccess();
        peer.onLocalCandidate("candidate:2 1 udp 1 192.0.2.2 5000 typ host", "0");

        assertEquals(List.of("answer returned",
                "candidate 0 candidate:1 1 udp 1 192.0.2.1 5000 typ host",
                "gathering complete",
                "candidate 0 candidate:2 1 udp 1 192.0.2.2 5000 typ host"), listener.snapshot());
    }

    @Test
    void theGatheredDescriptionIsReadAfterGatheringAndEndsTheCandidates() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        answerUpTo(peer, Step.SET_LOCAL);
        connection.local.onSuccess();
        CompletableFuture<String> gathered = peer.gatheredLocalDescription().toCompletableFuture();

        peer.onGatheringState(RTCIceGatheringState.COMPLETE);
        assertFalse(gathered.isDone());
        control.runAll();

        assertEquals(connection.localDescription + "a=end-of-candidates\r\n", gathered.join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anUnreadableGatheredDescriptionFailsTheNegotiation(boolean throwsOnRead) {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        answerUpTo(peer, Step.SET_LOCAL);
        connection.local.onSuccess();
        connection.localDescription = null;
        connection.throwOnRead = throwsOnRead;

        peer.onGatheringState(RTCIceGatheringState.COMPLETE);
        control.runAll();

        assertFailedWith(peer.gatheredLocalDescription().toCompletableFuture(), "GetLocalDescription failed");
        assertEquals(List.of("gathering complete", "closed NEGOTIATION_FAILED"), listener.snapshot());
    }

    @Test
    void closeTearsDownOnTheControlThreadAndReleasesChannelsAfterTheirSends() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        remoteLink(peer, ChannelParameters.unreliable("UnreliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        AtomicReference<Throwable> sendResult = new AtomicReference<>();
        link.send(ByteBuffer.allocate(10), sendResult::set);
        RTCDataChannelObserver late = reliable.observer;
        CompletableFuture<String> pending = peer.gatheredLocalDescription().toCompletableFuture();
        List<String> before = listener.snapshot();

        CompletionStage<Void> closed = peer.close();
        assertSame(closed, peer.close());
        assertTrue(nativeLog.isEmpty(), "Teardown ran inside the caller");

        control.runAll();
        assertEquals(List.of("unregister ReliableDataChannel", "close ReliableDataChannel",
                "unregister UnreliableDataChannel", "close UnreliableDataChannel", "close peer"), nativeLog);
        assertFalse(closed.toCompletableFuture().isDone());
        assertFailedWith(pending, "Peer closed");

        // Events libwebrtc had queued before the close reach nobody.
        late.onMessage(new RTCDataChannelBuffer(ByteBuffer.allocate(3), true));
        peer.onConnectionState(RTCPeerConnectionState.CLOSED);
        peer.onLocalCandidate("candidate:9 1 udp 1 192.0.2.9 5000 typ host", "0");

        reliable.send(0).onFailure("[INVALID_STATE] Send operation was discarded before completion");
        assertInstanceOf(IOException.class, sendResult.get());
        assertFalse(closed.toCompletableFuture().isDone());

        control.runAll();
        assertEquals(List.of("dispose ReliableDataChannel", "dispose UnreliableDataChannel"),
                nativeLog.subList(5, nativeLog.size()));
        assertTrue(closed.toCompletableFuture().isDone());
        assertEquals(before, listener.snapshot());
        assertEquals(1, connection.closes);
    }

    @Test
    void teardownFailsASendWithoutResultOnceAndDropsTheLateResult() throws InterruptedException {
        WebRtcPeer peer = new WebRtcPeer(Role.ANSWERER, listener, control, null, Duration.ZERO);
        peer.start(observer -> connection);
        control.runAll();
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        List<Throwable> results = Collections.synchronizedList(new ArrayList<>());
        listener.remoteLinks.getFirst().send(ByteBuffer.allocate(10), result -> {
            results.add(result);
            nativeLog.add("send settled");
        });

        CompletableFuture<Void> closed = peer.close().toCompletableFuture();
        // The timeout fires on another thread and hands the rest back to the control thread.
        runControlUntil(closed::isDone);

        assertEquals(List.of("send settled", "dispose ReliableDataChannel"), nativeLog.subList(3, nativeLog.size()));
        assertEquals(1, results.size());
        assertInstanceOf(IOException.class, results.getFirst());

        reliable.send(0).onSuccess();
        assertEquals(1, results.size());
    }

    @Test
    void sendsAfterCloseFailAtOnce() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        peer.close();
        control.runAll();

        AtomicReference<Throwable> result = new AtomicReference<>();
        link.send(ByteBuffer.allocate(4), result::set);

        assertInstanceOf(IllegalStateException.class, result.get());
        assertEquals(0, link.queuedBytes());
    }

    @Test
    void queuedBytesFollowSendsAndReportedDecreases() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        link.lowWatermark(100);
        List<Throwable> results = new ArrayList<>();

        link.send(ByteBuffer.allocate(300), results::add);
        assertEquals(300, link.queuedBytes());
        reliable.send(0).onSuccess();
        reliable.observer.onBufferedAmountChange(150);
        reliable.observer.onBufferedAmountChange(100);
        reliable.observer.onBufferedAmountChange(50);

        assertEquals(0, link.queuedBytes());
        assertEquals(1, results.size());
        assertNull(results.getFirst());
        assertEquals(1, listener.snapshot().stream().filter(event -> event.startsWith("queue low")).count());
    }

    @Test
    void onlySendsLibwebrtcNeverCountedLeaveTheQueueWithTheirFailure() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        List<Throwable> results = new ArrayList<>();

        // libwebrtc tried this one, so it reports the bytes with its next decrease.
        link.send(ByteBuffer.allocate(200), results::add);
        reliable.send(0).onFailure("[RESOURCE_EXHAUSTED] queue full");
        assertEquals(200, link.queuedBytes());
        reliable.observer.onBufferedAmountChange(200);
        assertEquals(0, link.queuedBytes());

        reliable.error = USER_INITIATED_ABORT;
        reliable.changeState(RTCDataChannelState.CLOSED);
        link.send(ByteBuffer.allocate(50), results::add);
        reliable.send(1).onFailure(WebRtcLink.DISCARDED);
        reliable.sendFailure = new IllegalStateException("RTCDataChannel is closed");
        link.send(ByteBuffer.allocate(70), results::add);
        reliable.sendFailure = new Error("std::bad_alloc");
        link.send(ByteBuffer.allocate(30), results::add);

        assertEquals(0, link.queuedBytes());
        assertEquals(List.of(IOException.class, DataLinkClosedException.class, IllegalStateException.class,
                Error.class), results.stream().map(Object::getClass).toList());
    }

    /**
     * A client quits with a send in flight, and webrtc-java reports the dropped send before the
     * close: the natives that make libwebrtc post data channel events to the signaling thread do
     * that. The send waits, so the listener hears of the close first and both carry one cause.
     */
    @Test
    void aSendRefusedBeforeTheCloseFailsAfterItWithTheClosesCause() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        link.send(ByteBuffer.allocate(40), result -> listener.events.add("send failed " + closeCauseOf(result)));

        reliable.send(0).onFailure(WebRtcLink.DISCARDED);
        assertEquals(List.of(), listener.snapshot());
        assertEquals(40, link.queuedBytes(), "counted until it fails");
        reliable.error = USER_INITIATED_ABORT;
        reliable.changeState(RTCDataChannelState.CLOSING);
        reliable.changeState(RTCDataChannelState.CLOSED);

        assertEquals(List.of("channel closed ReliableDataChannel REMOTE_ABORTED", "send failed REMOTE_ABORTED"),
                listener.snapshot());
        assertEquals(0, link.queuedBytes());
    }

    /** The order libwebrtc keeps when it calls the observer on its network thread: the close comes first. */
    @Test
    void aSendRefusedAfterTheCloseFailsAtOnceWithTheClosesCause() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        link.send(ByteBuffer.allocate(40), result -> listener.events.add("send failed " + closeCauseOf(result)));

        reliable.error = USER_INITIATED_ABORT;
        reliable.changeState(RTCDataChannelState.CLOSED);
        reliable.send(0).onFailure(WebRtcLink.DISCARDED);

        assertEquals(List.of("channel closed ReliableDataChannel REMOTE_ABORTED", "send failed REMOTE_ABORTED"),
                listener.snapshot());
    }

    /**
     * While the remote peer closes a channel in an orderly way, libwebrtc refuses sends with a bare
     * INVALID_STATE and keeps that as the channel's error when the close completes.
     */
    @Test
    void aSendRefusedWhileTheRemoteClosesTheChannelFailsAsClosedByTheRemote() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        link.send(ByteBuffer.allocate(40), result -> listener.events.add("send failed " + closeCauseOf(result)));

        reliable.changeState(RTCDataChannelState.CLOSING);
        reliable.error = new NativeLink.LinkError(RTCErrorType.INVALID_STATE, RTCErrorDetailType.NONE, null, "");
        reliable.send(0).onFailure("[INVALID_STATE] ");
        assertEquals(List.of(), listener.snapshot());
        reliable.changeState(RTCDataChannelState.CLOSED);

        assertEquals(List.of("channel closed ReliableDataChannel REMOTE_CLOSED", "send failed REMOTE_CLOSED"),
                listener.snapshot());
        assertEquals(40, link.queuedBytes(), "libwebrtc tried it, so its next decrease reports it");
    }

    @Test
    void otherRefusalsFailAtOnceAsTheyAre() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        List<Throwable> results = new ArrayList<>();
        link.send(ByteBuffer.allocate(4), results::add);

        reliable.send(0).onFailure("[NETWORK_ERROR] Failure to send data");

        assertEquals(1, results.size());
        assertEquals(IOException.class, results.getFirst().getClass());
    }

    /** A refusal waiting for a close that teardown silences fails then, and teardown does not wait for it. */
    @Test
    void aRefusalWaitingForTheCloseFailsAtTeardown() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link reliable = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        DataLink link = listener.remoteLinks.getFirst();
        List<Throwable> results = new ArrayList<>();
        link.send(ByteBuffer.allocate(4), results::add);
        reliable.send(0).onFailure(WebRtcLink.DISCARDED);

        CompletableFuture<Void> closed = peer.close().toCompletableFuture();
        control.runAll();

        assertEquals(1, results.size());
        assertEquals(IOException.class, results.getFirst().getClass());
        assertEquals(0, link.queuedBytes());
        assertTrue(closed.isDone(), "teardown waited for the refusal");
    }

    @Test
    void aRefusedRemoteChannelIsClosedAndReleasedOffTheCallback() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        listener.keepRemote = false;
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("unexpected"), nativeLog);

        peer.onRemoteChannel(channel);
        assertTrue(nativeLog.isEmpty());
        control.runAll();

        assertEquals(List.of("close unexpected", "dispose unexpected"), nativeLog);
        assertNull(channel.observer);
    }

    @Test
    void aRemoteChannelArrivingAfterCloseIsReleasedUnannounced() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        peer.close();
        control.runAll();
        nativeLog.clear();
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), nativeLog);

        peer.onRemoteChannel(channel);
        control.runAll();

        assertEquals(List.of("close ReliableDataChannel", "dispose ReliableDataChannel"), nativeLog);
        assertTrue(listener.snapshot().isEmpty());
    }

    @Test
    void aPeerClosedBeforeItsConnectionExistsNeverCreatesOne() {
        WebRtcPeer peer = new WebRtcPeer(Role.ANSWERER, listener, control, null);
        List<String> created = new ArrayList<>();
        peer.start(observer -> {
            created.add("created");
            return connection;
        });

        CompletableFuture<Void> closed = peer.close().toCompletableFuture();
        control.runAll();

        assertTrue(created.isEmpty());
        assertTrue(closed.isDone());
        assertTrue(nativeLog.isEmpty());
    }

    @Test
    void aKeptRemoteChannelIsAnnouncedBeforeItOpensAndCarriesMessages() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), nativeLog);
        channel.state = RTCDataChannelState.OPEN;

        peer.onRemoteChannel(channel);
        channel.observer.onMessage(new RTCDataChannelBuffer(ByteBuffer.allocate(7), true));
        channel.changeState(RTCDataChannelState.OPEN);

        assertEquals(List.of("remote ReliableDataChannel", "open ReliableDataChannel", "message ReliableDataChannel 7"),
                listener.snapshot());
    }

    @Test
    void dataChannelObserversAskForTheNetworkThread() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), nativeLog);

        peer.onRemoteChannel(channel);

        assertTrue(channel.observer.isOkToCallOnTheNetworkThread());
    }

    /**
     * A remote channel can open before its observer is registered, which fires no event, and the
     * messages that arrive meanwhile are delivered by the registration itself. The listener hears
     * of the open first.
     */
    @Test
    void aRemoteChannelOpenedBeforeItsObserverReportsTheOpenBeforeItsQueuedMessages() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), nativeLog);
        channel.onRegister = link -> {
            link.state = RTCDataChannelState.OPEN;
            link.receive(new RTCDataChannelBuffer(ByteBuffer.allocate(7), true));
        };

        peer.onRemoteChannel(channel);

        assertEquals(List.of("remote ReliableDataChannel", "open ReliableDataChannel", "message ReliableDataChannel 7"),
                listener.snapshot());
    }

    /**
     * The attaching thread's look at the state can be older than the close the
     * network thread reports meanwhile; the listener must not hear of an open
     * after the close.
     */
    @Test
    void aRemoteChannelClosedWhileItIsAttachedIsNotReportedOpenAfterwards() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), nativeLog);
        // Opens before the observer is registered, which fires no event.
        channel.onRegister = link -> link.state = RTCDataChannelState.OPEN;
        int[] reads = {0};
        channel.onStateRead = link -> {
            // Closes during the look after registration, which reads OPEN.
            if (++reads[0] == 1) {
                link.changeState(RTCDataChannelState.CLOSED);
            }
        };

        peer.onRemoteChannel(channel);

        assertEquals(List.of("remote ReliableDataChannel", "channel closed ReliableDataChannel REMOTE_CLOSED"),
                listener.snapshot());
    }

    @Test
    void aTextMessageIsReportedAsText() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link channel = new FakeNative.Link(ChannelParameters.reliable("ReliableDataChannel"), nativeLog);
        channel.state = RTCDataChannelState.OPEN;
        peer.onRemoteChannel(channel);

        channel.observer.onMessage(new RTCDataChannelBuffer(ByteBuffer.allocate(5), false));
        channel.observer.onMessage(new RTCDataChannelBuffer(ByteBuffer.allocate(7), true));

        assertEquals(List.of("remote ReliableDataChannel", "open ReliableDataChannel", "text ReliableDataChannel 5",
                "message ReliableDataChannel 7"), listener.snapshot());
    }

    /** A row's message of {@code ''} is an empty one, as libwebrtc gives a bare error. */
    @ParameterizedTest
    @CsvSource({
            ",,,,false,REMOTE_CLOSED",
            "OPERATION_ERROR_WITH_DATA,SCTP_FAILURE,12,User-Initiated Abort,false,REMOTE_ABORTED",
            "OPERATION_ERROR_WITH_DATA,SCTP_FAILURE,13,Protocol Violation,false,TRANSPORT_FAILED",
            "OPERATION_ERROR_WITH_DATA,DTLS_FAILURE,,closed,false,TRANSPORT_FAILED",
            "NETWORK_ERROR,NONE,,Failure to send data,true,ICE_FAILED",
            // A send refused while the remote peer closed it in an orderly way.
            "INVALID_STATE,NONE,,'',false,REMOTE_CLOSED",
            // The open handshake message could not be sent.
            "INVALID_STATE,NONE,,Failed to send a CONTROL message,false,TRANSPORT_FAILED",
    })
    void aClosedChannelGetsItsCauseFromTheChannelError(RTCErrorType type, RTCErrorDetailType detail, Integer code,
                                                        String message, boolean iceFailed, CloseCause expected) {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        FakeNative.Link channel = remoteLink(peer, ChannelParameters.reliable("ReliableDataChannel"));
        if (iceFailed) {
            peer.onIceConnectionState(RTCIceConnectionState.FAILED);
        }
        channel.error = type == null ? null : new NativeLink.LinkError(type, detail, code, message);

        channel.changeState(RTCDataChannelState.CLOSING);
        channel.changeState(RTCDataChannelState.CLOSED);
        channel.changeState(RTCDataChannelState.CLOSED);

        assertEquals(List.of("channel closed ReliableDataChannel " + expected), listener.snapshot().stream()
                .filter(event -> event.startsWith("channel closed")).toList());
    }

    @ParameterizedTest
    @CsvSource({"FAILED,ICE_FAILED", "CONNECTED,TRANSPORT_FAILED"})
    void aFailedConnectionReportsWhetherIceFailedOnce(RTCIceConnectionState ice, CloseCause expected) {
        WebRtcPeer peer = peer(Role.ANSWERER, null);

        peer.onIceConnectionState(ice);
        peer.onConnectionState(RTCPeerConnectionState.FAILED);
        peer.onConnectionState(RTCPeerConnectionState.FAILED);

        assertEquals(List.of("ice " + ice, "state FAILED", "closed " + expected, "state FAILED"), listener.snapshot());
    }

    @Test
    void metricsAreReadOnTheControlThread() {
        WebRtcPeer peer = peer(Role.ANSWERER, null);
        connection.metrics = new RTCTransportMetrics(Duration.ofMillis(12), new RTCSctpMetrics(Duration.ofMillis(40),
                4800, 1, 131072, 10, 9, 2, 300, 11, 8));

        CompletableFuture<TransportMetrics> metrics = peer.metrics().toCompletableFuture();
        assertFalse(metrics.isDone());
        control.runAll();

        TransportMetrics read = metrics.join();
        assertEquals(Optional.of(Duration.ofMillis(12)), read.iceRoundTripTime());
        assertEquals(new TransportMetrics.Sctp(Duration.ofMillis(40), 4800, 1, 131072, 10, 9, 2, 300, 11, 8),
                read.sctp().orElseThrow());

        connection.metrics = null;
        CompletableFuture<TransportMetrics> none = peer.metrics().toCompletableFuture();
        control.runAll();
        assertEquals(TransportMetrics.NONE, none.join());

        connection.throwOnRead = true;
        CompletableFuture<TransportMetrics> unreadable = peer.metrics().toCompletableFuture();
        control.runAll();
        assertEquals(TransportMetrics.NONE, unreadable.join());

        peer.close();
        assertEquals(TransportMetrics.NONE, peer.metrics().toCompletableFuture().join());
    }

    @Test
    void negotiationsOnAClosedPeerFailAtOnce() {
        WebRtcPeer offerer = peer(Role.OFFERER, null);
        offerer.close();
        control.runAll();

        CompletableFuture<String> offer = offerer.offer().toCompletableFuture();
        CompletableFuture<Void> applied = offerer.applyAnswer(ANSWER).toCompletableFuture();

        assertFailedWith(offer, "Peer closed");
        assertFailedWith(applied, "Peer closed");
        assertEquals(0, control.pending());
    }

    @Test
    void negotiationsFailWhenTheEngineStoppedTheControlThread() {
        WebRtcPeer answerer = peer(Role.ANSWERER, null);
        control.shutdown = true;

        assertFailedWith(answerer.answer(OFFER).toCompletableFuture(), "The WebRTC engine is closed");
        answerer.close();
        assertFailedWith(answerer.gatheredLocalDescription().toCompletableFuture(), "Peer closed");
    }

    @Test
    void rolesAndOrderAreEnforced() {
        WebRtcPeer answerer = peer(Role.ANSWERER, null);
        assertThrows(IllegalStateException.class, answerer::offer);
        assertThrows(IllegalStateException.class, () -> answerer.openChannel(ChannelParameters.reliable("x")));
        answerer.answer(OFFER);
        assertThrows(IllegalStateException.class, () -> answerer.answer(OFFER));

        WebRtcPeer offerer = new WebRtcPeer(Role.OFFERER, listener, control, null);
        assertThrows(IllegalStateException.class, () -> offerer.applyAnswer(ANSWER));
        offerer.offer();
        assertThrows(IllegalStateException.class, () -> offerer.openChannel(ChannelParameters.reliable("x")));
    }

    private WebRtcPeer peer(Role role, IceCredentials credentials) {
        WebRtcPeer peer = new WebRtcPeer(role, listener, control, credentials);
        peer.start(observer -> connection);
        control.runAll();
        return peer;
    }

    /** Runs the answer up to and including the given step's native call. */
    private CompletionStage<String> answerUpTo(WebRtcPeer peer, Step step) {
        CompletionStage<String> stage = peer.answer(OFFER);
        control.runAll();
        if (step == Step.SET_REMOTE || connection.throwAt == Step.SET_REMOTE) {
            return stage;
        }
        connection.remote.onSuccess();
        control.runAll();
        if (step == Step.CREATE_ANSWER || connection.throwAt == Step.CREATE_ANSWER) {
            return stage;
        }
        connection.created.onSuccess(new RTCSessionDescription(RTCSdpType.ANSWER, ANSWER));
        control.runAll();
        return stage;
    }

    private void deliverLateSuccesses() {
        if (connection.remote != null) {
            connection.remote.onSuccess();
        }
        if (connection.created != null) {
            connection.created.onSuccess(new RTCSessionDescription(RTCSdpType.ANSWER, ANSWER));
        }
        if (connection.local != null) {
            connection.local.onSuccess();
        }
    }

    private void runControlUntil(BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        control.runAll();
        while (!done.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "The control thread never got the work it waited for");
            Thread.sleep(1);
            control.runAll();
        }
    }

    private FakeNative.Link remoteLink(WebRtcPeer peer, ChannelParameters parameters) {
        FakeNative.Link channel = new FakeNative.Link(parameters, nativeLog);
        peer.onRemoteChannel(channel);
        listener.events.clear();
        return channel;
    }

    private static CloseCause closeCauseOf(Throwable result) {
        return assertInstanceOf(DataLinkClosedException.class, result).closeCause();
    }

    private static void assertFailedWith(CompletableFuture<?> stage, String messageStart) {
        assertTrue(stage.isCompletedExceptionally(), "The stage did not fail");
        CompletionException failure = assertThrows(CompletionException.class, stage::join);
        assertTrue(failure.getCause().getMessage().startsWith(messageStart), failure.getCause().getMessage());
    }
}

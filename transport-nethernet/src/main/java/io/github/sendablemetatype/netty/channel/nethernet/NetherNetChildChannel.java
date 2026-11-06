package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.JoinFailedMessage.JoinProgress;
import io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressChangedEvent;
import io.github.sendablemetatype.netty.channel.nethernet.address.RemoteAddressTracker;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.ice.RemoteCandidateIntake;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A NetherNet connection a {@link NetherNetServerChannel} accepted. The server
 * exposes it as soon as it admits the join; the child owns its engine peer
 * from creation, negotiates in its own initializer once registered, and goes
 * active once both data channels the client opened are open, or the reliable
 * one is and the unreliable one closed again.
 *
 * <p>Its remote address is where signaling met the client until it goes
 * active, then the selected ICE pair, with a {@link RemoteAddressChangedEvent}
 * for each later move; see {@link #reportedAddress()}.
 */
public class NetherNetChildChannel extends NetherNetChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetChildChannel.class);

    /** The offer check result, also accessible through channel wrappers that delegate attributes. */
    public static final AttributeKey<OfferCheckResult> OFFER_CHECK =
            AttributeKey.valueOf(NetherNetChildChannel.class, "offerCheck");

    /**
     * The connection ID the client chose for this join, exactly as it wrote it:
     * what ties the child back to its offer and its signaling.
     */
    public static final AttributeKey<String> CONNECTION_ID =
            AttributeKey.valueOf(NetherNetChildChannel.class, "connectionId");

    private final DataChannelSlots slots = new DataChannelSlots();
    private final Negotiation negotiation;
    private final AtomicBoolean activationReported = new AtomicBoolean();
    private volatile boolean reliableOpen;
    private volatile boolean unreliableSettled;

    private final Addressing addressing;
    private final Join.Addressed addressed;
    private boolean addressActivated;

    /**
     * @param tracker the join's address tracker, seeded with where signaling met the client
     * @param intake the join's candidate intake, which knows the client's TURN relays
     * @param relays the shared ICE endpoint when PROXY protocol relays front it, else null
     * @param addressed what the client reached, from the join, or null when its source does not see it
     */
    NetherNetChildChannel(Channel parent, InetSocketAddress local, Negotiation negotiation,
                          RemoteAddressTracker tracker, RemoteCandidateIntake intake, IceEndpoint relays,
                          Join.Addressed addressed) {
        super(parent, tracker.reported().address(), local);
        this.negotiation = Objects.requireNonNull(negotiation, "negotiation");
        this.addressing = new Addressing(tracker, intake, relays);
        this.addressed = addressed;
        this.config = new DefaultNetherChannelConfig(this);
    }

    /**
     * The server side of the join's signaling, which the child's peer events
     * feed. Called on engine threads unless noted.
     */
    interface Negotiation {
        /** The peer gathered a local candidate. */
        void localCandidate(String candidate, String mid);

        /** The join got further than before: ICE or the peer connection came up. */
        default void progressed(JoinProgress progress) {
        }

        /** The child went active. Once, on any thread. */
        default void activated() {
        }

        /** The peer ended before the child went active. The child closes itself after this. */
        void failed(CloseCause cause);
    }

    /** Returns the offer check result, or null when the signaling path ran no check. */
    public OfferCheckResult getOfferCheck() { return attr(OFFER_CHECK).get(); }

    /**
     * Records the offer check and installs the login binding it earns: bound to the proven key,
     * or unbound when no key was proven or no check ran.
     */
    void setOfferCheck(OfferCheckResult result) {
        if (result != null) attr(OFFER_CHECK).set(result);
        TransportIdentityBinding.install(this, result != null
                ? TransportIdentityBinding.forOffer(result) : TransportIdentityBinding.unbound());
    }

    /** Installs the login binding of a join whose source authenticated the client itself, with no offer check. */
    void installBinding(TransportIdentityBinding binding) {
        TransportIdentityBinding.install(this, binding);
    }

    /**
     * How the join arrived: the HTTP connection, a trusted proxy's declaration, the LAN datagram,
     * the NXS admission, or {@link io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource#UNKNOWN}
     * for Xbox joins, which carry no address. Per-address limits count the join under it.
     */
    @Override
    public SourcedAddress signaledAddress() {
        return addressing.tracker.signaled();
    }

    /**
     * What the client reached, as the request of a join over HTTP signaling showed it: its
     * {@code Host}, the local port, whether a trusted proxy sent it, and whether its connection
     * spoke TLS ({@link Join.Addressed#tls()}), which tells a join signaled over HTTPS from one over
     * plain HTTP.
     *
     * @return the join's {@link Join#addressed()}; empty for sources that do not see it (Xbox RPC,
     *         LAN, NXS)
     */
    public Optional<Join.Addressed> addressed() {
        return Optional.ofNullable(addressed);
    }

    /**
     * The remote side of the newest selected pair, whose source says whether it is the client, a
     * TURN relay or a PROXY protocol relay's client; taken in before activation too.
     */
    @Override
    public Optional<SourcedAddress> mediaAddress() {
        return addressing.tracker.media();
    }

    /**
     * What {@link #remoteAddress()} reports, with its source: the signaled address until the child
     * goes active, then the media address (or, with
     * {@link io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption#NETHER_SERVER_PREFER_PROXIED_ADDRESS},
     * a trusted proxy's declaration).
     */
    @Override
    public SourcedAddress reportedAddress() {
        return addressing.tracker.reported();
    }

    /**
     * Creates this child's peer, which the child owns from now on and closes
     * with itself. A child closed meanwhile closes the peer at once.
     */
    Peer openPeer(NetherNetEngine engine, PeerConfig peerConfig) {
        Peer created = engine.createPeer(peerConfig, new Events());
        attachPeer(created);
        return created;
    }

    /** The remote peer said it gave up. */
    void connectErrorReceived() {
        if (!isActive()) {
            connectionFailed(NetherConnectionFailure.CONNECT_ERROR);
        }
        close();
    }

    private final class Events extends PeerEvents {
        @Override
        public void onLocalCandidate(String candidate, String mid) {
            if (!stale()) {
                negotiation.localCandidate(candidate, mid);
            }
        }

        @Override
        public void onGatheringComplete() {
            // The negotiation waits on Peer.gatheredLocalDescription instead.
        }

        @Override
        public void onIceStateChange(IceState state) {
            super.onIceStateChange(state);
            if (!stale() && (state == IceState.CONNECTED || state == IceState.COMPLETED)) {
                negotiation.progressed(JoinProgress.ICE_CONNECTED);
            }
        }

        @Override
        public void onStateChange(PeerState state) {
            super.onStateChange(state);
            if (!stale() && state == PeerState.CONNECTED) {
                negotiation.progressed(JoinProgress.PEER_CONNECTED);
            }
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            if (stale() || !slots.admit(link)) {
                return false;
            }
            if (link == slots.reliable()) {
                if (reliableLink() != link) {
                    bindReliable(link);
                }
            } else {
                bindUnreliable(link);
            }
            activateIfReady();
            return true;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            if (stale()) {
                return;
            }
            if (link == slots.reliable()) {
                reliableOpen = true;
            } else if (link == slots.unreliable()) {
                unreliableSettled = true;
            }
            activateIfReady();
        }

        /** The connection carries on without the unreliable channel, so its close does not hold up activation. */
        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
            super.onChannelClosed(link, cause);
            if (!stale() && link == slots.unreliable()) {
                unreliableSettled = true;
                activateIfReady();
            }
        }

        @Override
        void endedBeforeActivation(CloseCause cause, boolean reliableChannel) {
            connectionFailed(failureOf(cause));
            negotiation.failed(cause);
            close();
        }
    }

    /**
     * Activates once the reliable channel is open and the unreliable one opened or closed. libwebrtc
     * announces a remote channel before it opens it ({@code DataChannelController::OnDataChannelOpenMessage}
     * posts the open after the announcement returns), so a channel announced after the reliable one
     * opened would still be connecting, and libwebrtc refuses what is sent on it then. Vanilla
     * clients open both channels, so this waits at most for that open. Both opens can be reported
     * at once on different threads; the flags are volatile, so at least one of them sees both.
     */
    private void activateIfReady() {
        if (reliableOpen && unreliableSettled) {
            markTransportOpen();
            if (activationReported.compareAndSet(false, true)) {
                negotiation.activated();
            }
        }
    }

    @Override
    protected void fireChannelActiveIfReady() {
        // Right before channelActive, on the loop: consumers read the address when the channel
        // goes active, so the switch to the selected pair fires no event.
        if (!addressActivated && isRegistered() && isActive() && eventLoop().inEventLoop()) {
            addressActivated = true;
            remoteAddress = addressing.tracker.activate().address();
        }
        super.fireChannelActiveIfReady();
    }

    @Override
    void followPath(TransportPath path) {
        addressing.selected(path);
    }

    /** The child's address reporting, fed on the event loop. */
    private final class Addressing {
        private final RemoteAddressTracker tracker;
        private final RemoteCandidateIntake intake;
        private final IceEndpoint relays;
        // The newest pair, so a slow relay lookup for an older one is dropped.
        private TransportPath newest;

        private Addressing(RemoteAddressTracker tracker, RemoteCandidateIntake intake, IceEndpoint relays) {
            this.tracker = tracker;
            this.intake = intake;
            this.relays = relays;
        }

        void selected(TransportPath path) {
            newest = path;
            if (relays == null) {
                apply(path, null);
                return;
            }
            CompletionStage<Optional<InetSocketAddress>> lookup;
            try {
                lookup = relays.relayOf(path.remote());
            } catch (RuntimeException e) {
                lookup = CompletableFuture.failedFuture(e);
            }
            lookup.whenComplete((via, failure) -> {
                if (failure != null) {
                    log.debug("Could not look up the relay of {}: {}", path.remote(), failure.toString());
                }
                InetSocketAddress relay = via == null ? null : via.orElse(null);
                try {
                    eventLoop().execute(() -> {
                        if (newest == path && isOpen()) {
                            apply(path, relay);
                        }
                    });
                } catch (RejectedExecutionException closing) {
                    // Nobody reads the address of a channel whose loop is gone.
                }
            });
        }

        private void apply(TransportPath path, InetSocketAddress relay) {
            SourcedAddress pair = SourcedAddress.selectedPair(path.remote(), path.remoteType(),
                    intake.isPeerRelay(path.remote()), relay);
            Optional<RemoteAddressChangedEvent> change = tracker.selectedPair(pair);
            if (change.isPresent()) {
                remoteAddress = change.get().current().address();
                pipeline().fireUserEventTriggered(change.get());
            }
        }
    }

    @Override
    public EventLoop eventLoop() {
        try {
            return super.eventLoop();
        } catch (IllegalStateException e) {
            // Negotiation can fail before ServerBootstrap registers the child.
            // Its close and close-future listeners still need an executor.
            if (parent() == null) {
                throw e;
            }
            return parent().eventLoop();
        }
    }

    @Override
    protected NetherNetUnsafe newUnsafe() {
        return new NetherNetUnsafe() {
            @Override
            public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
                promise.setFailure(new UnsupportedOperationException("Child channel cannot connect"));
            }
        };
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        throw new UnsupportedOperationException("Child channel cannot be bound");
    }

    @Override
    protected void doClose() throws Exception {
        slots.stopAccepting();
        super.doClose();
    }
}

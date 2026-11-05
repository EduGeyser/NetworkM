package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.DefaultNetherChannelConfig;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherConnectionFailure;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.util.nethernet.ClientIdentity;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PublicKey;
import java.util.Objects;

/**
 * A NetherNet connection a {@link NetherNetServerChannel} accepted. The server
 * exposes it as soon as it accepts the join; the child owns its engine peer
 * from creation, negotiates in its own initializer once registered, and goes
 * active when the client opened both data channels and the reliable one is
 * open.
 */
public class NetherNetChildChannel extends NetherNetChannel {
    /** Validated offer identity, also accessible through channel wrappers that delegate attributes. */
    public static final AttributeKey<ClientIdentity> CLIENT_IDENTITY =
            AttributeKey.valueOf(NetherNetChildChannel.class, "clientIdentity");

    /**
     * The connection ID the client chose for this join, as an unsigned decimal
     * string: what ties the child back to its offer and its signaling.
     */
    public static final AttributeKey<String> CONNECTION_ID =
            AttributeKey.valueOf(NetherNetChildChannel.class, "connectionId");

    private final DataChannelSlots slots = new DataChannelSlots();
    private final Negotiation negotiation;
    private volatile boolean reliableOpen;

    NetherNetChildChannel(Channel parent, InetSocketAddress remote, InetSocketAddress local, Negotiation negotiation) {
        super(parent, remote, local);
        this.negotiation = Objects.requireNonNull(negotiation, "negotiation");
        this.config = new DefaultNetherChannelConfig(this);
    }

    /**
     * The server side of the join's signaling, which the child's peer events
     * feed. Called on engine threads.
     */
    interface Negotiation {
        /** The peer gathered a local candidate. */
        void localCandidate(String candidate, String mid);

        /** Negotiation failed; the remote peer should hear so. */
        void failed(String reason);
    }

    /** Returns the validated offer identity, or null when validation was disabled or unsupported by the signaling path. */
    public ClientIdentity getClientIdentity() { return attr(CLIENT_IDENTITY).get(); }

    /**
     * Checks a Bedrock Login chain key against the identity that opened the given channel,
     * including wrappers that delegate attributes to the accepted child.
     *
     * @return null when the login key matches the verified transport key, otherwise the reason it does not,
     *         including when the channel carries no validated identity
     */
    public static String loginKeyMismatch(Channel channel, PublicKey loginKey) {
        ClientIdentity identity = channel == null ? null : channel.attr(CLIENT_IDENTITY).get();
        if (identity == null) {
            return "the transport carries no validated identity to bind the login chain to";
        }
        return identity.loginKeyMismatch(loginKey);
    }

    void setClientIdentity(ClientIdentity identity) {
        if (identity != null) attr(CLIENT_IDENTITY).set(identity);
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
            // A full ICE answer waits on Peer.gatheredLocalDescription instead.
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
            if (!stale() && link == slots.reliable()) {
                reliableOpen = true;
                activateIfReady();
            }
        }

        @Override
        void endedBeforeActivation(CloseCause cause, boolean reliableChannel) {
            connectionFailed(failureOf(cause));
            if (cause == CloseCause.NEGOTIATION_FAILED) {
                negotiation.failed("Negotiation failed");
            }
            close();
        }
    }

    private void activateIfReady() {
        if (reliableOpen && slots.complete()) {
            markTransportOpen();
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

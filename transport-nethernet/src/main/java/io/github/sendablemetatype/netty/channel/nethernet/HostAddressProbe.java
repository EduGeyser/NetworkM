package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateLine;
import io.github.sendablemetatype.netty.channel.nethernet.ice.CandidateType;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Gathers once on the shared ICE port with a peer of its own and returns the host addresses it gathered there. The
 * engine binds the port's socket on a local address only once a peer gathers on it (webrtc-java's
 * {@code IceUdpEndpoint}), and a peer of a signaled join always does before its client sends a check. A client
 * admitted from its first check alone (NXS stateless admission) sends it to a published address before any peer
 * exists, so the port must already be bound there: this gathering binds it on every address the engine gathers on,
 * and the sockets stay bound until the port closes.
 */
final class HostAddressProbe {
    /** How long gathering may take; host candidates need no server, so this is generous. */
    static final Duration DEADLINE = Duration.ofSeconds(10);

    private HostAddressProbe() {
    }

    /**
     * @return completes with the distinct UDP host addresses gathered on the port, with its port number
     */
    static CompletionStage<List<InetSocketAddress>> gather(NetherNetEngine engine, IceEndpoint endpoint,
                                                           CandidatePolicy policy) {
        Peer probe;
        try {
            probe = engine.createPeer(new PeerConfig(PeerConfig.Role.OFFERER, List.of(), policy, endpoint, null, null),
                    IGNORED);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<List<InetSocketAddress>> result = new CompletableFuture<>();
        try {
            // An offer needs a data section, which the link gives it
            probe.openChannel(ChannelParameters.reliable("probe"));
            probe.offer().thenCompose(offer -> probe.gatheredLocalDescription())
                    .toCompletableFuture()
                    .orTimeout(DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .whenComplete((description, failure) -> {
                        probe.close();
                        if (failure != null) {
                            result.completeExceptionally(failure);
                        } else {
                            result.complete(hostAddresses(description, endpoint.port()));
                        }
                    });
        } catch (RuntimeException e) {
            probe.close();
            result.completeExceptionally(e);
        }
        return result;
    }

    /** The UDP host candidates on the port, in the order the description names them. */
    static List<InetSocketAddress> hostAddresses(String description, int port) {
        Set<InetSocketAddress> addresses = new LinkedHashSet<>();
        for (String line : description.split("\r\n|\n")) {
            if (!line.startsWith("a=candidate:")) {
                continue;
            }
            Optional<CandidateLine> candidate = CandidateLine.parse(line);
            if (candidate.isEmpty() || candidate.get().type() != CandidateType.HOST || !candidate.get().isUdp()
                    || candidate.get().port() != port) {
                continue;
            }
            Optional<InetAddress> address = candidate.get().ipAddress();
            address.ifPresent(found -> addresses.add(new InetSocketAddress(found, port)));
        }
        return List.copyOf(addresses);
    }

    /** The probe carries no traffic, so its events matter to nobody. */
    private static final PeerListener IGNORED = new PeerListener() {
        @Override
        public void onLocalCandidate(String candidate, String mid) {
        }

        @Override
        public void onGatheringComplete() {
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            return false;
        }

        @Override
        public void onChannelOpen(DataLink link) {
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
        }

        @Override
        public void onQueueLow(DataLink link) {
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
        }

        @Override
        public void onPathSelected(TransportPath path) {
        }

        @Override
        public void onStateChange(PeerState state) {
        }

        @Override
        public void onIceStateChange(IceState state) {
        }

        @Override
        public void onClosed(CloseCause cause) {
        }
    };
}

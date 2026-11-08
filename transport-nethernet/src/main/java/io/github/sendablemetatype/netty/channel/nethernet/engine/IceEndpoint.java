package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * One UDP port shared by many peers for their ICE host candidates. A peer
 * joins it through {@link PeerConfig#endpoint()}. A connectivity check from a
 * new address reaches a peer only if it authenticates with that peer's local
 * ICE credentials.
 *
 * <p>Settings take effect shortly after their method returns, in the order of
 * the calls. Queries complete once the endpoint's own thread has answered them.
 */
public interface IceEndpoint {

    /** The UDP port, the same on every local address. */
    int port();

    /**
     * Passes each held check for which no peer exists to the handler. The
     * handler either creates a peer whose local credentials are the check's
     * local ufrag and the password it authenticates with, or rejects it; a
     * check it leaves alone expires. The engine answers the held check as soon
     * as the new peer can take it. Replaces an earlier handler.
     *
     * @param handler receives the checks
     * @param executor runs the handler, off the engine's threads
     */
    void onHeldCheck(Consumer<HeldCheck> handler, Executor executor);

    /** Stops passing on held checks; they are dropped from now on. */
    void clearHeldCheckHandler();

    /**
     * Limits the checks held at once and how long each is held.
     *
     * @param maxPending the most checks held at once, at least 1
     * @param timeout how long a check is held at most
     */
    void heldCheckLimits(int maxPending, Duration timeout);

    /**
     * Passes on a check for which no peer exists only if it authenticates with
     * a password derived from a key: the check's local ufrag starts with a
     * prefix in {@code keys}, and its password is
     * {@code base64(HMAC-SHA256(key, context || ufrag))} cut to 24 bytes before
     * encoding. Other checks are dropped before they reach the handler.
     *
     * @param context the context bytes
     * @param keys the keys by ufrag prefix; copied
     */
    void derivedPasswords(byte[] context, Map<String, byte[]> keys);

    /** Removes the derived passwords; every held check is passed on again. */
    void clearDerivedPasswords();

    /**
     * Takes PROXY protocol version 2 headers from these relays, numeric
     * addresses or CIDR ranges. A relay's header attributes its datagrams to
     * the client it declares; replies go back through the relay once a check
     * from that client authenticated. An empty list removes the relays.
     *
     * @param relays the relays
     * @throws IllegalArgumentException if an entry is neither
     */
    void proxyRelays(List<String> relays);

    /**
     * The relay a client's datagrams come through.
     *
     * @param client the client's address as the relay declared it
     * @return completes with the relay, if the client is behind one
     */
    CompletionStage<Optional<InetSocketAddress>> relayOf(InetSocketAddress client);

    /**
     * Keeps a binding with a STUN server alive from this port, which keeps the
     * port's NAT mapping open and known.
     *
     * @param server the server's resolved address
     * @return completes with the monitor, or exceptionally if the endpoint
     *         cannot send to the server; close the monitor to stop
     */
    CompletionStage<StunMonitor> monitorStun(InetSocketAddress server);

    /**
     * The endpoint's counters since it opened, and its current sizes.
     *
     * @return completes with the counters
     */
    CompletionStage<Stats> stats();

    /**
     * The local addresses the port is open on, and those it could not open
     * on. The engine opens the port on a local address when the first peer
     * gathers there or a STUN monitor needs it, so an address shows up only
     * once something used it.
     *
     * @return completes with a snapshot, or exceptionally once the endpoint
     *         is closed
     */
    CompletionStage<Sockets> sockets();

    /**
     * Closes the endpoint's sockets. Peers still on it stop sending and
     * receiving; close them first. Idempotent.
     *
     * @return completes once the sockets are closed
     */
    CompletionStage<Void> close();

    /** A connectivity check that no peer on the endpoint has taken. */
    interface HeldCheck {

        /** The endpoint address the check arrived at. */
        InetSocketAddress localAddress();

        /** The address the check came from, as attributed by a relay if any. */
        InetSocketAddress remoteAddress();

        /** The local ICE ufrag the check names. */
        String localUfrag();

        /** The remote side's ICE ufrag. */
        String remoteUfrag();

        /** Drops the check. No effect once a peer took it or it expired. */
        void reject();
    }

    /** A STUN binding the endpoint keeps alive. */
    interface StunMonitor extends AutoCloseable {

        /**
         * The binding's state now.
         *
         * @return completes with the state, or exceptionally once the monitor
         *         or its endpoint is closed
         */
        CompletionStage<StunReading> read();

        /** Stops keeping the binding alive. Idempotent. */
        @Override
        void close();
    }

    /**
     * The state of a STUN binding.
     *
     * @param server the STUN server
     * @param local the local address the requests leave from
     * @param mapped the address the server last saw them from, or null
     * @param state the outcome of the latest transaction
     * @param successfulResponses responses with a mapped address
     * @param failedTransactions transactions that failed or timed out
     * @param mappingRevision how often the mapped address changed, the first
     *        answer included
     * @param lastSuccessAge time since the latest success, empty if none
     */
    record StunReading(
            InetSocketAddress server,
            InetSocketAddress local,
            InetSocketAddress mapped,
            State state,
            long successfulResponses,
            long failedTransactions,
            long mappingRevision,
            Optional<Duration> lastSuccessAge) {

        /** The outcome of a binding's latest transaction. */
        public enum State {
            PENDING,
            SUCCEEDED,
            FAILED
        }
    }

    /**
     * The sockets of an endpoint, all on its port. An address is in at most
     * one of the lists. Java's address equality leaves an IPv6 scope out, so
     * the same link-local address may appear once per scope.
     *
     * @param bound the local addresses the port is open on
     * @param failed the local addresses the port could not be opened on, each
     *        with the reason of the latest attempt there; the engine tries
     *        again when the next peer gathers on the address
     */
    record Sockets(List<InetSocketAddress> bound, List<BindFailure> failed) {

        /** No sockets at all. */
        public static final Sockets NONE = new Sockets(List.of(), List.of());

        /** Copies both lists, keeping their order. */
        public Sockets {
            bound = List.copyOf(bound);
            failed = List.copyOf(failed);
        }
    }

    /**
     * A local address the port could not be opened on.
     *
     * @param address the address, with its scope, and the endpoint's port
     * @param reason why the system refused the bind, such as
     *        {@code Address already in use}; it does not name the address
     */
    record BindFailure(InetSocketAddress address, String reason) {
    }

    /**
     * An endpoint's counters and sizes.
     *
     * @param datagramsReceived datagrams received
     * @param bytesReceived bytes received
     * @param datagramsSent datagrams sent
     * @param bytesSent bytes sent
     * @param unroutable datagrams no peer took: from an address with no
     *        route, or for a route another peer still uses
     * @param unauthenticated checks that authenticated for no peer and were
     *        not held
     * @param pendingDropped checks not held because the most allowed were held
     *        or they were too large
     * @param announced held checks passed to the handler
     * @param replayed held checks answered by a peer
     * @param expired held checks that expired
     * @param rejected held checks rejected
     * @param relayDropped datagrams the relay rules dropped on the way in
     * @param relayFiltered datagrams not sent because no relay routes their
     *        destination
     * @param routes current routes
     * @param sessions current ICE sessions on the endpoint
     * @param pending checks held now
     */
    record Stats(
            long datagramsReceived,
            long bytesReceived,
            long datagramsSent,
            long bytesSent,
            long unroutable,
            long unauthenticated,
            long pendingDropped,
            long announced,
            long replayed,
            long expired,
            long rejected,
            long relayDropped,
            long relayFiltered,
            long routes,
            long sessions,
            long pending) {
    }
}

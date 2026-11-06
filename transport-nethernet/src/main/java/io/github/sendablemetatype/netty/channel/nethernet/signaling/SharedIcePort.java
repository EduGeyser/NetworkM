package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * The endpoint's shared ICE port as a {@link JoinSource} may use it: it can take the connectivity checks that no
 * peer holds yet, which NXS stateless admission turns into joins (ruling A-1: a hook on the shared ICE socket), and
 * keep STUN bindings alive from the port. The endpoint keeps the port itself; a source never closes it.
 */
public interface SharedIcePort {

    /** The address and port the endpoint bound the shared port on: the wildcard when it binds every address. */
    InetSocketAddress localAddress();

    /**
     * Takes the checks that arrive at the port for a local ufrag no peer has. A check reaches the handler only once
     * it authenticated with a password derived from a key the claim {@linkplain HeldChecks#admit admits}; until then,
     * and with no keys at all, none does. For each check the handler either delivers a {@link Join} whose
     * {@link Join.Authenticated} credentials are the check's local ufrag and that password, which the new peer then
     * answers, or {@linkplain IceEndpoint.HeldCheck#reject rejects} it; a check it leaves alone expires.
     *
     * @param handler receives the checks on the endpoint's event loop; must not block
     * @return the claim, which ends with its source; closing it hands the checks back
     * @throws IllegalStateException if another source holds the checks, or the endpoint or the source is closed
     */
    HeldChecks claimHeldChecks(Consumer<IceEndpoint.HeldCheck> handler);

    /**
     * Keeps a binding with a STUN server alive from the port, see {@link IceEndpoint#monitorStun}. The source closes
     * the monitors it opens.
     *
     * @param server the server's resolved address
     * @return completes with the monitor, or exceptionally if the port cannot send to the server
     */
    CompletionStage<IceEndpoint.StunMonitor> monitorStun(InetSocketAddress server);

    /** A source's hold on the shared port's held checks. */
    interface HeldChecks extends AutoCloseable {

        /**
         * Passes on only checks whose password derives from one of these keys, as
         * {@link IceEndpoint#derivedPasswords} describes. Replaces the earlier keys; empty keys pass on none. The
         * endpoint copies both, so the caller may wipe them once this returns.
         *
         * @param context the bytes the ufrag is appended to
         * @param keys    the keys by ufrag prefix
         */
        void admit(byte[] context, Map<String, byte[]> keys);

        /** Hands the checks back: none reach the handler from now on. Idempotent. */
        @Override
        void close();
    }
}

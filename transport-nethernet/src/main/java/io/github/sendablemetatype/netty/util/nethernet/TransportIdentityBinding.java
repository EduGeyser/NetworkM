/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.security.MessageDigest;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/**
 * Ties the Bedrock login chain to the identity that opened the transport.
 * <p>
 * On RakNet the encryption handshake does this on its own: the session key comes out of an ECDH
 * against the chain's identity key, so only its holder can read what follows. NetherNet runs over
 * DTLS and skips that handshake, which leaves the chain unbound, and a chain is replayable until
 * something binds it. The offer's identity assertion binds it here, because the peer proved it
 * holds the key the assertion names before the transport was accepted.
 * <p>
 * Real clients sign the Login with the offer's {@code cpk}: the login identity key equals it on
 * retail 1.26.62 and Education 26.33, over HTTP and over Xbox RPC (captured 2026-11-05). So the
 * check covers Education too, although its offer token is self-signed.
 * <p>
 * Every NetherNet child carries a binding in {@link #ATTRIBUTE}, unbound when its offer proved no
 * key, so a consumer tells "NetherNet without a key" from "not NetherNet" by attribute, and
 * wrapper channels that delegate attributes work the same way.
 *
 * @see <a href="https://github.com/Mojang/bedrock-protocol-docs/blob/main/NetherNetOnboardingGuide.md">NetherNet onboarding guide, section 5</a>
 */
public final class TransportIdentityBinding {
    /** The binding of a NetherNet child channel. Absent on every other channel. */
    public static final AttributeKey<TransportIdentityBinding> ATTRIBUTE =
            AttributeKey.valueOf(TransportIdentityBinding.class, "binding");

    private static final TransportIdentityBinding UNBOUND = new TransportIdentityBinding(null);

    private final IdentityKeyVerifier verifier;

    private TransportIdentityBinding(IdentityKeyVerifier verifier) {
        this.verifier = verifier;
    }

    /** What a login decision came to. */
    public enum Result {
        /** The login key is the key the transport proved; the binding is now spent. */
        ACCEPTED,
        /** The login key is another key or none; the binding is now spent. */
        MISMATCH,
        /** The transport is NetherNet but proved no key, so nothing binds the login. */
        UNBOUND,
        /** A login was already decided against this binding, or the admission behind it lapsed. */
        SPENT,
        /** The channel is not a NetherNet transport; its own authentication applies. */
        NOT_NETHERNET
    }

    /** Where the binding on a channel stands, for diagnostics. */
    public enum State {
        /** The channel is not a NetherNet transport. */
        NOT_NETHERNET,
        /** A NetherNet transport whose offer proved no key. */
        NONE,
        /** A key was proven and no login has been decided against it yet. */
        PENDING,
        /** A login was accepted against it. */
        ACCEPTED,
        /** Spent on a refused login, closed, or the admission behind it has lapsed. */
        REJECTED
    }

    /** A binding for a NetherNet transport whose offer proved no key. */
    public static TransportIdentityBinding unbound() {
        return UNBOUND;
    }

    /**
     * A binding decided by a verifier of the admission path's own, such as NXS stateless
     * admission.
     */
    public static TransportIdentityBinding of(IdentityKeyVerifier verifier) {
        return new TransportIdentityBinding(Objects.requireNonNull(verifier, "verifier"));
    }

    /**
     * A one-shot binding to a proven key, compared in canonical form and in constant time, and
     * erased once spent.
     *
     * @throws IllegalArgumentException if the key is not a secp384r1 point
     */
    public static TransportIdentityBinding forKey(PublicKey provenKey) {
        byte[] expected = IdentityPublicKey.canonical(provenKey);
        return of(new IdentityKeyVerifier() {
            @Override
            protected boolean matches(byte[] canonicalKey) {
                return MessageDigest.isEqual(expected, canonicalKey);
            }

            @Override
            protected void release() {
                Arrays.fill(expected, (byte) 0);
            }
        });
    }

    /** The binding an offer check earns: bound to the key whenever the check proved one, attested or not. */
    public static TransportIdentityBinding forOffer(OfferCheckResult result) {
        return result.clientKey() != null ? forKey(result.clientKey()) : UNBOUND;
    }

    /**
     * Installs a binding on a NetherNet child before the child is published. The binding is
     * closed with the channel.
     *
     * @throws IllegalStateException if the channel already carries one
     */
    public static void install(Channel channel, TransportIdentityBinding binding) {
        Objects.requireNonNull(binding, "binding");
        if (channel.attr(ATTRIBUTE).setIfAbsent(binding) != null) {
            if (binding.verifier != null) {
                binding.verifier.close();
            }
            throw new IllegalStateException("Identity binding already installed");
        }
        if (binding.verifier != null) {
            channel.closeFuture().addListener(ignored -> binding.verifier.close());
        }
    }

    /**
     * Decides a login against the transport it arrived on, spending the binding. Call it once per
     * Login with the key the login chain is signed with.
     *
     * @param channel  the channel the login arrived on, or a wrapper that delegates attributes to it
     * @param loginKey the key the login chain is signed with
     * @return the decision to branch on
     */
    public static Result verifyLogin(Channel channel, PublicKey loginKey) {
        TransportIdentityBinding binding = bindingOf(channel);
        if (binding == null) {
            return unboundOrForeign(channel);
        }
        return binding.verifier == null ? Result.UNBOUND : binding.verifier.verify(loginKey);
    }

    /**
     * Spends the binding for a login the application admits through its own trusted forwarding
     * path. Call it only after the application accepted that forwarded identity.
     *
     * @return {@link Result#ACCEPTED}, {@link Result#SPENT}, {@link Result#UNBOUND} or
     *         {@link Result#NOT_NETHERNET}
     */
    public static Result acceptForwardedIdentity(Channel channel) {
        TransportIdentityBinding binding = bindingOf(channel);
        if (binding == null) {
            return unboundOrForeign(channel);
        }
        return binding.verifier == null ? Result.UNBOUND : binding.verifier.acceptForwardedIdentity();
    }

    /** Where the binding on a channel stands, without spending it. */
    public static State state(Channel channel) {
        TransportIdentityBinding binding = bindingOf(channel);
        if (binding == null) {
            return unboundOrForeign(channel) == Result.UNBOUND ? State.NONE : State.NOT_NETHERNET;
        }
        IdentityKeyVerifier verifier = binding.verifier;
        if (verifier == null) {
            return State.NONE;
        }
        // Checked first: a binding whose admission lapsed is still pending but can admit nothing.
        if (verifier.rejected()) {
            return State.REJECTED;
        }
        return verifier.pending() ? State.PENDING : State.ACCEPTED;
    }

    private static TransportIdentityBinding bindingOf(Channel channel) {
        return Objects.requireNonNull(channel, "channel").attr(ATTRIBUTE).get();
    }

    // A NetherNet child without the attribute was created by a path that skipped installing one.
    // Reporting it as "not NetherNet" would let the consumer skip the check, so it counts as unbound.
    private static Result unboundOrForeign(Channel channel) {
        return channel instanceof NetherNetChildChannel ? Result.UNBOUND : Result.NOT_NETHERNET;
    }
}

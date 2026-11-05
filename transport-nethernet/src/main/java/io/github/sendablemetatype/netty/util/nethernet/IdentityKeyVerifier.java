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

import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding.Result;

import java.security.PublicKey;

/**
 * One login decision, owned by a transport channel. Never exposes admission secrets.
 * <p>
 * The built-in verifier compares the login key with the key the offer proved
 * ({@link TransportIdentityBinding#forKey}). Other admission paths, such as NXS stateless
 * admission, subclass this to bind their own material and to say when the admission behind it
 * has lapsed.
 */
public abstract class IdentityKeyVerifier implements AutoCloseable {
    private enum Phase {
        PENDING, ACCEPTED, REJECTED, CLOSED
    }

    private Phase phase = Phase.PENDING;

    /**
     * Spends the binding on one login, whatever the outcome.
     *
     * @param loginKey the key the login chain is signed with
     * @return {@link Result#ACCEPTED} when it is the key the transport admitted,
     *         {@link Result#MISMATCH} when it is another or no valid key, and
     *         {@link Result#SPENT} when a login was already decided or the admission lapsed
     */
    public final synchronized Result verify(PublicKey loginKey) {
        if (phase != Phase.PENDING) {
            return Result.SPENT;
        }
        boolean accepted = false;
        try {
            if (!usable()) {
                return Result.SPENT;
            }
            accepted = matches(IdentityPublicKey.canonical(loginKey));
            return accepted ? Result.ACCEPTED : Result.MISMATCH;
        } catch (RuntimeException invalidKey) {
            return Result.MISMATCH;
        } finally {
            phase = accepted ? Phase.ACCEPTED : Phase.REJECTED;
            release();
        }
    }

    /**
     * Spends the binding without a key of its own, for an application's already authenticated and
     * explicitly trusted forwarding path.
     *
     * @return {@link Result#ACCEPTED}, or {@link Result#SPENT} when a login was already decided or
     *         the admission lapsed
     */
    public final synchronized Result acceptForwardedIdentity() {
        if (phase != Phase.PENDING) {
            return Result.SPENT;
        }
        boolean accepted = usable();
        phase = accepted ? Phase.ACCEPTED : Phase.REJECTED;
        release();
        return accepted ? Result.ACCEPTED : Result.SPENT;
    }

    /** @return whether a login may still be decided against this binding */
    public final synchronized boolean pending() {
        return phase == Phase.PENDING;
    }

    /** @return whether nothing can be admitted against this binding any more */
    public final synchronized boolean rejected() {
        return phase == Phase.REJECTED || phase == Phase.CLOSED || (phase == Phase.PENDING && !usable());
    }

    @Override
    public final synchronized void close() {
        if (phase == Phase.PENDING) {
            phase = Phase.CLOSED;
            release();
        }
    }

    /** @return whether the admission behind this binding is still live */
    protected boolean usable() {
        return true;
    }

    /**
     * @param canonicalKey the login key, canonicalized by {@link IdentityPublicKey}
     * @return whether it is the key the transport admitted
     */
    protected abstract boolean matches(byte[] canonicalKey);

    /** Erases whatever the binding held. Called once, whichever way it is spent. */
    protected abstract void release();
}

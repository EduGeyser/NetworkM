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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.control.AssistedJoin;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * A player join the provider pushed over its authenticated WebSocket. The provider verified the client's identity
 * assertion, so the login must be signed with exactly the key the join names.
 *
 * @param expiresAt When the login deadline passes, epoch milliseconds on this host's clock
 */
public record AssistedAdmission(AssistedJoin join, AssistedJoin.Offer offer, long expiresAt,
                                LoginKeyBinding loginBinding) implements AdmissionContext {
    public AssistedAdmission {
        Objects.requireNonNull(join, "join");
        Objects.requireNonNull(offer, "offer");
        Objects.requireNonNull(loginBinding, "loginBinding");
        if (join.diagnostic()) {
            throw new IllegalArgumentException("A diagnostic join admits no player");
        }
    }

    /**
     * Checks the join's offer and client key and binds the login to that key.
     *
     * @param expiresAt When the join's deadline passes on this host's clock, after the provider's clock offset
     * @param live      Whether the provider authority and deadline behind the join still hold; asked by
     *                  {@link LoginKeyBinding#usable()}, so it must not block
     * @throws IllegalArgumentException If the offer or the client key cannot be served
     */
    public static AssistedAdmission of(AssistedJoin join, long expiresAt, BooleanSupplier live) {
        AssistedJoin.Offer offer = join.parseOffer();
        byte[] cpk = join.canonicalCpk();
        return new AssistedAdmission(join, offer, expiresAt, new KeyBinding(cpk, live));
    }

    @Override
    public TicketId ticketId() {
        return new TicketId(join.id());
    }

    @Override
    public String localUfrag() {
        return join.localUfrag();
    }

    @Override
    public String localPassword() {
        return join.localPassword();
    }

    @Override
    public String remoteUfrag() {
        return offer.ufrag();
    }

    @Override
    public String remoteDescription() {
        return join.offer();
    }

    @Override
    public String networkId() {
        return join.networkId();
    }

    @Override
    public String keyId() {
        return join.keyId();
    }

    @Override
    public String toString() {
        return "AssistedAdmission[ticketId=" + join.id() + "]";
    }

    private static final class KeyBinding implements LoginKeyBinding {
        private final byte[] key;
        private final BooleanSupplier live;
        private final AtomicBoolean released = new AtomicBoolean();

        KeyBinding(byte[] key, BooleanSupplier live) {
            this.key = key;
            this.live = Objects.requireNonNull(live);
        }

        @Override
        public boolean usable() {
            return !released.get() && live.getAsBoolean();
        }

        @Override
        public synchronized boolean matches(byte[] canonicalKey) {
            return !released.get() && MessageDigest.isEqual(key, canonicalKey);
        }

        @Override
        public synchronized void release() {
            if (released.compareAndSet(false, true)) {
                Arrays.fill(key, (byte) 0);
            }
        }
    }
}

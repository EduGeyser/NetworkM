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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The admission key epochs. A delivered key reaches the saved state only after the transport accepted it, so the
 * next heartbeat cannot acknowledge a key that was never installed and a refused key cannot survive a restart.
 * Retirements keep a key until its original deadline; repeated replies can only shorten it. Used on the lifecycle
 * executor.
 */
final class KeyManager {
    static final int MAX_EPOCHS = 8;

    private final ClientRuntime runtime;
    private final Diagnostics diagnostics;
    /** A delivered epoch, kept out of the saved state until the transport has accepted it. */
    private AdmissionKey pending;
    private List<AdmissionKey> installed = List.of();
    private String activeKeyId;

    KeyManager(ClientRuntime runtime, Diagnostics diagnostics) {
        this.runtime = runtime;
        this.diagnostics = diagnostics;
    }

    /** The newest installed epoch, which a published profile names, or null while none is installed. */
    String activeKeyId() {
        return activeKeyId;
    }

    List<AdmissionKey> installed() {
        return installed;
    }

    /** The epochs a heartbeat acknowledges, oldest first. */
    List<String> acknowledged() {
        return runtime.keeper.state().admissionKeys().stream().map(ProviderState.StoredKey::keyId).toList();
    }

    void deliver(AdmissionKey key) {
        pending = key;
    }

    /** Forgets a delivered key the transport refused, so it cannot reach memory or disk. */
    void discardDelivered() {
        pending = null;
    }

    /**
     * Installs the saved epochs that are not past their retirement, plus a delivered one, then saves exactly what the
     * transport accepted. Without any epoch it saves a key request instead, so the next heartbeat asks for one.
     */
    CompletableFuture<Void> install() {
        return Async.attempt(() -> {
            long now = runtime.clock.millis();
            List<ProviderState.StoredKey> retained = new ArrayList<>(runtime.keeper.state().admissionKeys().stream()
                    .filter(key -> key.retireAfter() > now)
                    .toList());
            if (pending != null) {
                retained.add(new ProviderState.StoredKey(pending.keyId(), pending.secret(), Long.MAX_VALUE));
            }
            if (retained.size() > MAX_EPOCHS) {
                throw new IOException("Too many admission key epochs");
            }
            if (retained.isEmpty()) {
                return diagnostics.disable().thenApply(ignored -> {
                    installed = List.of();
                    activeKeyId = null;
                    ProviderState state = runtime.keeper.state().withAdmissionKeys(List.of());
                    if (state.keyRequestId() == null) {
                        state = state.withKeyRequestId(UUID.randomUUID().toString());
                    }
                    save(state);
                    return null;
                });
            }
            // Reading the epochs is what rejects a malformed one, so it happens before anything is written
            List<AdmissionKey> keys = retained.stream().map(ProviderState.StoredKey::admissionKey).toList();
            for (AdmissionKey key : keys) {
                if (!key.keyId().matches("[A-Z0-9]{4}") || !AdmissionKey.validSecret(key.secret())) {
                    throw new IOException("Invalid saved admission key");
                }
            }
            CompletableFuture<Void> withdrawn = keys.equals(installed) ? Async.done() : diagnostics.disable();
            return withdrawn
                    .thenCompose(ignored -> Async.within(runtime.transport.installAdmissionKeys(keys),
                            Duration.ofSeconds(10), runtime.executor, "Admission key installation"))
                    .thenApply(ignored -> {
                        save(runtime.keeper.state().withAdmissionKeys(retained));
                        pending = null;
                        activeKeyId = keys.getLast().keyId();
                        installed = keys;
                        return null;
                    });
        });
    }

    /** Applies retirement deadlines, never extending one, then installs what remains. */
    CompletableFuture<Void> retire(List<Wire.KeyRetirement> retirements) {
        if (retirements.isEmpty()) {
            return Async.done();
        }
        return Async.attempt(() -> {
            List<ProviderState.StoredKey> keys = runtime.keeper.state().admissionKeys().stream().map(key -> {
                long retireAfter = key.retireAfter();
                for (Wire.KeyRetirement retirement : retirements) {
                    if (retirement.keyId().equals(key.keyId())) {
                        retireAfter = Math.min(retireAfter, retirement.retireAfter());
                    }
                }
                return new ProviderState.StoredKey(key.keyId(), key.secret(), retireAfter);
            }).toList();
            runtime.keeper.save(runtime.keeper.state().withAdmissionKeys(keys));
            return install();
        });
    }

    /** Saves a fresh key request, so the next heartbeat provisions a replacement epoch. */
    CompletableFuture<Void> requestReplacement() {
        return Async.attempt(() -> {
            runtime.requireResolved();
            return install().thenApply(ignored -> {
                if (runtime.keeper.state().admissionKeys().size() >= MAX_EPOCHS) {
                    throw new IllegalStateException("Wait for retiring admission epochs before rotating again");
                }
                save(runtime.keeper.state().withKeyRequestId(UUID.randomUUID().toString()));
                return null;
            });
        });
    }

    private void save(ProviderState state) {
        try {
            runtime.keeper.save(state);
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }
}

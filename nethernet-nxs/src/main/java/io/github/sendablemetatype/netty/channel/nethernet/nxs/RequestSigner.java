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

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Signs operational requests with the machine key and keeps their sequence. Each new request reserves the next
 * number, saved before it is sent, so a delayed request can never overwrite newer state; a retry reuses its number,
 * idempotency key and body, and only its timestamp changes. Used on the lifecycle executor only.
 */
final class RequestSigner {
    private final ClientRuntime runtime;
    private String keyFor;
    private PrivateKey key;

    RequestSigner(ClientRuntime runtime) {
        this.runtime = runtime;
    }

    /** The machine key, read from the saved state once per key. */
    PrivateKey machineKey() throws GeneralSecurityException {
        ProviderState.MachineKey saved = runtime.keeper.state().machineKey();
        if (saved == null) {
            throw new IllegalStateException("No machine key");
        }
        if (!saved.privateKey().equals(keyFor)) {
            key = ProviderCrypto.privateKey(saved.privateKey());
            keyFor = saved.privateKey();
        }
        return key;
    }

    /** Reserves the next sequence number of this generation. */
    void reserveSequence() throws IOException {
        ProviderState state = runtime.keeper.state();
        runtime.keeper.save(state.withSequence(state.sequence() + 1));
    }

    /**
     * The signed headers for one attempt of the reserved request.
     *
     * @param body The exact body bytes as text, or null for none, which hashes as empty
     */
    Map<String, String> sign(String method, URI uri, String body, String idempotencyKey) throws GeneralSecurityException {
        String path = uri.getRawPath() + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
        return headers(method, path, body == null ? "" : body, idempotencyKey);
    }

    /**
     * The headers that authenticate a WebSocket upgrade: {@code GET} on the control path with an empty body. They
     * carry the last sequence used and consume none.
     */
    Map<String, String> upgradeHeaders(URI controlEndpoint, boolean assisted) throws GeneralSecurityException {
        Map<String, String> headers = headers("GET", controlEndpoint.getRawPath(), "", UUID.randomUUID().toString());
        if (assisted) {
            headers.put("nxs-assisted", "1");
        }
        return headers;
    }

    private Map<String, String> headers(String method, String path, String body, String idempotencyKey)
            throws GeneralSecurityException {
        ProviderState state = runtime.keeper.state();
        Wire.SavedRegistration registration = runtime.registration();
        long now = runtime.clock.millis();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("nxs-instance-id", registration.instanceId());
        headers.put("nxs-key-id", registration.keyId());
        headers.put("nxs-timestamp", Long.toString(now));
        headers.put("nxs-signature-version", ProviderCrypto.SIGNATURE);
        headers.put("nxs-generation", Long.toString(state.generation()));
        headers.put("nxs-sequence", Long.toString(state.sequence()));
        headers.put("idempotency-key", idempotencyKey);
        headers.put("nxs-signature", ProviderCrypto.sign(machineKey(), ProviderCrypto.request(runtime.origin, method,
                path, now, registration.instanceId(), registration.keyId(), idempotencyKey, state.generation(),
                state.sequence(), body)));
        return headers;
    }
}

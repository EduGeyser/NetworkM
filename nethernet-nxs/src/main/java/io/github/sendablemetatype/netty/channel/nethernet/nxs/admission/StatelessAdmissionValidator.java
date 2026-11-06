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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * NXS1 token validation using locally installed keys and the incoming ICE username. Holds no per-join state:
 * everything a join needs comes out of its token.
 */
public final class StatelessAdmissionValidator {
    /** The secret of one installed epoch, kept until no admitted login refers to it. */
    private static final class Material {
        final byte[] encryption, secret;
        final long retireAfter;
        private int references;
        private boolean installed = true, revoked;

        Material(byte[] encryption, byte[] secret, long retireAfter) {
            this.encryption = encryption;
            this.secret = secret;
            this.retireAfter = retireAfter;
        }

        synchronized void retain() {
            references++;
        }

        synchronized void release() {
            references--;
            eraseIfUnused();
        }

        synchronized void retire() {
            installed = false;
            eraseIfUnused();
        }

        synchronized boolean usable() {
            return !revoked;
        }

        synchronized void revoke() {
            revoked = true;
            Arrays.fill(encryption, (byte) 0);
            Arrays.fill(secret, (byte) 0);
        }

        private void eraseIfUnused() {
            if (!installed && references == 0) {
                revoke();
            }
        }

        synchronized boolean unused() {
            return !installed && references == 0;
        }

        synchronized boolean matches(String audience, byte[] canonicalKey, byte[] expected) {
            if (revoked) {
                return false;
            }
            byte[] actual = hmac(secret, utf8(IDENTITY_BINDING + "\0" + audience + "\0"
                    + Base64.getEncoder().encodeToString(canonicalKey)));
            try {
                return MessageDigest.isEqual(expected, Arrays.copyOf(actual, 16));
            } finally {
                Arrays.fill(actual, (byte) 0);
            }
        }
    }

    /** Retains the epoch that admitted the ticket, so key rotation never changes which secret decides its login. */
    private final class Binding implements LoginKeyBinding {
        private final Material material;
        private final byte[] expected;
        private final long deadlineNanos;
        private final AtomicBoolean released = new AtomicBoolean();

        Binding(Material material, byte[] expected, long ttlMillis) {
            this.material = material;
            this.expected = expected;
            this.deadlineNanos = nanoTime.getAsLong() + ttlMillis * 1_000_000L;
            material.retain();
        }

        @Override
        public boolean usable() {
            return !released.get() && nanoTime.getAsLong() - deadlineNanos < 0 && material.usable();
        }

        @Override
        public boolean matches(byte[] canonicalKey) {
            return !released.get() && material.matches(audience, canonicalKey, expected);
        }

        @Override
        public void release() {
            if (released.compareAndSet(false, true)) {
                Arrays.fill(expected, (byte) 0);
                material.release();
            }
        }
    }

    // Domain separation, so each derivation gets its own use of the admission secret
    private static final String IDENTITY_BINDING = "nxs-identity-binding-v1";
    private static final String TOKEN_PREFIX = "NXS1";

    private final String audience;
    private final long maxTtlMs;
    private final LongSupplier nanoTime;
    private final Set<Material> epochs = new HashSet<>();
    private volatile Map<String, Material> keys = Map.of();
    private PrefilterKeySink prefilter = (context, secrets) -> {};

    /**
     * @param audience The endpoint's audience, {@code nxs-stateless-host-v1/<incarnation>}
     * @param maxTtlMs How far ahead of this host's clock a token may expire, at most 120 seconds
     */
    public StatelessAdmissionValidator(String audience, long maxTtlMs) {
        this(audience, maxTtlMs, System::nanoTime);
    }

    StatelessAdmissionValidator(String audience, long maxTtlMs, LongSupplier nanoTime) {
        if (audience == null || audience.isEmpty() || audience.length() > 512 || audience.indexOf(0) >= 0
                || maxTtlMs <= 0 || maxTtlMs > 120_000) {
            throw new IllegalArgumentException("Admission context");
        }
        this.audience = audience;
        this.maxTtlMs = maxTtlMs;
        this.nanoTime = Objects.requireNonNull(nanoTime);
    }

    /**
     * The audience of an endpoint incarnation.
     *
     * @param incarnation 32 lowercase hex digits, fresh for each bound endpoint
     */
    public static String audience(String incarnation) {
        if (incarnation == null || !incarnation.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("Invalid endpoint incarnation");
        }
        return "nxs-stateless-host-v1/" + incarnation;
    }

    /**
     * Validates everything before atomically replacing a bounded snapshot. An epoch whose ID and secret are
     * unchanged keeps its material; a reused ID with a new secret retires the old material, which stays alive for
     * logins it already admitted.
     */
    public synchronized void installKeys(List<AdmissionKey> snapshot) {
        if (snapshot.size() > 8) {
            throw new IllegalArgumentException("At most eight admission epochs");
        }
        Set<String> ids = new HashSet<>();
        for (AdmissionKey key : snapshot) {
            if (key.keyId() == null || !key.keyId().matches("[A-Z0-9]{4}") || key.secret() == null
                    || !AdmissionKey.validSecret(key.secret()) || !ids.add(key.keyId())
                    || key.retireAfter() <= 0) {
                throw new IllegalArgumentException("Invalid admission key snapshot");
            }
        }
        Map<String, Material> next = new HashMap<>();
        try {
            for (AdmissionKey key : snapshot) {
                byte[] secret = utf8(key.secret());
                Material previous = keys.get(key.keyId());
                if (previous != null && previous.retireAfter == key.retireAfter()
                        && MessageDigest.isEqual(previous.secret, secret)) {
                    Arrays.fill(secret, (byte) 0);
                    next.put(key.keyId(), previous);
                } else {
                    next.put(key.keyId(), new Material(StatelessAdmissionCodec.encryptionKey(secret, audience),
                            secret, key.retireAfter()));
                }
            }
        } catch (RuntimeException failed) {
            next.values().stream().filter(m -> !keys.containsValue(m)).forEach(Material::retire);
            throw failed;
        }
        keys.values().stream().filter(m -> !next.containsValue(m)).forEach(Material::retire);
        keys = Map.copyOf(next);
        epochs.addAll(next.values());
        epochs.removeIf(Material::unused);
        publish();
    }

    /**
     * Hands {@code sink} the installed secrets now and after every change, so the native prefilter drops requests
     * no installed key signed.
     */
    public synchronized void mirrorKeys(PrefilterKeySink sink) {
        prefilter = Objects.requireNonNull(sink);
        publish();
    }

    private void publish() {
        Map<String, byte[]> secrets = new HashMap<>();
        keys.forEach((id, key) -> secrets.put(TOKEN_PREFIX + id, key.secret.clone()));
        try {
            prefilter.install(StatelessAdmissionCodec.icePasswordContext(audience), secrets);
        } finally {
            secrets.values().forEach(secret -> Arrays.fill(secret, (byte) 0));
        }
    }

    /** Routine rotation stops new admissions; admitted logins retain their original epoch until completion or expiry. */
    public synchronized void retireKeys(long nowMillis) {
        Map<String, Material> retained = new HashMap<>();
        for (var entry : keys.entrySet()) {
            if (entry.getValue().retireAfter <= nowMillis) {
                entry.getValue().retire();
            } else {
                retained.put(entry.getKey(), entry.getValue());
            }
        }
        epochs.removeIf(Material::unused);
        if (retained.size() != keys.size()) {
            keys = Map.copyOf(retained);
            publish();
        }
    }

    /** Whether any admission key is installed. */
    public boolean ready() {
        return !keys.isEmpty();
    }

    /** The installed keys' IDs. */
    public Set<String> keyIds() {
        return keys.keySet();
    }

    /** Revokes all installed and retained epochs, which also invalidates every pending login. */
    public synchronized void clear() {
        epochs.forEach(Material::revoke);
        epochs.clear();
        keys = Map.of();
        publish();
    }

    /**
     * Opens the ticket a connectivity check names with the installed key, and checks its expiry. Uses no network.
     *
     * @return The admission the ticket grants, a player's or a diagnostic's ({@link VerifiedAdmission#diagnostic()}),
     *         or null to reject it
     */
    public synchronized VerifiedAdmission validate(AdmissionRequest request, long nowMillis) {
        if (request == null) {
            return null;
        }
        try {
            String token = request.localUfrag();
            if (token.length() < 8 || !token.startsWith(TOKEN_PREFIX)) {
                return null;
            }
            String keyId = token.substring(4, 8);
            Material key = keys.get(keyId);
            if (key == null || nowMillis >= key.retireAfter) {
                return null;
            }
            var claims = StatelessAdmissionCodec.open(key.encryption, audience, token, request.remoteUfrag());
            long expiresAt = claims.expiresAt();
            if (expiresAt <= nowMillis || expiresAt - nowMillis > maxTtlMs) {
                return null;
            }
            byte[] identity = HexFormat.of().parseHex(claims.identityBindingHex());
            return new VerifiedAdmission(
                    TicketId.ofToken(token),
                    token,
                    StatelessAdmissionCodec.icePassword(key.secret, audience, token),
                    request.remoteUfrag(),
                    claims.password(),
                    DtlsFingerprint.of(HexFormat.of().parseHex(claims.fingerprintHex())),
                    claims.sctpPort(),
                    claims.maxMessageSize(),
                    expiresAt,
                    claims.networkId(),
                    claims.identityBindingHex(),
                    keyId,
                    new Binding(key, identity, expiresAt - nowMillis),
                    claims.diagnostic());
        } catch (RuntimeException invalid) {
            return null;
        }
    }

    /**
     * When a token expires, as the installed key it names reads it, without the checks admission applies. For a token
     * that reached this host another way, such as an assisted join's ufrag: its used-ticket record must last as long
     * as {@link #validate} could still admit it.
     *
     * @param clientUfrag The client's ICE ufrag the token is bound to
     * @return Epoch milliseconds, or 0 when no installed key opens the token for this client ufrag
     */
    public synchronized long expiry(String token, String clientUfrag) {
        if (token.length() < 8 || !token.startsWith(TOKEN_PREFIX)) {
            return 0;
        }
        Material key = keys.get(token.substring(4, 8));
        if (key == null) {
            return 0;
        }
        try {
            return StatelessAdmissionCodec.open(key.encryption, audience, token, clientUfrag).expiresAt();
        } catch (RuntimeException unreadable) {
            return 0;
        }
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}

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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ProtocolExtension;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.Registration;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/**
 * Registration and recovery. A host saves a fresh P-384 machine key before it asks for a challenge, signs the
 * challenge to complete, and starts a fresh generation each time. On restart it proves the same key again, keeping
 * its IDs and fencing the previous process; a completion whose reply was lost is recovered by its challenge. Used on
 * the lifecycle executor.
 */
final class Registrar {
    private static final String NEW_SERVICE = "new-service", ATTACH_INSTANCE = "attach-instance",
            RECOVER = "recover", BEARER_TOKEN = "bearer-token";

    private final ClientRuntime runtime;
    private final Exchanger exchanger;
    private final RequestSigner signer;
    private final KeyManager keys;
    private final OutcomeQueue outcomes;
    private final ControlChannel control;
    private final OperatorActions actions;
    private Map<String, ProtocolExtension> extensions = Map.of();

    Registrar(ClientRuntime runtime, Exchanger exchanger, RequestSigner signer, KeyManager keys, OutcomeQueue outcomes,
              ControlChannel control, OperatorActions actions) {
        this.actions = actions;
        this.runtime = runtime;
        this.exchanger = exchanger;
        this.signer = signer;
        this.keys = keys;
        this.outcomes = outcomes;
        this.control = control;
    }

    Map<String, ProtocolExtension> extensions() {
        return extensions;
    }

    Registration view() {
        Wire.SavedRegistration r = runtime.registration();
        return new Registration(r.registrationId(), r.instanceId(), r.keyId(), r.leaseGeneration(), r.serviceId(),
                r.publicAddress());
    }

    /** Reads discovery, recovers or registers, and starts a fresh generation whose sequence counts from zero. */
    CompletableFuture<Void> register() {
        return discover()
                .thenCompose(ignored -> Async.attempt(() -> {
                    runtime.keeper.load();
                    String saved = runtime.keeper.state().provider();
                    if (saved != null && !runtime.origin.equals(saved)) {
                        throw new IOException("State belongs to another provider; use a separate directory");
                    }
                    loadMachineKey();
                    return recoverOrEnroll();
                }))
                .thenApply(ignored -> {
                    beginGeneration();
                    return null;
                });
    }

    private CompletableFuture<Void> discover() {
        URI document = URI.create(runtime.origin + "/.well-known/nethernet-external-signaling");
        return exchanger.get(document).thenCompose(json -> {
            Wire.Discovery discovery;
            try {
                discovery = WireCodec.discovery(json);
                if (!runtime.origin.equals(discovery.provider())) {
                    throw new IOException("Discovery provider mismatch");
                }
                if (!discovery.protocols().contains(ProviderCrypto.PROTOCOL)) {
                    throw new IOException("Unsupported required provider capability: protocols");
                }
                if (!discovery.signatures().contains(ProviderCrypto.SIGNATURE)) {
                    throw new IOException("Unsupported required provider capability: signatures");
                }
                if (!discovery.schemes().contains(runtime.config.authorizationScheme())) {
                    throw new IOException("Unsupported provider authorization");
                }
            } catch (IOException | RuntimeException invalid) {
                return CompletableFuture.failedFuture(invalid);
            }
            runtime.discovery = discovery;
            return Async.within(runtime.transport.configureStunServers(discovery.stunServers()), Duration.ofSeconds(10),
                    runtime.executor, "STUN configuration");
        });
    }

    /** A new logical instance gets a fresh machine key, saved before any challenge is requested. */
    private void loadMachineKey() throws IOException, GeneralSecurityException {
        ProviderState state = runtime.keeper.state();
        if (state.machineKey() == null) {
            KeyPair pair = ProviderCrypto.generate();
            runtime.keeper.save(state.withMachineKey(runtime.origin, new ProviderState.MachineKey(
                    ProviderCrypto.base64(pair.getPrivate().getEncoded()), ProviderCrypto.publicJwk(pair.getPublic()))));
        }
        signer.machineKey();
    }

    private CompletableFuture<Void> recoverOrEnroll() {
        CompletableFuture<Void> recovered = Async.done();
        if (runtime.keeper.state().registration() != null) {
            recovered = recover(runtime.keeper.state().registration().registrationId(), true).thenAccept(found -> {
                if (!found) {
                    // Gone for good: register afresh with a new key
                    runtime.log.registrationLost(runtime.keeper.state().registration().instanceId());
                    outcomes.clear();
                    save(ProviderState.EMPTY);
                    try {
                        loadMachineKey();
                    } catch (IOException | GeneralSecurityException failure) {
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                }
            });
        }
        return recovered.thenCompose(ignored -> runtime.keeper.state().registration() == null ? enroll() : Async.done())
                .thenApply(ignored -> {
                    if (!runtime.registration().provider().equals(runtime.origin)) {
                        throw new IllegalStateException("Registration audience changed");
                    }
                    return null;
                });
    }

    private void beginGeneration() {
        ProviderState state = runtime.keeper.state();
        save(state.withRegistration(state.registration()).withPendingOperation(null));
    }

    /** Recovers the saved registration; false when the provider has nothing to recover. */
    CompletableFuture<Boolean> recoverExisting() {
        return recover(runtime.registration().registrationId(), true);
    }

    /**
     * Settles a WebSocket operation whose fate is unknown: ordinary recovery starts a fresh generation, so neither
     * the old operation nor expired profile bytes are replayed, and the carrier reconnects for it.
     */
    CompletableFuture<Void> recoverAmbiguous() {
        control.close();
        return recoverExisting().thenApply(ignored -> {
            save(runtime.keeper.state().withPendingOperation(null));
            try {
                control.configure();
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
            return null;
        });
    }

    /**
     * Recovers by registration ID, or by challenge ID after a lost completion. A recovery whose own completion reply
     * was lost is retried once with a fresh challenge.
     *
     * @return false when the provider has nothing to recover
     */
    private CompletableFuture<Boolean> recover(String id, boolean registrationId) {
        return recover(id, registrationId, 0);
    }

    private CompletableFuture<Boolean> recover(String id, boolean registrationId, int attempt) {
        return exchanger.unsigned("register", WireCodec.recovery(id), null)
                .thenCompose(json -> Async.attempt(() -> completeRecovery(WireCodec.challenge(json),
                        registrationId ? id : null)).thenApply(ignored -> true))
                .exceptionallyCompose(failure -> {
                    if (ProviderFailures.refusedWith(failure, ProviderFailures.RECOVERY_UNAVAILABLE)) {
                        return CompletableFuture.completedFuture(false);
                    }
                    if (ProviderFailures.refusedWith(failure, ProviderFailures.RECOVERY_REQUIRED) && attempt == 0) {
                        return recover(id, registrationId, 1);
                    }
                    return CompletableFuture.failedFuture(failure);
                });
    }

    /** @param registrationId The saved registration ID, or null after a lost completion */
    private CompletableFuture<Void> completeRecovery(Wire.Challenge challenge, String registrationId) throws Exception {
        requireBound(challenge, "Unbound recovery challenge");
        String bound = challenge.context().registrationId();
        if (!RECOVER.equals(challenge.context().mode()) || bound.isEmpty()
                || registrationId != null && !registrationId.equals(bound)
                || challenge.expiresAt() <= runtime.clock.millis() || challenge.difficulty() != 0) {
            throw new IOException("Unbound recovery challenge");
        }
        ProviderState state = runtime.keeper.state();
        boolean pending = state.pendingMachineKey() != null
                && state.pendingMachineKey().thumbprint().equals(challenge.thumbprint());
        if (!pending && !state.machineKey().thumbprint().equals(challenge.thumbprint())) {
            throw new IOException("Recovery key does not match durable state");
        }
        PrivateKey key = pending
                ? ProviderCrypto.privateKey(state.pendingMachineKey().privateKey())
                : signer.machineKey();
        String intent = UUID.randomUUID().toString();
        String body = WireCodec.completion(challenge.challengeId(), "0", intent,
                ProviderCrypto.sign(key, challenge.proof("0", intent)));
        return exchanger.unsigned("complete", body, null).thenApply(json -> {
            Wire.RegistrationReply recovered = registration(json);
            if (!bound.equals(recovered.registration().registrationId())) {
                throw new IllegalStateException("Recovered registration changed");
            }
            Wire.SavedRegistration previous = runtime.keeper.state().registration();
            if (previous != null && (!previous.instanceId().equals(recovered.registration().instanceId())
                    || !previous.registrationId().equals(recovered.registration().registrationId()))) {
                throw new IllegalStateException("Recovered instance identity changed");
            }
            extensions = recovered.extensions();
            // Completion starts a fresh fenced generation; operational sequencing starts at zero
            ProviderState next = runtime.keeper.state().withRegistration(recovered.registration());
            save(pending ? next.withPromotedMachineKey() : next);
            return null;
        });
    }

    private CompletableFuture<Void> enroll() {
        Wire.Challenge saved = runtime.keeper.state().challenge();
        CompletableFuture<Wire.Challenge> challenge;
        if (saved != null) {
            challenge = recover(saved.challengeId(), false).thenCompose(found -> {
                if (found) {
                    return CompletableFuture.completedFuture(null);
                }
                // Nothing was created, so an expired challenge is simply replaced
                return saved.expiresAt() > runtime.clock.millis()
                        ? CompletableFuture.completedFuture(saved)
                        : requestChallenge();
            });
        } else {
            challenge = requestChallenge();
        }
        return challenge.thenCompose(c -> c == null ? Async.done() : Async.attempt(() -> complete(c)));
    }

    private CompletableFuture<Wire.Challenge> requestChallenge() {
        String body = WireCodec.register(runtime.keeper.state().machineKey().publicJwk(), runtime.config.label());
        return exchanger.unsigned("register", body, runtime.config.authorizationToken()).thenApply(json -> {
            try {
                Wire.Challenge challenge = WireCodec.challenge(json);
                runtime.keeper.save(runtime.keeper.state().withChallenge(challenge));
                return challenge;
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        });
    }

    private CompletableFuture<Void> complete(Wire.Challenge challenge) throws IOException {
        requireBound(challenge, "Unbound registration challenge");
        if (!runtime.keeper.state().machineKey().thumbprint().equals(challenge.thumbprint())) {
            throw new IOException("Unbound registration challenge");
        }
        // Anonymous hosts can only create a service; a token may also attach to one
        String mode = challenge.context().mode();
        boolean bearer = BEARER_TOKEN.equals(runtime.config.authorizationScheme());
        if (!NEW_SERVICE.equals(mode) && !(bearer && ATTACH_INSTANCE.equals(mode))) {
            throw new IOException("Challenge registration context changed");
        }
        if (challenge.authorizationScheme() != null
                && !runtime.config.authorizationScheme().equals(challenge.authorizationScheme())) {
            throw new IOException("Challenge authorization changed");
        }
        if (bearer && (challenge.authorizationReference() == null || challenge.authorizationReference().isBlank())) {
            throw new IOException("Bearer challenge omitted its authority reference");
        }
        int bits = challenge.difficulty();
        if (bits < 0 || bits > 24) {
            throw new IOException("Unsupported proof of work");
        }
        if (bearer && bits != 0) {
            throw new IOException("Bearer-authorized registration unexpectedly requires proof of work");
        }
        String intent = UUID.randomUUID().toString();
        // Up to 24 bits takes seconds of hashing, which runs off the lifecycle thread
        return CompletableFuture.supplyAsync(() -> proofOfWork(challenge, intent, bits), ForkJoinPool.commonPool())
                .thenComposeAsync(nonce -> {
                    if (nonce == null) {
                        return CompletableFuture.failedFuture(new IOException("Challenge expired before proof completed"));
                    }
                    String signature;
                    try {
                        signature = ProviderCrypto.sign(signer.machineKey(), challenge.proof(nonce, intent));
                    } catch (GeneralSecurityException failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                    return exchanger.unsigned("complete",
                            WireCodec.completion(challenge.challengeId(), nonce, intent, signature), null);
                }, runtime.executor)
                .exceptionallyCompose(failure -> {
                    if (!ProviderFailures.refusedWith(failure, ProviderFailures.RECOVERY_REQUIRED)) {
                        return CompletableFuture.failedFuture(failure);
                    }
                    // The completion committed but its reply was lost
                    return recover(challenge.challengeId(), false).thenCompose(found -> {
                        if (found) {
                            return CompletableFuture.completedFuture(null);
                        }
                        // Consumed but unrecoverable, so the next start registers afresh
                        save(runtime.keeper.state().withChallenge(null));
                        return CompletableFuture.failedFuture(failure);
                    });
                })
                .thenApply(json -> {
                    if (json == null) {
                        return null;
                    }
                    Wire.RegistrationReply registration = registration(json);
                    // The link may carry a one-time credential, so it stays in memory only
                    actions.offered(registration.operatorAction());
                    extensions = registration.extensions();
                    save(runtime.keeper.state().withAdmissionKeys(java.util.List.of())
                            .withRegistration(registration.registration()));
                    if (registration.admissionKey() != null) {
                        keys.deliver(registration.admissionKey());
                    }
                    return null;
                });
    }

    /** The lowest nonce whose proof hash has the leading zero bits asked for, or null once the challenge expired. */
    private String proofOfWork(Wire.Challenge challenge, String intent, int bits) {
        for (long i = 0; runtime.clock.millis() < challenge.expiresAt(); i++) {
            String candidate = Long.toString(i);
            if (ProviderCrypto.meetsDifficulty(ProviderCrypto.digest(challenge.proof(candidate, intent)), bits)) {
                return candidate;
            }
        }
        return null;
    }

    /** Protocol and signature are schema constants, which the codec already refused when different. */
    private void requireBound(Wire.Challenge challenge, String failure) throws IOException {
        String digest = ProviderCrypto.base64(ProviderCrypto.digest(ProviderCrypto.array(challenge.context().values())));
        if (!runtime.origin.equals(challenge.audience()) || !digest.equals(challenge.contextDigest())) {
            throw new IOException(failure);
        }
    }

    private Wire.RegistrationReply registration(JsonObject json) {
        try {
            Wire.RegistrationReply reply = WireCodec.registration(json);
            if (!runtime.origin.equals(reply.registration().provider())) {
                throw new IOException("Registration provider changed");
            }
            return reply;
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    /** Proves and installs a replacement machine key, then retires the old one. */
    /** The canonical array the replacement machine key signs to prove it may take over the registration. */
    static String rotationPayload(String audience, String instanceId, String oldKeyId, JsonObject replacementJwk,
                                  long generation, String intent) {
        return ProviderCrypto.array(ProviderCrypto.PROTOCOL, "rotate", audience, instanceId, oldKeyId,
                ProviderCrypto.thumbprint(replacementJwk), generation, intent);
    }

    CompletableFuture<Registration> rotateMachineKey() {
        return Async.attempt(() -> {
            runtime.requireResolved();
            KeyPair replacement = ProviderCrypto.generate();
            JsonObject jwk = ProviderCrypto.publicJwk(replacement.getPublic());
            runtime.keeper.save(runtime.keeper.state().withPendingMachineKey(new ProviderState.MachineKey(
                    ProviderCrypto.base64(replacement.getPrivate().getEncoded()), jwk)));
            String intent = UUID.randomUUID().toString();
            Wire.SavedRegistration registration = runtime.registration();
            String proof = ProviderCrypto.sign(replacement.getPrivate(), rotationPayload(runtime.origin,
                    registration.instanceId(), registration.keyId(), jwk, runtime.keeper.state().generation(), intent));
            String oldKey = registration.keyId();
            return exchanger.signed("rotate", WireCodec.rotation(jwk, proof), intent,
                            io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard.ALWAYS)
                    .thenCompose(json -> {
                        String keyId;
                        try {
                            keyId = WireCodec.rotationKeyId(json);
                        } catch (IOException failure) {
                            return CompletableFuture.failedFuture(failure);
                        }
                        ProviderState state = runtime.keeper.state();
                        save(new ProviderState(state.provider(), state.pendingMachineKey(), null, state.challenge(),
                                state.registration().withKeyId(keyId), state.generation(), state.sequence(),
                                state.admissionKeys(), state.keyRequestId(), state.pendingOperation(),
                                state.pendingEvents()));
                        return exchanger.signed("retire", WireCodec.retire(oldKey));
                    })
                    .thenApply(ignored -> view());
        });
    }

    /** Ends the registration for good; a deregistered instance can never recover, so its saved state goes too. */
    CompletableFuture<Void> deregister() {
        return exchanger.signed("deregister", "{}").thenApply(ignored -> {
            runtime.started = false;
            outcomes.clear();
            save(ProviderState.EMPTY);
            return null;
        });
    }

    /** An application's explicit request to an operation an extension advertised; never automatic. */
    CompletableFuture<String> extensionRequest(String namespace, String operation, String method, String body) {
        return Async.attempt(() -> {
            ProtocolExtension extension = runtime.discovery == null ? null : runtime.discovery.extensions().get(namespace);
            if (extension == null) {
                throw new IOException("Extension unavailable");
            }
            JsonObject data = JsonParser.parseString(extension.data()).getAsJsonObject();
            JsonObject operations = data.has("operations") ? data.getAsJsonObject("operations") : null;
            if (operations == null || !operations.has(operation)) {
                throw new IOException("Extension operation unavailable");
            }
            URI uri = exchanger.trusted(URI.create(operations.get(operation).getAsString()));
            String request = body == null ? null : WireCodec.JSON.toJson(JsonParser.parseString(body).getAsJsonObject());
            return exchanger.signedTo(uri, method, request).thenApply(reply -> {
                ProtocolExtensions.validate(reply);
                return WireCodec.JSON.toJson(reply);
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

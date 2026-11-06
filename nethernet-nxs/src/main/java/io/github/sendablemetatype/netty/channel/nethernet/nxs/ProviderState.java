package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import io.netty.util.NetUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Everything the client saves, typed. The file keeps the member names of the upstream client, so a host moving
 * from it keeps its registration, and adds {@code stateVersion}; a file without one has that layout.
 *
 * @param provider         The origin the machine key was created for, null before one exists
 * @param machineKey       Null before the first start creates one
 * @param pendingMachineKey A replacement saved before {@code rotate}, until the provider confirms it
 * @param challenge        An enrollment challenge saved before completing it, so a lost reply can be recovered
 * @param registration     Null until registered
 * @param admissionKeys    The installed epochs, oldest first, active last
 * @param keyRequestId     The outstanding admission key request, saved before it is sent
 * @param pendingOperation The idempotency key of a WebSocket operation whose fate is unknown, which requires ordinary
 *                         recovery before anything fresh is sent
 * @param pendingEvents    Outcomes collected but not yet acknowledged
 */
record ProviderState(String provider, MachineKey machineKey, MachineKey pendingMachineKey, Wire.Challenge challenge,
                     Wire.SavedRegistration registration, long generation, long sequence,
                     List<StoredKey> admissionKeys, String keyRequestId, String pendingOperation,
                     List<Wire.OutcomeEvent> pendingEvents) {
    static final int VERSION = 1;
    static final ProviderState EMPTY = new ProviderState(null, null, null, null, null, 0, 0, List.of(), null, null, List.of());

    ProviderState {
        admissionKeys = List.copyOf(admissionKeys);
        pendingEvents = List.copyOf(pendingEvents);
    }

    /**
     * A machine signing key.
     *
     * @param privateKey The PKCS#8 encoding as unpadded base64url
     * @param publicJwk  The public P-384 JWK, members {@code crv,kty,x,y}
     */
    record MachineKey(String privateKey, JsonObject publicJwk) {
        MachineKey {
            publicJwk = publicJwk.deepCopy();
        }

        @Override
        public JsonObject publicJwk() {
            return publicJwk.deepCopy();
        }

        String thumbprint() {
            return ProviderCrypto.thumbprint(publicJwk);
        }

        @Override
        public String toString() {
            return "MachineKey[" + thumbprint() + "]";
        }
    }

    /** @param retireAfter {@link Long#MAX_VALUE} until the provider schedules a retirement */
    record StoredKey(String keyId, String secret, long retireAfter) {
        AdmissionKey admissionKey() {
            return new AdmissionKey(keyId, secret, retireAfter);
        }

        @Override
        public String toString() {
            return "StoredKey[keyId=" + keyId + "]";
        }
    }

    ProviderState withMachineKey(String provider, MachineKey key) {
        return new ProviderState(provider, key, pendingMachineKey, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withPendingMachineKey(MachineKey key) {
        return new ProviderState(provider, machineKey, key, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withChallenge(Wire.Challenge challenge) {
        return new ProviderState(provider, machineKey, pendingMachineKey, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    /** A completed registration or recovery: a fresh generation whose sequence starts again. */
    ProviderState withRegistration(Wire.SavedRegistration registration) {
        return new ProviderState(provider, machineKey, pendingMachineKey, null, registration,
                registration.leaseGeneration(), 0, admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withSequence(long sequence) {
        return new ProviderState(provider, machineKey, pendingMachineKey, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withAdmissionKeys(List<StoredKey> keys) {
        return new ProviderState(provider, machineKey, pendingMachineKey, challenge, registration, generation, sequence,
                keys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withKeyRequestId(String keyRequestId) {
        return new ProviderState(provider, machineKey, pendingMachineKey, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withPendingOperation(String pendingOperation) {
        return new ProviderState(provider, machineKey, pendingMachineKey, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    ProviderState withPendingEvents(List<Wire.OutcomeEvent> events) {
        return new ProviderState(provider, machineKey, pendingMachineKey, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, events);
    }

    /** Promotes the saved replacement key, once the provider has confirmed it. */
    ProviderState withPromotedMachineKey() {
        return new ProviderState(provider, pendingMachineKey, null, challenge, registration, generation, sequence,
                admissionKeys, keyRequestId, pendingOperation, pendingEvents);
    }

    JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("stateVersion", VERSION);
        json.addProperty("protocol", ProviderCrypto.PROTOCOL);
        if (provider != null) {
            json.addProperty("provider", provider);
        }
        if (machineKey != null) {
            json.addProperty("privateKey", machineKey.privateKey());
            json.add("publicKeyJwk", machineKey.publicJwk());
        }
        if (pendingMachineKey != null) {
            json.addProperty("pendingPrivateKey", pendingMachineKey.privateKey());
            json.add("pendingPublicKeyJwk", pendingMachineKey.publicJwk());
        }
        if (challenge != null) {
            json.add("challenge", WireCodec.challengeJson(challenge));
        }
        if (registration != null) {
            json.add("registration", WireCodec.registrationJson(registration));
            json.addProperty("generation", generation);
            json.addProperty("sequence", sequence);
        }
        JsonArray keys = new JsonArray();
        for (StoredKey key : admissionKeys) {
            JsonObject entry = new JsonObject();
            entry.addProperty("keyId", key.keyId());
            entry.addProperty("secret", key.secret());
            if (key.retireAfter() != Long.MAX_VALUE) {
                entry.addProperty("retireAfter", key.retireAfter());
            }
            keys.add(entry);
        }
        json.add("ticketKeys", keys);
        if (keyRequestId != null) {
            json.addProperty("keyRequestId", keyRequestId);
        }
        if (pendingOperation != null) {
            json.addProperty("pendingWebSocketOperation", pendingOperation);
        }
        if (!pendingEvents.isEmpty()) {
            JsonArray events = new JsonArray();
            pendingEvents.forEach(event -> events.add(WireCodec.outcome(event, true)));
            json.add("pendingEvents", events);
        }
        return json;
    }

    /**
     * Reads a saved state. Outcomes that no longer validate are dropped, since the provider would refuse them.
     *
     * @throws IOException If the file comes from a later client or its identity cannot be read
     */
    static ProviderState fromJson(JsonObject json) throws IOException {
        try {
            if (json.has("stateVersion") && json.get("stateVersion").getAsInt() > VERSION) {
                throw new IOException("Provider state was written by a newer client");
            }
            MachineKey machineKey = json.has("privateKey")
                    ? new MachineKey(json.get("privateKey").getAsString(), json.getAsJsonObject("publicKeyJwk"))
                    : null;
            MachineKey pending = json.has("pendingPrivateKey")
                    ? new MachineKey(json.get("pendingPrivateKey").getAsString(),
                    json.getAsJsonObject("pendingPublicKeyJwk"))
                    : null;
            Wire.Challenge challenge = json.has("challenge") ? WireCodec.challenge(json.getAsJsonObject("challenge")) : null;
            Wire.SavedRegistration registration = json.has("registration")
                    ? WireCodec.savedRegistration(json.getAsJsonObject("registration"))
                    : null;
            List<StoredKey> keys = new ArrayList<>();
            if (json.has("ticketKeys")) {
                for (JsonElement entry : json.getAsJsonArray("ticketKeys")) {
                    JsonObject key = entry.getAsJsonObject();
                    keys.add(new StoredKey(key.get("keyId").getAsString(), key.get("secret").getAsString(),
                            key.has("retireAfter") ? key.get("retireAfter").getAsLong() : Long.MAX_VALUE));
                }
            }
            List<Wire.OutcomeEvent> events = new ArrayList<>();
            if (json.has("pendingEvents")) {
                for (JsonElement entry : json.getAsJsonArray("pendingEvents")) {
                    outcome(entry).ifPresent(events::add);
                }
            }
            return new ProviderState(
                    json.has("provider") ? json.get("provider").getAsString() : null,
                    machineKey,
                    pending,
                    challenge,
                    registration,
                    json.has("generation") ? json.get("generation").getAsLong() : 0,
                    json.has("sequence") ? json.get("sequence").getAsLong() : 0,
                    keys,
                    json.has("keyRequestId") ? json.get("keyRequestId").getAsString() : null,
                    json.has("pendingWebSocketOperation") ? json.get("pendingWebSocketOperation").getAsString() : null,
                    events);
        } catch (RuntimeException invalid) {
            throw new IOException("Unreadable provider state", invalid);
        }
    }

    private static java.util.Optional<Wire.OutcomeEvent> outcome(JsonElement entry) {
        try {
            JsonObject event = entry.getAsJsonObject();
            OutcomeStage stage = null;
            for (OutcomeStage candidate : OutcomeStage.values()) {
                if (candidate.wire().equals(event.get("stage").getAsString())) {
                    stage = candidate;
                }
            }
            if (stage == null) {
                return java.util.Optional.empty();
            }
            String reason = event.has("reason") ? event.get("reason").getAsString() : null;
            if ((stage == OutcomeStage.FAILED) != (reason != null)
                    || reason != null && !TransportOutcome.REASON.matcher(reason).matches()) {
                return java.util.Optional.empty();
            }
            InetSocketAddress remote = null;
            if (event.has("remoteAddress") && event.has("remotePort")) {
                InetAddress address = NetUtil.createInetAddressFromIpAddressString(event.get("remoteAddress").getAsString());
                if (address != null) {
                    remote = new InetSocketAddress(address, event.get("remotePort").getAsInt());
                }
            }
            return java.util.Optional.of(new Wire.OutcomeEvent(new TicketId(event.get("ticketId").getAsString()), stage,
                    event.get("occurredAt").getAsLong(), reason, remote));
        } catch (RuntimeException invalid) {
            return java.util.Optional.empty();
        }
    }
}

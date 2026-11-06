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

import com.google.gson.*;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.*;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.*;
import io.github.sendablemetatype.netty.util.address.EndpointAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Maps the typed messages to the exact NXS v1 JSON and back. Senders follow the schema; receivers ignore members
 * they do not know and refuse a document only where the wire reference says so. Member order follows the bodies
 * upstream sends, though only the bytes actually sent are signed.
 */
final class WireCodec {
    /** Nulls are meaningful: a heartbeat's {@code location:null} clears a declaration, and extension data may hold them. */
    static final Gson JSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    static final String STATELESS_CAPABILITY = "nethernet.stateless-admission.v1";
    private static final long MAX_SAFE = 9007199254740991L;

    private WireCodec() {
    }

    // ---- Documents from the provider

    /** @throws IOException If the body is not one JSON object */
    static JsonObject object(String body) throws IOException {
        try {
            JsonElement element = JsonParser.parseString(body);
            if (!element.isJsonObject()) {
                throw new IOException("Provider reply is not a JSON object");
            }
            return element.getAsJsonObject();
        } catch (JsonParseException invalid) {
            throw new IOException("Provider reply is not JSON", invalid);
        }
    }

    static Wire.Discovery discovery(JsonObject document) throws IOException {
        try {
            ProviderContract.require("discovery", document);
            ProtocolExtensions.validate(document);
            long interval = document.getAsJsonObject("limits").get("heartbeatIntervalMs").getAsLong();
            if (interval < 1000 || interval > 30000) {
                throw new IOException("Unsupported heartbeat interval");
            }
            Map<String, ProtocolExtension> extensions = extensions(document);
            return new Wire.Discovery(
                    document.get("provider").getAsString(),
                    strings(document.getAsJsonArray("protocols")),
                    strings(document.getAsJsonArray("signatures")),
                    strings(document.getAsJsonObject("authorization").getAsJsonArray("schemes")),
                    interval,
                    extensions,
                    stunServers(document));
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid provider discovery", invalid);
        }
    }

    private static List<StunServer> stunServers(JsonObject document) throws IOException {
        try {
            JsonObject extensions = document.getAsJsonObject("extensions");
            JsonObject extension = extensions == null ? null : extensions.getAsJsonObject(Wire.CONNECTIVITY);
            if (extension == null || extension.get("version").getAsInt() != 1) {
                return List.of();
            }
            JsonArray values = extension.getAsJsonObject("data").getAsJsonArray("stunServers");
            if (values == null) {
                return List.of();
            }
            if (values.size() > 2) {
                throw new IllegalArgumentException("STUN discovery bound");
            }
            List<StunServer> servers = new ArrayList<>();
            for (JsonElement value : values) {
                JsonObject server = value.getAsJsonObject();
                servers.add(new StunServer(server.get("host").getAsString(),
                        server.get("port").getAsBigDecimal().intValueExact()));
            }
            return List.copyOf(servers);
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid provider STUN discovery", invalid);
        }
    }

    static Wire.Challenge challenge(JsonObject document) throws IOException {
        try {
            ProviderContract.require("challenge", document);
            JsonObject context = document.getAsJsonObject("context");
            JsonObject authorization = document.has("authorization") ? document.getAsJsonObject("authorization") : null;
            return new Wire.Challenge(
                    document.get("challengeId").getAsString(),
                    document.get("nonce").getAsString(),
                    document.get("audience").getAsString(),
                    document.get("thumbprint").getAsString(),
                    new Wire.ChallengeContext(
                            context.get("mode").getAsString(),
                            optionalString(context, "label"),
                            optionalString(context, "authorizationId"),
                            optionalString(context, "serviceId"),
                            optionalString(context, "registrationId")),
                    document.get("contextDigest").getAsString(),
                    document.get("expiresAt").getAsLong(),
                    document.get("serverTime").getAsLong(),
                    document.getAsJsonObject("pow").get("difficulty").getAsInt(),
                    authorization == null ? null : authorization.get("scheme").getAsString(),
                    authorization == null ? null : authorization.get("reference").getAsString());
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid provider challenge", invalid);
        }
    }

    static JsonObject challengeJson(Wire.Challenge challenge) {
        JsonObject json = new JsonObject();
        json.addProperty("protocol", ProviderCrypto.PROTOCOL);
        json.addProperty("signature", ProviderCrypto.SIGNATURE);
        json.addProperty("challengeId", challenge.challengeId());
        json.addProperty("nonce", challenge.nonce());
        json.addProperty("audience", challenge.audience());
        json.addProperty("thumbprint", challenge.thumbprint());
        JsonObject context = new JsonObject();
        Wire.ChallengeContext c = challenge.context();
        context.addProperty("mode", c.mode());
        context.addProperty("label", c.label());
        context.addProperty("authorizationId", c.authorizationId());
        context.addProperty("serviceId", c.serviceId());
        context.addProperty("registrationId", c.registrationId());
        json.add("context", context);
        json.addProperty("contextDigest", challenge.contextDigest());
        json.addProperty("expiresAt", challenge.expiresAt());
        json.addProperty("serverTime", challenge.serverTime());
        JsonObject pow = new JsonObject();
        pow.addProperty("difficulty", challenge.difficulty());
        json.add("pow", pow);
        if (challenge.authorizationScheme() != null) {
            JsonObject authorization = new JsonObject();
            authorization.addProperty("scheme", challenge.authorizationScheme());
            authorization.addProperty("reference", challenge.authorizationReference());
            json.add("authorization", authorization);
        }
        return json;
    }

    static Wire.RegistrationReply registration(JsonObject document) throws IOException {
        try {
            ProviderContract.require("registration", document);
            ProtocolExtensions.validate(document);
            boolean hasService = document.has("serviceId"), hasAddress = document.has("publicAddress");
            if (hasService != hasAddress) {
                throw new IOException("Incomplete public endpoint metadata");
            }
            String serviceId = null, publicAddress = null;
            if (hasService) {
                serviceId = nonBlank(document.get("serviceId"));
                publicAddress = nonBlank(document.get("publicAddress"));
            }
            long generation = document.get("leaseGeneration").getAsLong();
            if (generation < 1 || generation > MAX_SAFE) {
                throw new IOException("Invalid lease generation");
            }
            var registration = new Wire.SavedRegistration(
                    document.get("provider").getAsString(),
                    document.get("registrationId").getAsString(),
                    document.get("instanceId").getAsString(),
                    document.get("keyId").getAsString(),
                    generation,
                    serviceId,
                    publicAddress);
            JsonElement key = document.get("admissionKey");
            return new Wire.RegistrationReply(
                    registration,
                    key == null || key.isJsonNull() ? null : admissionKey(key.getAsJsonObject()),
                    operatorAction(document.get("operatorAction")).orElse(null),
                    extensions(document),
                    serverTime(document));
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid provider registration", invalid);
        }
    }

    static JsonObject registrationJson(Wire.SavedRegistration registration) {
        JsonObject json = new JsonObject();
        json.addProperty("protocol", ProviderCrypto.PROTOCOL);
        json.addProperty("provider", registration.provider());
        json.addProperty("registrationId", registration.registrationId());
        json.addProperty("instanceId", registration.instanceId());
        json.addProperty("keyId", registration.keyId());
        json.addProperty("leaseGeneration", registration.leaseGeneration());
        if (registration.serviceId() != null) {
            json.addProperty("serviceId", registration.serviceId());
            json.addProperty("publicAddress", registration.publicAddress());
        }
        return json;
    }

    /** Reads a saved registration, whatever else an older client kept beside it. */
    static Wire.SavedRegistration savedRegistration(JsonObject json) {
        return new Wire.SavedRegistration(
                json.get("provider").getAsString(),
                json.get("registrationId").getAsString(),
                json.get("instanceId").getAsString(),
                json.get("keyId").getAsString(),
                json.get("leaseGeneration").getAsLong(),
                optionalString(json, "serviceId"),
                optionalString(json, "publicAddress"));
    }

    /**
     * Validates the members a heartbeat reply must carry, so an incomplete reply fails the heartbeat instead of
     * being half applied. Readiness reasons that are not provider codes are dropped, as they only inform.
     */
    static Wire.HeartbeatReply heartbeatReply(JsonObject document) throws IOException {
        try {
            ProtocolExtensions.validate(document);
            for (String member : List.of("hostProfileRevision", "readiness", "admissionKeyRetirements")) {
                if (!document.has(member)) {
                    throw new IOException("Heartbeat reply omits " + member);
                }
            }
            JsonElement revision = document.get("hostProfileRevision");
            String hostProfileRevision = null;
            if (!revision.isJsonNull()) {
                hostProfileRevision = string(revision);
                if (hostProfileRevision.length() > 184) {
                    throw new IOException("Host profile revision too long");
                }
            }
            JsonObject readiness = document.getAsJsonObject("readiness");
            JsonElement routable = readiness.get("routable");
            if (routable == null || !routable.isJsonPrimitive() || !routable.getAsJsonPrimitive().isBoolean()) {
                throw new IOException("Readiness omits routable");
            }
            JsonArray reasonArray = readiness.getAsJsonArray("reasons");
            if (reasonArray == null) {
                throw new IOException("Readiness omits reasons");
            }
            LinkedHashSet<String> reasons = new LinkedHashSet<>();
            for (JsonElement reason : reasonArray) {
                if (reasons.size() < 32 && reason.isJsonPrimitive() && reason.getAsJsonPrimitive().isString()
                        && reason.getAsString().matches("[a-z][a-z0-9_]{0,63}")) {
                    reasons.add(reason.getAsString());
                }
            }
            List<Wire.KeyRetirement> retirements = new ArrayList<>();
            for (JsonElement entry : document.getAsJsonArray("admissionKeyRetirements")) {
                JsonObject retirement = entry.getAsJsonObject();
                String keyId = string(retirement.get("keyId"));
                long retireAfter = retirement.get("retireAfter").getAsBigDecimal().longValueExact();
                if (!keyId.matches("[A-Z0-9]{4}") || retireAfter < 0 || retireAfter > MAX_SAFE) {
                    throw new IOException("Invalid admission key retirement");
                }
                retirements.add(new Wire.KeyRetirement(keyId, retireAfter));
            }
            CheckInSchedule checkIn = document.has("checkIn") ? CheckInSchedule.parse(document) : null;
            Wire.KeyRequest keyRequest = null;
            if (document.has("admissionKeyRequest")) {
                JsonObject request = document.getAsJsonObject("admissionKeyRequest");
                keyRequest = new Wire.KeyRequest(string(request.get("id")), string(request.get("keyId")));
            }
            AdmissionKey delivered = document.has("admissionKey")
                    ? admissionKey(document.getAsJsonObject("admissionKey"))
                    : null;
            JsonElement available = document.get("operatorActionAvailable");
            return new Wire.HeartbeatReply(
                    hostProfileRevision,
                    new Readiness(routable.getAsBoolean(), List.copyOf(reasons)),
                    List.copyOf(retirements),
                    checkIn,
                    keyRequest,
                    delivered,
                    available != null && available.isJsonPrimitive() && available.getAsJsonPrimitive().isBoolean()
                            && available.getAsBoolean(),
                    extensions(document),
                    serverTime(document));
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid heartbeat reply", invalid);
        }
    }

    /** The regional checks a heartbeat reply's connectivity extension reports. */
    record ConnectivityFeedback(long candidateRevision, List<ConnectivityCheck> checks) {
    }

    /**
     * @return The feedback, or empty when the reply carries none in version 1
     * @throws IllegalArgumentException If the feedback is malformed or over its bound
     */
    static Optional<ConnectivityFeedback> connectivityFeedback(Map<String, ProtocolExtension> extensions) {
        ProtocolExtension extension = extensions.get(Wire.CONNECTIVITY);
        if (extension == null || extension.version() != 1) {
            return Optional.empty();
        }
        JsonObject data = JsonParser.parseString(extension.data()).getAsJsonObject();
        if (!data.has("checks")) {
            return Optional.empty();
        }
        long revision = data.get("candidateRevision").getAsBigDecimal().longValueExact();
        JsonArray checks = data.getAsJsonArray("checks");
        if (checks.size() > 18) {
            throw new IllegalArgumentException("Connectivity feedback bound");
        }
        List<ConnectivityCheck> parsed = new ArrayList<>();
        for (JsonElement item : checks) {
            JsonObject check = item.getAsJsonObject();
            ConnectivityOutcome outcome;
            CandidateMethod method;
            try {
                outcome = ConnectivityOutcome.ofWire(check.get("outcome").getAsString());
                method = check.has("method") ? CandidateMethod.ofWire(check.get("method").getAsString())
                        : CandidateMethod.DEFINED;
            } catch (IllegalArgumentException unknown) {
                // An unrecognized value outside registration, keys and admission drops only its item
                continue;
            }
            AddressFamily family = AddressFamily.of(check.get("family").getAsBigDecimal().intValueExact());
            InetSocketAddress target = null;
            if (check.has("target")) {
                JsonObject endpoint = check.getAsJsonObject("target");
                target = new InetSocketAddress(numeric(endpoint.get("address").getAsString()),
                        endpoint.get("port").getAsBigDecimal().intValueExact());
            }
            parsed.add(new ConnectivityCheck(check.get("region").getAsString(), family, method, target, outcome,
                    check.get("checkedAt").getAsBigDecimal().longValueExact(),
                    check.get("expiresAt").getAsBigDecimal().longValueExact()));
        }
        return Optional.of(new ConnectivityFeedback(revision, List.copyOf(parsed)));
    }

    static String rotationKeyId(JsonObject document) throws IOException {
        try {
            ProviderContract.require("rotationResponse", document);
            return document.get("keyId").getAsString();
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid rotation reply", invalid);
        }
    }

    /** An invalid optional action is ignored rather than failing the operation that carried it. */
    static Optional<OperatorAction> operatorAction(JsonElement value) {
        try {
            if (value == null || !value.isJsonObject()) {
                return Optional.empty();
            }
            JsonObject json = value.getAsJsonObject();
            URI url = operatorUrl(json.get("url"));
            String text = operatorText(json.get("text"));
            long expiresAt = json.get("expiresAt").getAsLong();
            if (url == null || text == null || expiresAt < 0) {
                return Optional.empty();
            }
            return Optional.of(new OperatorAction(url, text, expiresAt));
        } catch (RuntimeException invalid) {
            return Optional.empty();
        }
    }

    /** Reads an error body leniently: each optional member is checked alone, and an invalid one is dropped. */
    static Wire.ErrorReply error(String body) {
        String code = "request_rejected", message = null;
        URI actionUrl = null;
        try {
            JsonObject error = JsonParser.parseString(body).getAsJsonObject();
            message = operatorText(error.get("message"));
            actionUrl = operatorUrl(error.get("actionUrl"));
            JsonElement value = error.get("code");
            if (value != null && value.isJsonPrimitive() && value.getAsString().matches("[a-z0-9_]{1,80}")) {
                code = value.getAsString();
            }
        } catch (RuntimeException ignored) {
            // The status alone then describes the refusal
        }
        return new Wire.ErrorReply(code, message, actionUrl);
    }

    /** Text for the operator, as in an operator action or an error: 1 to 256 characters without control characters. */
    private static String operatorText(JsonElement value) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            return null;
        }
        String text = value.getAsString();
        if (text.isEmpty() || text.codePointCount(0, text.length()) > 256
                || text.chars().anyMatch(Character::isISOControl)) {
            return null;
        }
        return text;
    }

    /** A link for the operator: HTTPS, or HTTP on loopback, without userinfo and at most 2048 characters. */
    private static URI operatorUrl(JsonElement value) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            return null;
        }
        String link = value.getAsString();
        try {
            URI url = new URI(link);
            boolean loopback = "http".equals(url.getScheme())
                    && Set.of("localhost", "127.0.0.1", "[::1]").contains(url.getHost());
            if (link.length() > 2048 || url.getRawUserInfo() != null || url.getHost() == null
                    || !("https".equals(url.getScheme()) || loopback)) {
                return null;
            }
            return url;
        } catch (URISyntaxException invalid) {
            return null;
        }
    }

    // ---- Requests to the provider

    static String register(JsonObject publicKeyJwk, String label) {
        JsonObject request = new JsonObject();
        request.addProperty("protocol", ProviderCrypto.PROTOCOL);
        request.add("publicKeyJwk", publicKeyJwk);
        if (label != null) {
            request.addProperty("label", label);
        }
        return JSON.toJson(request);
    }

    static String recovery(String registrationId) {
        JsonObject recovery = new JsonObject();
        recovery.addProperty("registrationId", registrationId);
        recovery.addProperty("protocol", ProviderCrypto.PROTOCOL);
        return JSON.toJson(recovery);
    }

    static String completion(String challengeId, String proofNonce, String idempotencyKey, String signature) {
        JsonObject completion = new JsonObject();
        completion.addProperty("protocol", ProviderCrypto.PROTOCOL);
        completion.addProperty("challengeId", challengeId);
        completion.addProperty("proofNonce", proofNonce);
        completion.addProperty("idempotencyKey", idempotencyKey);
        completion.addProperty("signature", signature);
        return JSON.toJson(completion);
    }

    /**
     * What a host's connectivity extension reports in a heartbeat.
     *
     * @param probeCandidates Null when they equal the profile's candidates
     */
    record ConnectivityReport(boolean diagnostics, long candidateRevision, List<HostCandidate> probeCandidates,
                              CandidateMethod method) {
    }

    /**
     * One heartbeat body. Optional observations are null when omitted.
     *
     * @param hostProfile         The full profile, or null when sending its revision instead
     * @param sendLocation        Whether to send {@code location}, which is null to clear a declaration
     */
    record HeartbeatRequest(Wire.PublishedProfile hostProfile, String hostProfileRevision,
                            List<String> installedAdmissionKeyIds, String admissionKeyRequestId,
                            boolean acceptingPlayers, Map<String, ProtocolExtension> extensions,
                            ConnectivityReport connectivity, int capacity, ConnectedPlayers connectedPlayers,
                            Boolean gameOutcomes, Software software, boolean sendLocation, Location location,
                            BedrockProtocols bedrockProtocols, ServerStatus serverStatus) {
    }

    static String heartbeat(HeartbeatRequest request) {
        JsonObject body = new JsonObject();
        if (request.hostProfile() != null) {
            body.add("hostProfile", profile(request.hostProfile()));
        } else if (request.hostProfileRevision() != null) {
            body.addProperty("hostProfileRevision", request.hostProfileRevision());
        }
        if (request.installedAdmissionKeyIds() != null) {
            JsonArray installed = new JsonArray();
            request.installedAdmissionKeyIds().forEach(installed::add);
            body.add("installedAdmissionKeyIds", installed);
        }
        if (request.admissionKeyRequestId() != null) {
            body.addProperty("admissionKeyRequestId", request.admissionKeyRequestId());
        }
        body.addProperty("acceptingPlayers", request.acceptingPlayers());
        JsonObject extensions = extensionsJson(request.extensions());
        if (request.connectivity() != null) {
            ConnectivityReport report = request.connectivity();
            JsonObject data = new JsonObject();
            data.addProperty("diagnostics", report.diagnostics());
            data.addProperty("candidateRevision", report.candidateRevision());
            if (report.probeCandidates() != null) {
                data.add("probeCandidates", candidates(report.probeCandidates()));
            }
            data.addProperty("method", report.method().wire());
            JsonObject extension = new JsonObject();
            extension.addProperty("version", 1);
            extension.addProperty("critical", false);
            extension.add("data", data);
            extensions.add(Wire.CONNECTIVITY, extension);
        }
        if (!extensions.isEmpty()) {
            body.add("extensions", extensions);
        }
        ProtocolExtensions.validate(body);
        body.addProperty("capacity", request.capacity());
        JsonObject connected = new JsonObject();
        connected.addProperty("count", request.connectedPlayers().count());
        connected.addProperty("sampledAt", request.connectedPlayers().sampledAt());
        body.add("connectedPlayers", connected);
        if (request.gameOutcomes() != null) {
            body.addProperty("gameOutcomes", request.gameOutcomes() ? "available" : "unavailable");
        }
        if (request.software() != null) {
            JsonObject software = new JsonObject();
            software.addProperty("name", request.software().name());
            software.addProperty("version", request.software().version());
            body.add("software", software);
        }
        if (request.sendLocation()) {
            body.add("location", request.location() == null ? JsonNull.INSTANCE : location(request.location()));
        }
        if (request.bedrockProtocols() != null) {
            JsonArray accepted = new JsonArray();
            request.bedrockProtocols().accepted().forEach(accepted::add);
            JsonObject protocols = new JsonObject();
            protocols.add("accepted", accepted);
            protocols.addProperty("version", request.bedrockProtocols().version());
            body.add("bedrockProtocols", protocols);
        }
        if (request.serverStatus() != null) {
            ServerStatus status = request.serverStatus();
            JsonObject listing = new JsonObject();
            listing.addProperty("name", status.name());
            listing.addProperty("level", status.level());
            listing.addProperty("maxPlayerCount", status.maxPlayerCount());
            listing.addProperty("gameType", status.gameType());
            body.add("serverStatus", listing);
        }
        return JSON.toJson(body);
    }

    private static JsonObject location(Location location) {
        JsonObject json = new JsonObject();
        if (location.country() != null) {
            json.addProperty("country", location.country());
        }
        if (location.city() != null) {
            json.addProperty("city", location.city());
        }
        if (location.latitude() != null) {
            json.addProperty("latitude", location.latitude());
            json.addProperty("longitude", location.longitude());
        }
        return json;
    }

    static JsonObject profile(Wire.PublishedProfile published) {
        HostProfile profile = published.profile();
        JsonObject capability = new JsonObject();
        capability.addProperty("capability", STATELESS_CAPABILITY);
        capability.addProperty("incarnation", profile.incarnation());
        if (!published.assistedFamilies().isEmpty()) {
            JsonArray families = new JsonArray();
            published.assistedFamilies().stream().map(AddressFamily::number).sorted().forEach(families::add);
            capability.add("assistedFamilies", families);
        }
        JsonObject json = new JsonObject();
        json.add("candidates", candidates(profile.candidates()));
        json.add("statelessAdmission", capability);
        json.addProperty("admissionKeyId", profile.admissionKeyId());
        json.addProperty("dtlsFingerprint", profile.dtlsFingerprint().value());
        json.addProperty("maxMessageSize", profile.maxMessageSize());
        json.addProperty("sctpPort", profile.sctpPort());
        return json;
    }

    /**
     * Encodes candidates with RFC 8445 priorities: the type preference first, then a local preference that falls
     * with each earlier candidate of the same type, so the list order ranks candidates within a type only.
     */
    static JsonArray candidates(List<HostCandidate> candidates) {
        JsonArray array = new JsonArray();
        Map<CandidateType, Integer> seen = new EnumMap<>(CandidateType.class);
        int index = 0;
        for (HostCandidate candidate : candidates) {
            int rank = seen.merge(candidate.type(), 1, Integer::sum) - 1;
            JsonObject json = new JsonObject();
            json.addProperty("address", candidate.endpoint().getAddress().getHostAddress());
            json.addProperty("port", candidate.endpoint().getPort());
            json.addProperty("foundation", Integer.toString(++index));
            json.addProperty("priority", priority(candidate.type(), rank));
            json.addProperty("type", candidate.typeName());
            array.add(json);
        }
        return array;
    }

    /** {@code type preference << 24 | local preference << 8 | (256 - component 1)}. */
    static int priority(CandidateType type, int rank) {
        return typePreference(type) << 24 | (65535 - rank) << 8 | 255;
    }

    /** RFC 8445's recommended type preferences (section 5.1.2.2), for the types a host profile publishes. */
    private static int typePreference(CandidateType type) {
        return switch (type) {
            case HOST -> 126;
            case SERVER_REFLEXIVE -> 100;
            case RELAY -> 0;
            case PEER_REFLEXIVE, UNKNOWN -> throw new IllegalArgumentException("No host profile type: " + type);
        };
    }

    static String outcomes(List<Wire.OutcomeEvent> events, boolean endpoints) {
        JsonArray batch = new JsonArray();
        for (Wire.OutcomeEvent event : events) {
            batch.add(outcome(event, endpoints));
        }
        JsonObject body = new JsonObject();
        body.add("events", batch);
        return JSON.toJson(body);
    }

    static JsonObject outcome(Wire.OutcomeEvent event, boolean endpoint) {
        JsonObject json = new JsonObject();
        json.addProperty("ticketId", event.ticketId().value());
        json.addProperty("stage", event.stage().wire());
        json.addProperty("occurredAt", event.occurredAt());
        if (event.reason() != null) {
            json.addProperty("reason", event.reason());
        }
        if (endpoint && event.remote() != null) {
            json.addProperty("remoteAddress", event.remote().getAddress().getHostAddress());
            json.addProperty("remotePort", event.remote().getPort());
        }
        return json;
    }

    static String rotation(JsonObject publicKeyJwk, String proof) {
        JsonObject body = new JsonObject();
        body.add("publicKeyJwk", publicKeyJwk);
        body.addProperty("proof", proof);
        return JSON.toJson(body);
    }

    static String retire(String keyId) {
        JsonObject body = new JsonObject();
        body.addProperty("keyId", keyId);
        return JSON.toJson(body);
    }

    // ---- Shared pieces

    static AdmissionKey admissionKey(JsonObject json) throws IOException {
        String keyId = string(json.get("keyId")), secret = string(json.get("secret"));
        if (!keyId.matches("[A-Z0-9]{4}") || !AdmissionKey.validSecret(secret)) {
            throw new IOException("Invalid admission key");
        }
        return new AdmissionKey(keyId, secret);
    }

    /** Validated optional extensions as typed values whose data stays opaque JSON text. */
    static Map<String, ProtocolExtension> extensions(JsonObject document) {
        Map<String, ProtocolExtension> result = new LinkedHashMap<>();
        for (var entry : ProtocolExtensions.copy(document).entrySet()) {
            JsonObject extension = entry.getValue().getAsJsonObject();
            result.put(entry.getKey(), new ProtocolExtension(extension.get("version").getAsInt(),
                    extension.get("critical").getAsBoolean(), JSON.toJson(extension.get("data"))));
        }
        return Collections.unmodifiableMap(result);
    }

    static JsonObject extensionsJson(Map<String, ProtocolExtension> extensions) {
        JsonObject json = new JsonObject();
        extensions.forEach((namespace, extension) -> {
            JsonObject value = new JsonObject();
            value.addProperty("version", extension.version());
            value.addProperty("critical", extension.critical());
            value.add("data", JsonParser.parseString(extension.data()));
            json.add(namespace, value);
        });
        return json;
    }

    private static long serverTime(JsonObject document) {
        try {
            JsonElement time = document.get("serverTime");
            return time == null ? 0 : time.getAsLong();
        } catch (RuntimeException ignored) {
            return 0;
        }
    }

    private static Set<String> strings(JsonArray array) {
        Set<String> values = new HashSet<>();
        for (JsonElement element : array) {
            if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
                values.add(element.getAsString());
            }
        }
        return Set.copyOf(values);
    }

    private static String string(JsonElement value) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException("Expected a string");
        }
        return value.getAsString();
    }

    private static String optionalString(JsonObject json, String member) {
        JsonElement value = json.get(member);
        return value == null || value.isJsonNull() ? null : string(value);
    }

    private static String nonBlank(JsonElement value) throws IOException {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()
                || value.getAsString().isBlank()) {
            throw new IOException("Invalid public endpoint metadata");
        }
        return value.getAsString();
    }

    private static InetAddress numeric(String address) {
        try {
            return EndpointAddress.parse(address);
        } catch (UnknownHostException invalid) {
            throw new IllegalArgumentException(invalid.getMessage(), invalid);
        }
    }
}

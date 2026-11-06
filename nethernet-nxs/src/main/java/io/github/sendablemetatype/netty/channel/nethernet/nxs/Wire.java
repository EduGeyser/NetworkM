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

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.ProtocolExtension;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient.Readiness;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.AddressFamily;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfile;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.StunServer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** The typed NXS v1 messages the client sends and receives; {@link WireCodec} maps them to their exact JSON. */
final class Wire {
    static final String CONNECTIVITY = "dev.opencollab.nxs.connectivity";
    static final String WEBSOCKET = "dev.opencollab.nxs.websocket";

    private Wire() {
    }

    /**
     * @param heartbeatIntervalMs The check-in interval before the first scheduled reply, 1000 to 30000
     * @param stunServers         From the connectivity extension, at most two
     */
    record Discovery(String provider, Set<String> protocols, Set<String> signatures, Set<String> schemes,
                     long heartbeatIntervalMs, Map<String, ProtocolExtension> extensions,
                     List<StunServer> stunServers) {
        boolean advertises(String namespace) {
            ProtocolExtension extension = extensions.get(namespace);
            return extension != null && extension.version() == 1;
        }
    }

    /** Absent context strings are empty, which is how the digest reads them. */
    record ChallengeContext(String mode, String label, String authorizationId, String serviceId,
                            String registrationId) {
        ChallengeContext {
            Objects.requireNonNull(mode);
            label = Objects.requireNonNullElse(label, "");
            authorizationId = Objects.requireNonNullElse(authorizationId, "");
            serviceId = Objects.requireNonNullElse(serviceId, "");
            registrationId = Objects.requireNonNullElse(registrationId, "");
        }

        Object[] values() {
            return new Object[]{mode, label, authorizationId, serviceId, registrationId};
        }
    }

    /**
     * @param authorizationScheme    Null when the challenge names no authorization
     * @param authorizationReference Null when the challenge names no authorization
     */
    record Challenge(String challengeId, String nonce, String audience, String thumbprint, ChallengeContext context,
                     String contextDigest, long expiresAt, long serverTime, int difficulty,
                     String authorizationScheme, String authorizationReference) {
        /** The completion proof's canonical bytes. */
        String proof(String proofNonce, String idempotencyKey) {
            return ProviderCrypto.array(ProviderCrypto.PROTOCOL, "complete", audience, challengeId, nonce, thumbprint,
                    contextDigest, expiresAt, proofNonce, idempotencyKey);
        }
    }

    /** The registration as saved: secrets, extensions and operator links stay out. */
    record SavedRegistration(String provider, String registrationId, String instanceId, String keyId,
                             long leaseGeneration, String serviceId, String publicAddress) {
        SavedRegistration withKeyId(String keyId) {
            return new SavedRegistration(provider, registrationId, instanceId, keyId, leaseGeneration, serviceId,
                    publicAddress);
        }
    }

    /** @param admissionKey The one-time delivered key, or null */
    record RegistrationReply(SavedRegistration registration, AdmissionKey admissionKey,
                             ProviderClient.OperatorAction operatorAction, Map<String, ProtocolExtension> extensions,
                             long serverTime) {
    }

    record KeyRetirement(String keyId, long retireAfter) {
    }

    /** @param id The request ID the reply answers; keyId the epoch it provisions */
    record KeyRequest(String id, String keyId) {
    }

    /**
     * @param hostProfileRevision Null when the provider holds no profile
     * @param checkIn             Null while the provider has no usable profile to grant a lease for
     * @param keyRequest          Null when the reply answers no key request
     * @param admissionKey        The first delivery of the requested key, or null
     * @param serverTime          The provider's clock, or 0 when the reply does not say
     */
    record HeartbeatReply(String hostProfileRevision, Readiness readiness, List<KeyRetirement> retirements,
                          CheckInSchedule checkIn, KeyRequest keyRequest, AdmissionKey admissionKey,
                          boolean operatorActionAvailable, Map<String, ProtocolExtension> extensions,
                          long serverTime) {
    }

    /** The profile as published: the endpoint's profile plus the families this client assists. */
    record PublishedProfile(HostProfile profile, Set<AddressFamily> assistedFamilies) {
        PublishedProfile {
            assistedFamilies = Set.copyOf(assistedFamilies);
        }
    }

    /**
     * One outcome as queued, saved and sent.
     *
     * @param reason Null unless the stage carries one
     * @param remote Null when unknown
     */
    record OutcomeEvent(TicketId ticketId, OutcomeStage stage, long occurredAt, String reason,
                        InetSocketAddress remote) {
        OutcomeEvent {
            Objects.requireNonNull(ticketId);
            Objects.requireNonNull(stage);
        }
    }

    /** @param actionUrl Null when the error offers none; it never carries a credential */
    record ErrorReply(String code, String message, java.net.URI actionUrl) {
    }
}

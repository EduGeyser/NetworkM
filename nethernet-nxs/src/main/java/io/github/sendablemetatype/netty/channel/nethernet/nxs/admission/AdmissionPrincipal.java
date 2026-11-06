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

import io.netty.util.AttributeKey;

/**
 * What an NXS child channel was admitted as, for the application. Carries no credentials.
 *
 * @param ticketId The correlation ID outcome reports use, including the game's own
 * @param networkId The NetherNet network ID as an unsigned decimal
 * @param keyId The admission key epoch the ticket was issued under
 */
public record AdmissionPrincipal(TicketId ticketId, String networkId, String keyId) {
    public static final AttributeKey<AdmissionPrincipal> KEY =
            AttributeKey.valueOf(AdmissionPrincipal.class, "principal");

    /** Who an admission let in. */
    public static AdmissionPrincipal of(AdmissionContext admission) {
        return new AdmissionPrincipal(admission.ticketId(), admission.networkId(), admission.keyId());
    }
}

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

/**
 * An authenticated join, from a stateless ticket ({@link VerifiedAdmission}) or from the provider's live
 * WebSocket ({@link AssistedAdmission}): what the endpoint needs to create the peer and bind the login.
 */
public sealed interface AdmissionContext permits VerifiedAdmission, AssistedAdmission {
    TicketId ticketId();

    /** The host's ICE ufrag for this join. */
    String localUfrag();

    /** The host's ICE password for this join. */
    String localPassword();

    /** The client's ICE ufrag. */
    String remoteUfrag();

    /** The remote offer to apply, pinned to the client's DTLS fingerprint. */
    String remoteDescription();

    /** Epoch milliseconds, on the clock the admission was checked against, after which no login is accepted. */
    long expiresAt();

    /** The NetherNet network ID as an unsigned decimal; never zero for a player. */
    String networkId();

    String keyId();

    LoginKeyBinding loginBinding();
}

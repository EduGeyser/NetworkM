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

package io.github.sendablemetatype.netty.util.nethernet;

import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.lang.JoseException;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Objects;

/**
 * The identity a client presents in the {@code a=identity} of its offer: a token whose {@code cpk}
 * claim names the client's key, and a signature over the offer's DTLS fingerprints made with that
 * key. A server's {@link OfferCheck} proves the key from it, and attests the token when it trusts
 * the token's issuer. Servers that require a proven key (vanilla BDS, and this library's default)
 * refuse an offer without one.
 * <p>
 * The caller brings the token and the key: a retail client's multiplayer token from Minecraft's
 * authorization service with the key it was issued for, or a token it signs itself, as Education
 * clients do (captured 2026-11-05). Tokens expire, so build one identity per connection.
 *
 * @see <a href="https://github.com/Mojang/bedrock-protocol-docs/blob/7330880ab78ef001cad0b9cdfedb3aa3eaa6d4af/NetherNetOnboardingGuide.md">NetherNet onboarding guide, section 5</a>
 */
public final class ClientIdentity {
    private final AssertionSigner signer;
    private final String token;
    private final String idpDomain;

    private ClientIdentity(AssertionSigner signer, String token, String idpDomain) {
        this.signer = signer;
        this.token = token;
        this.idpDomain = idpDomain;
    }

    /**
     * @param key       the secp384r1 key pair the token names
     * @param token     the compact token; its {@code cpk} claim (base64 SPKI or JWK) must name the key
     * @param idpDomain the envelope's {@code idp.domain}, which no check reads: retail clients send
     *                  Minecraft's authorization service, Education clients an empty string
     * @return the identity
     * @throws GeneralSecurityException if the key is not a matching secp384r1 pair, or the token is
     *                                  not a JWT whose {@code cpk} names the key
     */
    public static ClientIdentity of(KeyPair key, String token, String idpDomain) throws GeneralSecurityException {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(idpDomain, "idpDomain");
        AssertionSigner signer;
        try {
            signer = new AssertionSigner(key, "client identity");
        } catch (IllegalArgumentException e) {
            throw new InvalidKeyException("The client identity's key is not a secp384r1 key", e);
        }
        // A token naming another key fails every server's proof; fail here instead
        byte[] named;
        try {
            JsonWebSignature parsed = new JsonWebSignature();
            parsed.setCompactSerialization(token);
            named = IdentityPublicKey.canonical(IdentityPublicKey.fromClaim(
                    JwtClaims.parse(parsed.getUnverifiedPayload()).getClaimValue("cpk")));
        } catch (JoseException | InvalidJwtException | IllegalArgumentException e) {
            throw new GeneralSecurityException("The client token is not a JWT with a readable cpk claim", e);
        }
        if (!Arrays.equals(named, IdentityPublicKey.canonical(signer.publicKey()))) {
            throw new InvalidKeyException("The client token's cpk names another key than the client identity's");
        }
        return new ClientIdentity(signer, token, idpDomain);
    }

    /**
     * Inserts this identity's assertion into a finished offer, before the first media line. The
     * signature covers only the fingerprint lines, so the offer may carry its candidates already.
     *
     * @param offerSdp the offer without an identity line
     * @return the offer with the identity inserted, line endings kept
     * @throws JoseException            if signing fails
     * @throws IllegalArgumentException if the offer has no well-formed fingerprint line
     */
    public String withAssertion(String offerSdp) throws JoseException {
        return signer.withAssertion(offerSdp, token, idpDomain);
    }
}

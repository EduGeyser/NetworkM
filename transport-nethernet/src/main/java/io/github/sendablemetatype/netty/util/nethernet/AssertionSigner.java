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

import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.List;

/**
 * Signs NetherNet identity assertions with one secp384r1 key: the {@code a=identity} a server puts
 * in its answers and a client in its offers. Both carry a token whose {@code cpk} claim names the
 * key, and a detached ES384 signature over the description's canonical fingerprints, which ties the
 * assertion to the DTLS certificate (onboarding guide, sections 5.1 and 5.2).
 */
final class AssertionSigner {
    private static final String ALG = AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384;

    private final PrivateKey privateKey;
    private final ECPublicKey publicKey;
    private final String owner;

    /**
     * @param pair  the key pair
     * @param owner what the key identifies, for the error message
     * @throws InvalidKeyException      if the private key does not belong to the public key
     * @throws IllegalArgumentException if the public key is not a secp384r1 point
     */
    AssertionSigner(KeyPair pair, String owner) throws GeneralSecurityException {
        this.privateKey = pair.getPrivate();
        this.publicKey = IdentityPublicKey.fromCanonical(IdentityPublicKey.canonical(pair.getPublic()));
        this.owner = owner;
        // A private key that does not belong to the public key signs assertions no peer accepts,
        // which players only see as "not invited"; fail at startup instead.
        boolean matches;
        try {
            JsonWebSignature check = new JsonWebSignature();
            check.setCompactSerialization(sign("{}"));
            check.setKey(publicKey);
            matches = check.verifySignature();
        } catch (JoseException e) {
            throw signingFailed(e);
        }
        if (!matches) {
            throw new InvalidKeyException("The " + owner + "'s private and public keys do not match");
        }
    }

    /** @return the public key, in its canonical form */
    ECPublicKey publicKey() {
        return publicKey;
    }

    /**
     * @return the compact ES384 JWS of the payload
     * @throws GeneralSecurityException if the key cannot sign
     */
    String sign(String payload) throws GeneralSecurityException {
        JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(payload);
        jws.setKey(privateKey);
        jws.setAlgorithmHeaderValue(ALG);
        try {
            return jws.getCompactSerialization();
        } catch (JoseException e) {
            throw signingFailed(e);
        }
    }

    /**
     * jose4j stays an implementation detail: its exception is not passed on, only the JCA failure
     * under it, which names no token contents.
     */
    private GeneralSecurityException signingFailed(JoseException e) {
        GeneralSecurityException failure = new GeneralSecurityException("Cannot sign with the " + owner + "'s key");
        if (e.getCause() instanceof GeneralSecurityException cause) {
            failure.initCause(cause);
        }
        return failure;
    }

    /**
     * Inserts an assertion into a finished description, before the first media line as the guide
     * places it. The signature covers only the fingerprint lines, so later edits to other lines
     * (candidates) keep it valid.
     *
     * @param sdp       the description without an identity line
     * @param token     the token naming this key in its {@code cpk} claim
     * @param idpDomain the envelope's {@code idp.domain}
     * @return the description with the identity inserted, line endings kept
     * @throws GeneralSecurityException if the key cannot sign
     * @throws IllegalArgumentException if the description has no well-formed fingerprint line
     */
    String withAssertion(String sdp, String token, String idpDomain) throws GeneralSecurityException {
        String[] signed = sign(SdpAssertion.localFingerprintJson(sdp)).split("\\.");
        Identity.Assertion assertion = new Identity.Assertion(token, signed[0] + ".." + signed[2]);
        String line = "a=identity:" + new Identity(new Identity.Idp(idpDomain, "default"), assertion).toBase64();
        String eol = sdp.contains("\r\n") ? "\r\n" : "\n";
        String[] lines = sdp.split("\r\n|\n", -1);
        List<String> out = new ArrayList<>(lines.length + 1);
        boolean inserted = false;
        for (String current : lines) {
            if (!inserted && current.startsWith("m=")) {
                out.add(line);
                inserted = true;
            }
            out.add(current);
        }
        if (!inserted) {
            out.add(line);
        }
        return String.join(eol, out);
    }
}

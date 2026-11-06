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

import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.lang.JoseException;

import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECFieldFp;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The one form every NetherNet identity key is compared in: named-curve secp384r1 SPKI DER with an
 * uncompressed, validated public point. Two encodings of one key (a base64 SPKI {@code cpk}, a JWK
 * {@code cpk}, a key from another provider) canonicalize to the same 120 bytes.
 */
public final class IdentityPublicKey {
    private static final ECParameterSpec CURVE;
    private static final byte[] PREFIX =
            HexFormat.of().parseHex("3076301006072a8648ce3d020106052b8104002203620004");
    private static final int LENGTH = 120;

    static {
        try {
            AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
            parameters.init(new ECGenParameterSpec("secp384r1"));
            CURVE = parameters.getParameterSpec(ECParameterSpec.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private IdentityPublicKey() {
    }

    /**
     * @param key the key to canonicalize
     * @return its fixed 120 byte encoding, so that two spellings of one key compare equal
     * @throws IllegalArgumentException if it is not a point on secp384r1
     */
    public static byte[] canonical(PublicKey key) {
        if (!(key instanceof ECPublicKey ec)) {
            throw new IllegalArgumentException("Expected an EC public key");
        }
        ECParameterSpec params = ec.getParams();
        if (params == null || !CURVE.getCurve().equals(params.getCurve())
                || !CURVE.getGenerator().equals(params.getGenerator())
                || !CURVE.getOrder().equals(params.getOrder()) || CURVE.getCofactor() != params.getCofactor()) {
            throw new IllegalArgumentException("Expected secp384r1");
        }
        BigInteger x = ec.getW().getAffineX(), y = ec.getW().getAffineY();
        BigInteger p = ((ECFieldFp) CURVE.getCurve().getField()).getP();
        if (x == null || y == null || x.signum() < 0 || y.signum() < 0 || x.compareTo(p) >= 0 || y.compareTo(p) >= 0
                || !y.multiply(y).mod(p).equals(x.pow(3).add(CURVE.getCurve().getA().multiply(x))
                .add(CURVE.getCurve().getB()).mod(p))) {
            throw new IllegalArgumentException("Invalid secp384r1 point");
        }
        byte[] result = Arrays.copyOf(PREFIX, LENGTH);
        coordinate(x, result, PREFIX.length);
        coordinate(y, result, PREFIX.length + 48);
        return result;
    }

    /**
     * The SHA-256 fingerprint operators publish so plain HTTP players can check the key they are
     * asked to trust (onboarding guide, section 5.2). It is the digest of the canonical SPKI, so
     * {@code openssl ec -in key.pem -pubout -outform DER | openssl dgst -sha256 -c} prints the same.
     *
     * @param key a secp384r1 public key
     * @return lowercase hex byte pairs separated by colons
     * @throws IllegalArgumentException if it is not a point on secp384r1
     */
    public static String fingerprint(PublicKey key) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical(key));
            return HexFormat.ofDelimiter(":").formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Reads a token's {@code cpk} claim in either shape clients send: a base64 X.509 SPKI string
     * (retail 1.26.62 and Education 26.33, captured 2026-11-05) or a public JWK object
     * (reported by go-nethernet for some retail build, never captured by us).
     *
     * @param claim the claim value as the JSON parser produced it
     * @return the key, rebuilt from its canonical encoding
     * @throws IllegalArgumentException if the claim is missing, unreadable, private, or not on secp384r1
     */
    static ECPublicKey fromClaim(Object claim) {
        PublicKey parsed = switch (claim) {
            case String spki -> fromSpki(spki);
            case Map<?, ?> jwk -> fromJwk(jwk);
            case null, default -> throw new IllegalArgumentException("Missing or unreadable client public key");
        };
        return fromCanonical(canonical(parsed));
    }

    /**
     * Encodes a key as the public JWK the server token's {@code cpk} claim carries (onboarding
     * guide, section 5.2). Members are in the lexicographic order RFC 7638 uses, so the claim text
     * stays the same across restarts for one key.
     */
    static Map<String, Object> toJwk(ECPublicKey key) {
        Map<String, Object> params = new EllipticCurveJsonWebKey(fromCanonical(canonical(key)))
                .toParams(JsonWebKey.OutputControlLevel.PUBLIC_ONLY);
        Map<String, Object> jwk = new LinkedHashMap<>();
        for (String member : new String[]{"crv", "kty", "x", "y"}) {
            jwk.put(member, params.get(member));
        }
        return jwk;
    }

    /** Which shape a {@code cpk} claim had, for diagnostics; null when it had neither. */
    static OfferCheckResult.CpkEncoding encodingOf(Object claim) {
        return switch (claim) {
            case String ignored -> OfferCheckResult.CpkEncoding.SPKI;
            case Map<?, ?> ignored -> OfferCheckResult.CpkEncoding.JWK;
            case null, default -> null;
        };
    }

    static ECPublicKey fromCanonical(byte[] canonical) {
        try {
            return (ECPublicKey) KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(canonical));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Invalid canonical key", e);
        }
    }

    private static PublicKey fromSpki(String encoded) {
        try {
            return KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(encoded)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Unreadable SPKI client public key", e);
        }
    }

    private static PublicKey fromJwk(Map<?, ?> jwk) {
        // A private member in a token is a client bug at best; never treat it as a key to use.
        if (jwk.containsKey("d") || !"EC".equals(jwk.get("kty"))) {
            throw new IllegalArgumentException("Expected a public EC JWK");
        }
        Map<String, Object> params = new LinkedHashMap<>();
        jwk.forEach((name, value) -> params.put(String.valueOf(name), value));
        try {
            if (JsonWebKey.Factory.newJwk(params).getKey() instanceof PublicKey key) {
                return key;
            }
        } catch (JoseException | RuntimeException e) {
            throw new IllegalArgumentException("Unreadable JWK client public key", e);
        }
        throw new IllegalArgumentException("Expected a public EC JWK");
    }

    private static void coordinate(BigInteger value, byte[] target, int offset) {
        byte[] bytes = value.toByteArray();
        int length = Math.min(48, bytes.length);
        System.arraycopy(bytes, bytes.length - length, target, offset + 48 - length, length);
    }
}

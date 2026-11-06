package io.github.sendablemetatype.netty.util.nethernet;

import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;

import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Offers shaped like the ones captured on 2026-11-05 (retail 1.26.62 and Education 26.33 over
 * HTTP), signed with throwaway test keys. No real token is used.
 */
public final class OfferFixtures {
    public static final String DIGEST = "AB:".repeat(31) + "AB";
    public static final String OTHER_DIGEST = "CD:".repeat(31) + "CD";
    public static final String RETAIL_IDP = "https://authorization.franchise.minecraft-services.net/";
    public static final String AUTH_KEY_ID = "test-auth-key";
    /** Stands in for Minecraft's authorization service key, served through a test JWKS or pinned. */
    public static final KeyPair AUTH_KEY = keyPair("RSA", null);
    public static final KeyPair CLIENT_KEY = keyPair("EC", "secp384r1");

    private OfferFixtures() {
    }

    public static KeyPair keyPair(String algorithm, String curve) {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance(algorithm);
            if (curve != null) generator.initialize(new ECGenParameterSpec(curve));
            else generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String spki(PublicKey key) {
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    /** The claims of a retail multiplayer token, as captured: Minecraft-issued, four hours, SPKI {@code cpk}. */
    public static JwtClaims retailClaims(PublicKey clientKey) {
        Instant now = Instant.now();
        JwtClaims claims = new JwtClaims();
        claims.setIssuer(TrustedIssuer.MINECRAFT_ISSUER);
        claims.setAudience(TrustedIssuer.MINECRAFT_AUDIENCE);
        claims.setSubject("test-subject");
        claims.setIssuedAt(NumericDate.fromSeconds(now.getEpochSecond()));
        claims.setExpirationTime(NumericDate.fromSeconds(now.plus(Duration.ofHours(4)).getEpochSecond()));
        claims.setClaim("xid", "2535400000000000");
        claims.setClaim("xname", "TestPlayer");
        claims.setClaim("cpk", spki(clientKey));
        return claims;
    }

    /** RS256 with a {@code kid}, signed by the stand-in authorization key. */
    public static String retailToken(JwtClaims claims) {
        return sign(claims.toJson(), AUTH_KEY.getPrivate(), AlgorithmIdentifiers.RSA_USING_SHA256, Map.of("kid", AUTH_KEY_ID));
    }

    /**
     * The claims of an Education 26.33 token, as captured: about a year long, no {@code iss},
     * {@code iat} or {@code sub}, empty player ids, SPKI {@code cpk}.
     */
    public static JwtClaims educationClaims(PublicKey clientKey) {
        JwtClaims claims = new JwtClaims();
        claims.setAudience(TrustedIssuer.MINECRAFT_AUDIENCE);
        claims.setExpirationTime(NumericDate.fromSeconds(Instant.now().plus(Duration.ofDays(365)).getEpochSecond()));
        claims.setClaim("leguuid", "00000000-0000-0000-0000-000000000000");
        claims.setClaim("mid", "test-mid");
        claims.setClaim("xid", "");
        claims.setClaim("xname", "");
        claims.setClaim("nid", "");
        claims.setClaim("pid", "");
        claims.setClaim("cpk", spki(clientKey));
        return claims;
    }

    /** Self-signed ES384 with the header {@code x5u} equal to {@code cpk}, as Education signs it. */
    public static String educationToken(JwtClaims claims, KeyPair client) {
        return sign(claims.toJson(), client.getPrivate(), AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384,
                Map.of("x5u", spki(client.getPublic())));
    }

    public static String sign(String payload, Key key, String algorithm, Map<String, String> headers) {
        try {
            JsonWebSignature signature = new JsonWebSignature();
            signature.setAlgorithmHeaderValue(algorithm);
            headers.forEach(signature::setHeader);
            signature.setKey(key);
            signature.setDoKeyValidation(false);
            signature.setPayload(payload);
            return signature.getCompactSerialization();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** The canonical fingerprint JSON for the given digests, all sha-256. */
    public static String canonical(String... digests) {
        return Stream.of(digests).map(digest -> "{\"algorithm\":\"sha-256\",\"digest\":\"" + digest + "\"}")
                .collect(Collectors.joining(",", "{\"fingerprint\":[", "]}"));
    }

    /** A detached ES384 signature over the canonical fingerprints of the given digests. */
    public static String proof(Key key, String... digests) {
        return detached(sign(canonical(digests), key, AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384, Map.of()));
    }

    public static String detached(String compact) {
        String[] parts = compact.split("\\.", -1);
        return parts[0] + ".." + parts[2];
    }

    public static String identity(String idpDomain, String token, String proof) {
        return new Identity(new Identity.Idp(idpDomain, "default"), new Identity.Assertion(token, proof)).toBase64();
    }

    /**
     * An HTTP offer in the captured shape: one session-level {@code a=identity}, a host candidate,
     * {@code a=setup:actpass} and {@code a=max-message-size:262144}, CRLF line endings.
     */
    public static String offer(String identity, String... digests) {
        StringBuilder sdp = new StringBuilder()
                .append("v=0\r\n")
                .append("o=- 4611731400430051336 2 IN IP4 127.0.0.1\r\n")
                .append("s=-\r\n")
                .append("t=0 0\r\n")
                .append("a=group:BUNDLE 0\r\n")
                .append("a=extmap-allow-mixed\r\n")
                .append("a=msid-semantic: WMS\r\n");
        if (identity != null) {
            sdp.append("a=identity:").append(identity).append("\r\n");
        }
        sdp.append("m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n")
                .append("c=IN IP4 0.0.0.0\r\n")
                .append("a=candidate:1 1 udp 2122260223 192.168.1.20 51234 typ host generation 0 network-id 1\r\n")
                .append("a=ice-ufrag:Xq3r\r\n")
                .append("a=ice-pwd:tq8wQZ3m8uA0v1o3cXr9nB2k\r\n")
                .append("a=ice-options:trickle\r\n");
        for (String digest : digests) {
            sdp.append("a=fingerprint:sha-256 ").append(digest).append("\r\n");
        }
        return sdp.append("a=setup:actpass\r\n")
                .append("a=mid:0\r\n")
                .append("a=sctp-port:5000\r\n")
                .append("a=max-message-size:262144\r\n")
                .toString();
    }

    public static String retailOffer() {
        return offer(identity(RETAIL_IDP, retailToken(retailClaims(CLIENT_KEY.getPublic())),
                proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST);
    }

    public static String educationOffer() {
        return offer(identity("", educationToken(educationClaims(CLIENT_KEY.getPublic()), CLIENT_KEY),
                proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST);
    }

    /** Attests the stand-in authorization key's tokens without a network fetch. */
    public static OfferCheck retailCheck() {
        return new OfferCheck(List.of(TrustedIssuer.fixedKey(AUTH_KEY.getPublic(),
                TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE)));
    }

    /** The stand-in authorization key as a JWKS document. */
    public static String keySet() {
        RsaJsonWebKey jwk = new RsaJsonWebKey((RSAPublicKey) AUTH_KEY.getPublic());
        jwk.setKeyId(AUTH_KEY_ID);
        jwk.setUse("sig");
        jwk.setAlgorithm(AlgorithmIdentifiers.RSA_USING_SHA256);
        return "{\"keys\":[" + jwk.toJson(JsonWebKey.OutputControlLevel.PUBLIC_ONLY) + "]}";
    }
}

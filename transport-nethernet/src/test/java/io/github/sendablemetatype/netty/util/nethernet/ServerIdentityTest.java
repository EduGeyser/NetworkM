package io.github.sendablemetatype.netty.util.nethernet;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Base64;
import java.util.List;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.keyPair;
import static org.junit.jupiter.api.Assertions.*;

class ServerIdentityTest {
    private static final String DIGEST = "FA:1F:B9:DF:DD:42:63:3B:E0:AB:2D:09:01:7A:3C:7C:49:60:0A:10:84:77:7C:6D:69:F5:42:63:FC:E6:53:7F";
    private static final String ANSWER = "v=0\r\n"
            + "o=- 987 2 IN IP4 127.0.0.1\r\n"
            + "s=-\r\n"
            + "t=0 0\r\n"
            + "a=group:BUNDLE 0\r\n"
            + "m=application 56241 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "c=IN IP4 192.168.1.112\r\n"
            + "a=fingerprint:sha-256 " + DIGEST + "\r\n"
            + "a=setup:active\r\n"
            + "a=sctp-port:5000\r\n";

    @Test
    void answerAssertionVerifiesTheWayTheClientChecksIt() throws Exception {
        KeyPair key = keyPair("EC", "secp384r1");
        String answer = ServerIdentity.of(key, "play.example.test").augmentAnswer(ANSWER);

        int identity = answer.indexOf("a=identity:");
        assertTrue(identity >= 0 && identity < answer.indexOf("m=application"), "a session-level attribute");
        assertTrue(answer.endsWith("a=sctp-port:5000\r\n"), "line endings kept");

        // The client's flow from the guide, section 5.2
        String value = answer.substring(identity + "a=identity:".length(), answer.indexOf("\r\n", identity));
        JsonObject envelope = JsonParser.parseString(new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8))
                .getAsJsonObject();
        assertEquals("play.example.test", envelope.getAsJsonObject("idp").get("domain").getAsString());
        assertEquals("default", envelope.getAsJsonObject("idp").get("protocol").getAsString());
        JsonObject assertion = JsonParser.parseString(envelope.get("assertion").getAsString()).getAsJsonObject();

        JsonWebSignature token = new JsonWebSignature();
        token.setCompactSerialization(assertion.get("token").getAsString());
        JwtClaims claims = JwtClaims.parse(token.getUnverifiedPayload());
        // cpk is a JWK object, as the guide specifies
        JsonObject cpk = JsonParser.parseString(token.getUnverifiedPayload()).getAsJsonObject().getAsJsonObject("cpk");
        assertEquals(List.of("crv", "kty", "x", "y"), List.copyOf(cpk.keySet()));
        PublicKey pinned = assertInstanceOf(PublicKey.class, JsonWebKey.Factory.newJwk(cpk.toString()).getKey());
        assertArrayEquals(IdentityPublicKey.canonical(key.getPublic()), IdentityPublicKey.canonical(pinned));
        token.setKey(pinned);
        assertEquals(AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384, token.getAlgorithmHeaderValue());
        assertTrue(token.verifySignature(), "self-signed by cpk");
        assertEquals("play.example.test", claims.getIssuer());
        assertNotNull(claims.getIssuedAt());

        JsonWebSignature fingerprints = new JsonWebSignature();
        fingerprints.setCompactSerialization(assertion.get("fingerprints").getAsString());
        fingerprints.setPayload("{\"fingerprint\":[{\"algorithm\":\"sha-256\",\"digest\":\"" + DIGEST + "\"}]}");
        fingerprints.setKey(pinned);
        assertTrue(fingerprints.verifySignature(), "signs the answer's canonical fingerprints");
    }

    @Test
    void ourOwnOfferCheckReadsTheJwkKey() throws Exception {
        // The same envelope and proof format in the other direction, so the check must prove it
        KeyPair key = keyPair("EC", "secp384r1");
        String asOffer = ServerIdentity.of(key, "self").augmentAnswer(ANSWER);
        OfferCheckResult result = new OfferCheck(List.of()).check(asOffer);

        assertEquals(OfferCheckResult.Level.KEY_PROVEN, result.level());
        assertEquals(OfferCheckResult.CpkEncoding.JWK, result.diagnostics().cpkEncoding());
        assertArrayEquals(IdentityPublicKey.canonical(key.getPublic()), result.clientKey().getEncoded());
    }

    @Test
    void mismatchedKeysAreRejectedAtConstruction() {
        KeyPair first = keyPair("EC", "secp384r1");
        KeyPair second = keyPair("EC", "secp384r1");

        GeneralSecurityException failure = assertThrows(GeneralSecurityException.class,
                () -> ServerIdentity.of(new KeyPair(second.getPublic(), first.getPrivate()), "self"));
        assertTrue(failure.getMessage().contains("do not match"), failure.getMessage());
    }

    @Test
    void aKeyThatCannotSignFailsWithoutExposingJose4j() {
        // A P-256 private key cannot make the ES384 signature the identity needs.
        KeyPair wrongCurve = new KeyPair(keyPair("EC", "secp384r1").getPublic(),
                keyPair("EC", "secp256r1").getPrivate());

        GeneralSecurityException failure = assertThrows(GeneralSecurityException.class,
                () -> ServerIdentity.of(wrongCurve, "self"));
        assertEquals("Cannot sign with the server identity's key", failure.getMessage());
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            assertFalse(cause.getClass().getName().startsWith("org.jose4j."), cause.toString());
        }
    }

    @Test
    void answerWithoutFingerprintsCannotBeSigned() {
        ServerIdentity identity = ServerIdentity.ephemeral("self");

        assertThrows(IllegalArgumentException.class, () -> identity.augmentAnswer("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"));
    }

    @Test
    void ephemeralIdentitiesNeverShareAKey() {
        assertNotEquals(ServerIdentity.ephemeral("self").fingerprint(), ServerIdentity.ephemeral("self").fingerprint());
    }
}

package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.util.nethernet.ServerTrust.Refusal;
import io.github.sendablemetatype.netty.util.nethernet.ServerTrust.Refused;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.junit.jupiter.api.Test;

import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.DIGEST;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.OTHER_DIGEST;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.identity;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.keyPair;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.offer;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.proof;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.sign;
import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.spki;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerTrustTest {
    private static final String ES384 = AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384;
    private static final String BARE_ANSWER = offer(null, DIGEST);

    private final KeyPair server = keyPair("EC", "secp384r1");

    @Test
    void anAnswerSignedByAPinnedServerIdentityIsAccepted() throws Exception {
        String answer = ServerIdentity.of(server, "self").augmentAnswer(BARE_ANSWER);
        assertDoesNotThrow(() -> ServerTrust.pinnedTo(server.getPublic()).verify(answer));
        // Across a rotation both keys are pinned.
        assertDoesNotThrow(() -> ServerTrust.pinnedTo(keyPair("EC", "secp384r1").getPublic(), server.getPublic())
                .verify(answer));
    }

    @Test
    void aPinnedKeyMatchesInAnyEncoding() throws Exception {
        PublicKey reencoded = KeyFactory.getInstance("EC")
                .generatePublic(new X509EncodedKeySpec(server.getPublic().getEncoded()));
        String answer = ServerIdentity.of(server, "self").augmentAnswer(BARE_ANSWER);
        assertDoesNotThrow(() -> ServerTrust.pinnedTo(List.of(reencoded)).verify(answer));
    }

    @Test
    void aSelfSignedTokenWithAnSpkiKeyIsAcceptedToo() {
        String token = sign(claims(spki(server.getPublic()), null).toJson(), server.getPrivate(), ES384, Map.of());
        String answer = offer(identity("self", token, proof(server.getPrivate(), DIGEST)), DIGEST);
        assertDoesNotThrow(() -> ServerTrust.pinnedTo(server.getPublic()).verify(answer));
    }

    @Test
    void anAnswerWithoutAnIdentityIsRefusedAsMissing() {
        Refused refused = assertThrows(Refused.class, () -> ServerTrust.pinnedTo(server.getPublic()).verify(BARE_ANSWER));
        assertEquals(Refusal.MISSING, refused.refusal());
        assertEquals("The server's answer carries no identity", refused.getMessage());
    }

    @Test
    void aValidIdentityUnderAnotherKeyIsRefusedWithItsFingerprint() throws Exception {
        KeyPair other = keyPair("EC", "secp384r1");
        String answer = ServerIdentity.of(other, "self").augmentAnswer(BARE_ANSWER);
        Refused refused = assertThrows(Refused.class, () -> ServerTrust.pinnedTo(server.getPublic()).verify(answer));
        assertEquals(Refusal.NOT_PINNED, refused.refusal());
        assertEquals("The server's key is not pinned (SHA-256 " + IdentityPublicKey.fingerprint(other.getPublic()) + ")",
                refused.getMessage());
    }

    @Test
    void anIdentityOverOtherFingerprintsIsRefusedAsInvalid() throws Exception {
        // A pinned server's identity replayed in front of another DTLS certificate.
        String replayed = ServerIdentity.of(server, "self").augmentAnswer(BARE_ANSWER).replace(DIGEST, OTHER_DIGEST);
        assertInvalid(replayed, "its signature over the answer's fingerprints does not verify");
    }

    @Test
    void aTokenSignedByAnotherKeyThanItNamesIsRefusedAsInvalid() {
        KeyPair other = keyPair("EC", "secp384r1");
        String token = sign(claims(IdentityPublicKey.toJwk((ECPublicKey) server.getPublic()), null)
                .toJson(), other.getPrivate(), ES384, Map.of());
        String answer = offer(identity("self", token, proof(server.getPrivate(), DIGEST)), DIGEST);
        String message = assertInvalid(answer, "its token is not signed with the key it names");
        assertFalse(message.contains(token));
    }

    @Test
    void anExpiredTokenIsRefusedAsInvalid() {
        long anHourAgo = Instant.now().minusSeconds(3600).getEpochSecond();
        String token = sign(claims(spki(server.getPublic()), anHourAgo).toJson(), server.getPrivate(), ES384, Map.of());
        assertInvalid(offer(identity("self", token, proof(server.getPrivate(), DIGEST)), DIGEST), "its token has expired");
    }

    @Test
    void onlySecp384r1KeysCanBePinned() {
        assertThrows(IllegalArgumentException.class, () -> ServerTrust.pinnedTo(List.of()));
        assertThrows(IllegalArgumentException.class, () -> ServerTrust.pinnedTo(keyPair("EC", "secp256r1").getPublic()));
    }

    private String assertInvalid(String answer, String why) {
        Refused refused = assertThrows(Refused.class, () -> ServerTrust.pinnedTo(server.getPublic()).verify(answer));
        assertEquals(Refusal.INVALID, refused.refusal());
        assertEquals("The server's answer identity does not verify: " + why, refused.getMessage());
        return refused.getMessage();
    }

    private static JwtClaims claims(Object cpk, Long expiry) {
        JwtClaims claims = new JwtClaims();
        claims.setClaim("cpk", cpk);
        claims.setIssuedAtToNow();
        if (expiry != null) {
            claims.setExpirationTime(NumericDate.fromSeconds(expiry));
        }
        return claims;
    }
}

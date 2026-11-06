package io.github.sendablemetatype.netty.util.nethernet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

class ClientIdentityTest {
    /** An offer as the engine creates it, without an identity. */
    private static final String OFFER = offer(null, DIGEST);
    /** Stands for what a bearer token carries. */
    private static final String SECRET = "sentinel-7f3a91";

    @Test
    void anOfferWithASelfSignedTokenProvesItsKey() throws Exception {
        ClientIdentity identity = ClientIdentity.of(CLIENT_KEY,
                educationToken(educationClaims(CLIENT_KEY.getPublic()), CLIENT_KEY), "");

        String signed = identity.withAssertion(OFFER);
        OfferCheckResult result = new OfferCheck(List.of()).check(signed);

        assertEquals(OfferCheckResult.Level.KEY_PROVEN, result.level());
        assertArrayEquals(IdentityPublicKey.canonical(CLIENT_KEY.getPublic()), result.clientKey().getEncoded());
        assertTrue(signed.indexOf("a=identity:") < signed.indexOf("m=application"), "a session-level attribute");
    }

    @Test
    void anOfferWithAnIssuedTokenIsAttested() throws Exception {
        ClientIdentity identity = ClientIdentity.of(CLIENT_KEY, retailToken(retailClaims(CLIENT_KEY.getPublic())),
                RETAIL_IDP);

        OfferCheckResult result = retailCheck().check(identity.withAssertion(OFFER));

        assertEquals(OfferCheckResult.Level.ATTESTED, result.level());
        assertEquals(RETAIL_IDP, result.diagnostics().idpDomain());
    }

    @Test
    void aTokenNamingAnotherKeyIsRefused() {
        KeyPair other = keyPair("EC", "secp384r1");

        assertThrows(InvalidKeyException.class,
                () -> ClientIdentity.of(CLIENT_KEY, retailToken(retailClaims(other.getPublic())), RETAIL_IDP));
    }

    @Test
    void mismatchedKeysAndUnreadableTokensAreRefused() {
        KeyPair other = keyPair("EC", "secp384r1");
        String token = retailToken(retailClaims(CLIENT_KEY.getPublic()));

        GeneralSecurityException mismatch = assertThrows(GeneralSecurityException.class,
                () -> ClientIdentity.of(new KeyPair(CLIENT_KEY.getPublic(), other.getPrivate()), token, ""));
        assertTrue(mismatch.getMessage().contains("do not match"), mismatch.getMessage());
        assertThrows(GeneralSecurityException.class, () -> ClientIdentity.of(CLIENT_KEY, "not a token", ""));
    }

    @ParameterizedTest
    @MethodSource("unreadableTokens")
    void anUnreadableTokenNeverReachesTheExceptionText(String token) {
        GeneralSecurityException failure = assertThrows(GeneralSecurityException.class,
                () -> ClientIdentity.of(CLIENT_KEY, token, ""));

        StringWriter trace = new StringWriter();
        failure.printStackTrace(new PrintWriter(trace));
        assertFalse(trace.toString().contains(SECRET), trace.toString());
    }

    private static Stream<String> unreadableTokens() {
        return Stream.of(
                compact("{\"alg\":\"ES384\"}", "{\"xuid\":\"" + SECRET + "\""),
                compact("{\"alg\":\"ES384\"} \"" + SECRET + "\"", "{}"),
                compact("{\"alg\":\"ES384\"}",
                        "{\"cpk\":{\"kty\":\"EC\",\"crv\":\"" + SECRET + "\",\"x\":\"AA\",\"y\":\"AA\"}}"));
    }

    private static String compact(String header, String payload) {
        Base64.Encoder url = Base64.getUrlEncoder().withoutPadding();
        return url.encodeToString(header.getBytes(StandardCharsets.UTF_8)) + "."
                + url.encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".c2ln";
    }
}

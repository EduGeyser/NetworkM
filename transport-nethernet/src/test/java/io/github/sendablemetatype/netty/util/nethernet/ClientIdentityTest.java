package io.github.sendablemetatype.netty.util.nethernet;

import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.util.List;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

class ClientIdentityTest {
    /** An offer as the engine creates it, without an identity. */
    private static final String OFFER = offer(null, DIGEST);

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
}

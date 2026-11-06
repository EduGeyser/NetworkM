package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Reason;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwa.AlgorithmConstraints.ConstraintType;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.lang.JoseException;

import java.io.Serial;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * The server identities a client channel accepts in answers, set with
 * {@code NetherChannelOption.NETHER_CLIENT_SERVER_TRUST}: pinned secp384r1 keys, as a client pins
 * the key of a server it knows.
 * <p>
 * An answer's {@code a=identity} is checked the way the retail client checks it (onboarding guide,
 * section 5.2, "What the Client Does With It"), with the same parser and proof as a server's
 * {@link OfferCheck}: its token names a key in {@code cpk} (a JWK, or base64 SPKI), is self-signed
 * with that key and is not expired when it carries an expiry; the key signed the answer's DTLS
 * fingerprints; and the key is pinned. The DTLS handshake then holds the server to those
 * fingerprints, so only the holder of a pinned key completes the connection.
 */
public final class ServerTrust {
    private static final String ALGORITHM = AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384;

    private final List<byte[]> pinned;

    private ServerTrust(List<byte[]> pinned) {
        this.pinned = pinned;
    }

    /**
     * @param keys the server keys to accept, more than one across a rotation; each in any encoding
     *             of a secp384r1 public key, such as {@link ServerIdentity#publicKey()}
     * @return the trust
     * @throws IllegalArgumentException if no key is given or a key is not a secp384r1 public key
     */
    public static ServerTrust pinnedTo(PublicKey... keys) {
        return pinnedTo(List.of(keys));
    }

    /** As {@link #pinnedTo(PublicKey...)}. */
    public static ServerTrust pinnedTo(Collection<? extends PublicKey> keys) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("No server key to pin");
        }
        return new ServerTrust(keys.stream().map(key -> IdentityPublicKey.canonical(Objects.requireNonNull(key, "key")))
                .toList());
    }

    /**
     * Checks the identity a server answered with. Needs no network, so it may run on an event loop.
     *
     * @param answerSdp the answer as signaled, identity line included
     * @throws Refused if the answer carries no identity, one that does not verify, or a key that is
     *                 not pinned; its message never quotes the token
     */
    public void verify(String answerSdp) throws Refused {
        OfferCheck.Proven proven;
        try {
            proven = OfferCheck.prove(answerSdp);
        } catch (SdpAssertion.Stop stop) {
            throw stop.reason == Reason.NO_IDENTITY
                    ? new Refused(Refusal.MISSING, "The server's answer carries no identity")
                    : invalid(describe(stop.reason));
        }
        if (!selfSigned(proven)) {
            throw invalid("its token is not signed with the key it names");
        }
        if (expired(proven)) {
            throw invalid("its token has expired");
        }
        byte[] key = IdentityPublicKey.canonical(proven.key());
        if (pinned.stream().noneMatch(candidate -> MessageDigest.isEqual(candidate, key))) {
            throw new Refused(Refusal.NOT_PINNED, "The server's key is not pinned (SHA-256 "
                    + IdentityPublicKey.fingerprint(proven.key()) + ")");
        }
    }

    private static boolean selfSigned(OfferCheck.Proven proven) {
        try {
            JsonWebSignature token = proven.token();
            token.setAlgorithmConstraints(new AlgorithmConstraints(ConstraintType.PERMIT, ALGORITHM));
            token.setKey(proven.key());
            return token.verifySignature();
        } catch (JoseException | RuntimeException e) {
            return false;
        }
    }

    // The guide: the client rejects clearly expired tokens; a server token may carry no expiry.
    private static boolean expired(OfferCheck.Proven proven) throws Refused {
        try {
            NumericDate expiry = proven.claims().getExpirationTime();
            return expiry != null && NumericDate.now().getValue() > expiry.getValue() + OfferCheck.CLOCK_SKEW_SECONDS;
        } catch (MalformedClaimException e) {
            throw invalid("its token's expiry is not a number");
        }
    }

    private static String describe(Reason reason) {
        return switch (reason) {
            case MALFORMED_SDP -> "the answer, its identity line or its fingerprint lines are malformed";
            case MALFORMED_ASSERTION -> "its envelope cannot be read";
            case MALFORMED_TOKEN -> "its token is not a readable JWT";
            case INVALID_CLIENT_KEY -> "its token's cpk is not a secp384r1 public key";
            case PROOF_INVALID -> "its signature over the answer's fingerprints does not verify";
            default -> reason.name();
        };
    }

    private static Refused invalid(String why) {
        return new Refused(Refusal.INVALID, "The server's answer identity does not verify: " + why);
    }

    /** Why an answer's identity was refused. */
    public enum Refusal {
        /** The answer carries no {@code a=identity}. */
        MISSING,
        /** The identity is malformed, its signatures do not verify, or its token has expired. */
        INVALID,
        /** The identity verifies, but under a key this trust does not pin. */
        NOT_PINNED
    }

    /** An answer identity a {@link ServerTrust} refused. */
    public static final class Refused extends GeneralSecurityException {
        @Serial
        private static final long serialVersionUID = 1L;

        private final Refusal refusal;

        Refused(Refusal refusal, String message) {
            super(message);
            this.refusal = refusal;
        }

        /** @return why the identity was refused */
        public Refusal refusal() {
            return refusal;
        }
    }
}

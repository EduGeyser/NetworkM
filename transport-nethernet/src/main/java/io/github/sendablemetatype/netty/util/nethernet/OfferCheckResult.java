package io.github.sendablemetatype.netty.util.nethernet;

import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What {@link OfferCheck} established about the client identity in one offer.
 *
 * @param level       how far the check got
 * @param clientKey   the key the offer proved it holds, canonical secp384r1; null below
 *                    {@link Level#KEY_PROVEN}
 * @param claims      an immutable snapshot of the offer token's claims, nested values included;
 *                    attested by the issuer only at {@link Level#ATTESTED}, otherwise asserted by
 *                    the client alone; empty below {@link Level#KEY_PROVEN}
 * @param reason      why the check stopped below {@link Level#ATTESTED}; null at that level
 * @param diagnostics non-secret facts about the offer for logs
 */
public record OfferCheckResult(Level level, ECPublicKey clientKey, Map<String, Object> claims, Reason reason,
                               Diagnostics diagnostics) {

    /** How far an offer check got, in the order the steps run. */
    public enum Level {
        /** The offer carries no {@code a=identity}. */
        NO_ASSERTION,
        /** The assertion, its token or its key cannot be read. */
        MALFORMED,
        /** The fingerprint signature does not verify under the token's key. */
        PROOF_FAILED,
        /** The client holds the token's key and signed this offer's fingerprints; the token itself is not attested. */
        KEY_PROVEN,
        /** As {@link #KEY_PROVEN}, and a trusted issuer signed the token. */
        ATTESTED;

        /** @return whether the client proved it holds the token's key */
        public boolean keyProven() {
            return this == KEY_PROVEN || this == ATTESTED;
        }
    }

    /** Why a check stopped where it did. Each reason belongs to one level. */
    public enum Reason {
        /** {@link Level#NO_ASSERTION}: no {@code a=identity} line. */
        NO_IDENTITY(Level.NO_ASSERTION),
        /** {@link Level#MALFORMED}: the SDP is oversized, has a malformed line, a misplaced or repeated identity, or no usable fingerprint. */
        MALFORMED_SDP(Level.MALFORMED),
        /** {@link Level#MALFORMED}: the identity envelope or its assertion cannot be decoded. */
        MALFORMED_ASSERTION(Level.MALFORMED),
        /** {@link Level#MALFORMED}: the token is not a readable JWS with JSON claims. */
        MALFORMED_TOKEN(Level.MALFORMED),
        /** {@link Level#MALFORMED}: {@code cpk} is missing, unreadable, private or not on P-384. */
        INVALID_CLIENT_KEY(Level.MALFORMED),
        /** {@link Level#PROOF_FAILED}: the fingerprint signature is not a valid ES384 signature by {@code cpk}. */
        PROOF_INVALID(Level.PROOF_FAILED),
        /** {@link Level#KEY_PROVEN}: the token names no issuer, as Education's self-signed tokens do. */
        NO_ISSUER(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: no trusted issuer is configured for the token's {@code iss}. */
        UNKNOWN_ISSUER(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the token is not addressed to the issuer's expected audience. */
        WRONG_AUDIENCE(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the token is not signed with an RSA or ECDSA algorithm. */
        ALGORITHM_NOT_ALLOWED(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: a required claim ({@code exp}, {@code sub}) is missing or has the wrong type. */
        INVALID_CLAIMS(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the token has expired. */
        EXPIRED(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the token is not valid yet. */
        NOT_YET_VALID(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the issuer has no key matching the token's header. */
        UNKNOWN_KEY(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the issuer's key does not verify the token. */
        BAD_SIGNATURE(Level.KEY_PROVEN),
        /** {@link Level#KEY_PROVEN}: the issuer's keys could not be fetched or read, and none are cached. */
        TRUST_SOURCE_UNREACHABLE(Level.KEY_PROVEN);

        private final Level level;

        Reason(Level level) {
            this.level = level;
        }

        /** @return the level a check that stops for this reason reaches */
        public Level level() {
            return level;
        }
    }

    /** The shape of the token's {@code cpk} claim. */
    public enum CpkEncoding {
        /** A base64 X.509 SubjectPublicKeyInfo string, as retail 1.26.62 and Education 26.33 send. */
        SPKI,
        /** A public JSON Web Key object. */
        JWK
    }

    /**
     * Non-secret facts about an offer, for logs. Every value comes from the client unverified; text
     * is stored with control characters removed and cut to a bounded length, so it is safe to log.
     * Each value is null when the check stopped before reading it.
     *
     * @param idpDomain   the envelope's {@code idp.domain}; empty on Education 26.33
     * @param idpProtocol the envelope's {@code idp.protocol}
     * @param algorithm   the token's {@code alg} header
     * @param keyId       the token's {@code kid} header
     * @param issuer      the token's {@code iss} claim
     * @param audience    the token's {@code aud} claim, empty when absent
     * @param cpkEncoding the shape of the token's {@code cpk} claim
     */
    public record Diagnostics(String idpDomain, String idpProtocol, String algorithm, String keyId, String issuer,
                              List<String> audience, CpkEncoding cpkEncoding) {
        private static final int MAX_TEXT = 256;
        private static final int MAX_AUDIENCES = 8;

        public Diagnostics {
            idpDomain = sanitize(idpDomain);
            idpProtocol = sanitize(idpProtocol);
            algorithm = sanitize(algorithm);
            keyId = sanitize(keyId);
            issuer = sanitize(issuer);
            audience = audience == null ? List.of() : audience.stream()
                    .filter(Objects::nonNull).limit(MAX_AUDIENCES).map(Diagnostics::sanitize).toList();
        }

        private static String sanitize(String value) {
            if (value == null) {
                return null;
            }
            String printable = value.codePoints().filter(c -> !Character.isISOControl(c))
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
            return printable.length() > MAX_TEXT ? printable.substring(0, MAX_TEXT) : printable;
        }
    }

    public OfferCheckResult {
        Objects.requireNonNull(level, "level");
        Objects.requireNonNull(diagnostics, "diagnostics");
        claims = claims == null ? Map.of() : immutableClaims(claims);
        if (level.keyProven() != (clientKey != null)) {
            throw new IllegalArgumentException("A client key is present exactly when the level proves one");
        }
        if ((level == Level.ATTESTED) != (reason == null)) {
            throw new IllegalArgumentException("A reason is present exactly when the check stopped below ATTESTED");
        }
        if (reason != null && reason.level() != level) {
            throw new IllegalArgumentException(reason + " belongs to " + reason.level() + ", not " + level);
        }
        if (!level.keyProven() && !claims.isEmpty()) {
            throw new IllegalArgumentException("Claims are kept only for proven keys");
        }
    }

    static OfferCheckResult stopped(Reason reason, Diagnostics diagnostics) {
        return new OfferCheckResult(reason.level(), null, Map.of(), reason, diagnostics);
    }

    /** @return whether the client proved it holds the token's key, attested or not */
    public boolean keyProven() {
        return level.keyProven();
    }

    /** @return whether a trusted issuer signed the token, so the claims are attested */
    public boolean attested() {
        return level == Level.ATTESTED;
    }

    /**
     * @param name a claim name
     * @return the claim when it is a non-empty string
     */
    public Optional<String> stringClaim(String name) {
        return claims.get(name) instanceof String value && !value.isEmpty() ? Optional.of(value) : Optional.empty();
    }

    /**
     * The player's XUID from the {@code xid} claim. Only as trustworthy as {@link #attested()} says;
     * Education 26.33 sends it empty.
     */
    public Optional<String> xuid() {
        return stringClaim("xid");
    }

    /** The player's display name from the {@code xname} claim, with the same caveat as {@link #xuid()}. */
    public Optional<String> displayName() {
        return stringClaim("xname");
    }

    private static Map<String, Object> immutableClaims(Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, immutable(value)));
        return Collections.unmodifiableMap(copy);
    }

    private static Object immutable(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            map.forEach((key, entry) -> copy.put(key, immutable(entry)));
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            list.forEach(entry -> copy.add(immutable(entry)));
            return Collections.unmodifiableList(copy);
        }
        return value;
    }
}

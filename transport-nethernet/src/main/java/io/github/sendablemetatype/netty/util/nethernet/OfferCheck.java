package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Diagnostics;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Level;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Reason;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwa.AlgorithmConstraints.ConstraintType;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;

import java.security.Key;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Checks the client identity assertion in a NetherNet offer, on any signaling path that carries
 * one, in three steps that grade the result:
 * <ol>
 *     <li><b>Parse</b> the {@code a=identity} envelope and the fingerprint lines strictly:
 *     {@link Level#NO_ASSERTION} or {@link Level#MALFORMED} on failure. The {@code idp} block is
 *     not checked.</li>
 *     <li><b>Prove the key</b>: read {@code cpk} from the unverified token (base64 SPKI or JWK),
 *     and verify the detached ES384 signature over the offer's canonical fingerprints with it.
 *     {@link Level#KEY_PROVEN} or {@link Level#PROOF_FAILED}. No network.</li>
 *     <li><b>Attest</b>: pick the {@link TrustedIssuer} by the token's exact {@code iss}, check
 *     {@code aud}, {@code alg}, {@code exp} and {@code nbf} on the unverified token, and only then
 *     look up the issuer's key and verify the token. {@link Level#ATTESTED}, or
 *     {@link Level#KEY_PROVEN} with the reason attestation stopped.</li>
 * </ol>
 * An unknown issuer never causes a key lookup, so a forged token cannot make the server fetch
 * anything. Education 26.33 tokens are self-signed and carry no {@code iss}: their proof verifies
 * and they end at {@link Level#KEY_PROVEN} (captured 2026-11-05).
 * <p>
 * Results never carry library exception text, which can contain the bearer token.
 */
public final class OfferCheck {
    private static final String PROOF_ALGORITHM = AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384;
    private static final Set<String> TOKEN_ALGORITHMS = Set.of(
            AlgorithmIdentifiers.RSA_USING_SHA256, AlgorithmIdentifiers.RSA_USING_SHA384,
            AlgorithmIdentifiers.RSA_USING_SHA512, AlgorithmIdentifiers.RSA_PSS_USING_SHA256,
            AlgorithmIdentifiers.RSA_PSS_USING_SHA384, AlgorithmIdentifiers.RSA_PSS_USING_SHA512,
            AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256,
            AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384,
            AlgorithmIdentifiers.ECDSA_USING_P521_CURVE_AND_SHA512);
    static final long CLOCK_SKEW_SECONDS = 30;

    private final Map<String, TrustedIssuer> issuers;

    /**
     * @param issuers the issuers whose tokens attest, at most one per {@code iss}; none means no
     *                token is ever attested and every proven key ends at {@link Level#KEY_PROVEN}
     */
    public OfferCheck(Collection<TrustedIssuer> issuers) {
        Map<String, TrustedIssuer> byIssuer = new LinkedHashMap<>();
        for (TrustedIssuer issuer : issuers) {
            if (byIssuer.putIfAbsent(issuer.issuer(), issuer) != null) {
                throw new IllegalArgumentException("Two trusted issuers for " + issuer.issuer());
            }
        }
        this.issuers = Map.copyOf(byIssuer);
    }

    /** A check that attests retail tokens against Minecraft's authorization service. */
    public static OfferCheck minecraft() {
        return new OfferCheck(List.of(TrustedIssuer.minecraft()));
    }

    /**
     * A bounded executor for checks: up to four daemon threads that time out when idle, and at
     * most 64 queued checks before {@link #checkAsync} fails with
     * {@link RejectedExecutionException}. More threads only help while issuer keys are cached:
     * with an empty cache every thread waits on the same key set fetch.
     */
    public static ExecutorService newExecutor() {
        int threads = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(64), new DefaultThreadFactory("nethernet-identity", true));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Fetches the trusted issuers' key sets, so the first join does not wait for them. The
     * endpoint calls it once at bind, off the I/O loop. Blocks for at most the fetch timeouts;
     * failures are logged and the first join fetches again.
     */
    public void prefetch() {
        issuers.values().forEach(TrustedIssuer::prefetch);
    }

    /**
     * Runs the check on {@code executor}. The admission stage owns the returned stage and may time
     * it out or cancel it. Either withdraws the check: a queued check never runs, a running one is
     * interrupted, and a check that finishes late cannot deliver a result, because the stage is
     * already complete.
     *
     * @param offerSdp the offer as signaled, identity line included
     * @param executor a bounded executor that is not an I/O loop, for example {@link #newExecutor()}
     * @return the result; completes exceptionally with {@link RejectedExecutionException} when the
     *         executor refuses the check
     */
    public CompletableFuture<OfferCheckResult> checkAsync(String offerSdp, Executor executor) {
        CompletableFuture<OfferCheckResult> stage = new CompletableFuture<>();
        FutureTask<OfferCheckResult> task = new FutureTask<>(() -> check(offerSdp)) {
            // Runs once the task is final, so completing the stage here never interrupts the task.
            @Override
            protected void done() {
                if (isCancelled()) {
                    return;
                }
                try {
                    stage.complete(get());
                } catch (ExecutionException failure) {
                    stage.completeExceptionally(failure.getCause());
                } catch (InterruptedException impossible) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        // A stage that ends first (cancelled, timed out, completed by its owner) withdraws the check.
        stage.whenComplete((result, failure) -> task.cancel(true));
        try {
            executor.execute(task);
        } catch (RejectedExecutionException rejected) {
            stage.completeExceptionally(rejected);
        }
        return stage;
    }

    /**
     * Runs the check on the calling thread. Can block on a key set fetch for an attested issuer,
     * so never call it on an I/O loop.
     *
     * @param offerSdp the offer as signaled, identity line included
     * @return the result, never null
     */
    public OfferCheckResult check(String offerSdp) {
        Facts facts = new Facts();
        try {
            Proven proven = prove(offerSdp, facts);
            Reason unattested = attest(proven.token(), proven.claims(), facts.issuer);
            Level level = unattested == null ? Level.ATTESTED : Level.KEY_PROVEN;
            return new OfferCheckResult(level, proven.key(), proven.claims().getClaimsMap(), unattested,
                    facts.diagnostics());
        } catch (SdpAssertion.Stop stop) {
            return OfferCheckResult.stopped(stop.reason, facts.diagnostics());
        }
    }

    /**
     * Steps 1 and 2 on any description that carries an identity assertion: an offer, or a server's
     * answer, which uses the same envelope and proof.
     *
     * @param token  the token, its header and payload read but not verified
     * @param claims the token's claims, unverified
     * @param key    the key the token names in {@code cpk}, canonical secp384r1, which signed the
     *               description's fingerprints
     */
    record Proven(JsonWebSignature token, JwtClaims claims, ECPublicKey key) {
    }

    /**
     * Parses a description's assertion and proves its key over the description's fingerprints.
     *
     * @throws SdpAssertion.Stop with a {@link Level#NO_ASSERTION}, {@link Level#MALFORMED} or
     *                           {@link Level#PROOF_FAILED} reason
     */
    static Proven prove(String sdp) throws SdpAssertion.Stop {
        return prove(sdp, new Facts());
    }

    private static Proven prove(String sdp, Facts facts) throws SdpAssertion.Stop {
        SdpAssertion assertion = SdpAssertion.parse(sdp);
        facts.idpDomain = assertion.idpDomain();
        facts.idpProtocol = assertion.idpProtocol();
        JsonWebSignature token = new JsonWebSignature();
        JwtClaims claims;
        try {
            token.setCompactSerialization(assertion.token());
            facts.algorithm = token.getAlgorithmHeaderValue();
            facts.keyId = token.getKeyIdHeaderValue();
            claims = JwtClaims.parse(token.getUnverifiedPayload());
        } catch (JoseException | InvalidJwtException | RuntimeException e) {
            throw new SdpAssertion.Stop(Reason.MALFORMED_TOKEN);
        }
        facts.issuer = claims.getClaimValue("iss") instanceof String issuer ? issuer : null;
        facts.audience = audienceOf(claims.getClaimValue("aud"));
        Object cpk = claims.getClaimValue("cpk");
        facts.cpkEncoding = IdentityPublicKey.encodingOf(cpk);

        ECPublicKey key;
        try {
            key = IdentityPublicKey.fromClaim(cpk);
        } catch (IllegalArgumentException e) {
            throw new SdpAssertion.Stop(Reason.INVALID_CLIENT_KEY);
        }
        if (!proves(assertion, key)) {
            throw new SdpAssertion.Stop(Reason.PROOF_INVALID);
        }
        return new Proven(token, claims, key);
    }

    private static boolean proves(SdpAssertion assertion, ECPublicKey key) {
        try {
            JsonWebSignature proof = new JsonWebSignature();
            proof.setCompactSerialization(assertion.fingerprintProof());
            // The proof must sign the payload we rebuild from the SDP; an unencoded payload would
            // change what the signature covers.
            if (!PROOF_ALGORITHM.equals(proof.getAlgorithmHeaderValue())
                    || Boolean.FALSE.equals(proof.getHeaders().getObjectHeaderValue("b64"))) {
                return false;
            }
            proof.setAlgorithmConstraints(new AlgorithmConstraints(ConstraintType.PERMIT, PROOF_ALGORITHM));
            proof.setKey(key);
            proof.setPayload(assertion.fingerprintJson());
            return proof.verifySignature();
        } catch (JoseException | RuntimeException e) {
            return false;
        }
    }

    /** @return null when attested, otherwise why not */
    private Reason attest(JsonWebSignature token, JwtClaims claims, String issuerName) {
        if (issuerName == null) {
            return Reason.NO_ISSUER;
        }
        TrustedIssuer issuer = issuers.get(issuerName);
        if (issuer == null) {
            return Reason.UNKNOWN_ISSUER;
        }
        // Everything checkable on the unverified token is checked before the key lookup, which
        // may fetch the issuer's key set.
        Reason precheck = precheck(token, claims, issuer);
        if (precheck != null) {
            return precheck;
        }
        Key key;
        try {
            key = issuer.resolve(token);
        } catch (UnresolvableKeyException e) {
            return e.getCause() != null ? Reason.TRUST_SOURCE_UNREACHABLE : Reason.UNKNOWN_KEY;
        } catch (RuntimeException e) {
            return Reason.TRUST_SOURCE_UNREACHABLE;
        }
        try {
            token.setAlgorithmConstraints(new AlgorithmConstraints(ConstraintType.PERMIT,
                    TOKEN_ALGORITHMS.toArray(String[]::new)));
            token.setKey(key);
            return token.verifySignature() ? null : Reason.BAD_SIGNATURE;
        } catch (JoseException | RuntimeException e) {
            return Reason.BAD_SIGNATURE;
        }
    }

    private static Reason precheck(JsonWebSignature token, JwtClaims claims, TrustedIssuer issuer) {
        try {
            if (!claims.hasAudience() || !claims.getAudience().contains(issuer.audience())) {
                return Reason.WRONG_AUDIENCE;
            }
            if (!TOKEN_ALGORITHMS.contains(token.getAlgorithmHeaderValue())) {
                return Reason.ALGORITHM_NOT_ALLOWED;
            }
            NumericDate expiry = claims.getExpirationTime();
            if (expiry == null || claims.getSubject() == null) {
                return Reason.INVALID_CLAIMS;
            }
            NumericDate now = NumericDate.now();
            if (now.getValue() > expiry.getValue() + CLOCK_SKEW_SECONDS) {
                return Reason.EXPIRED;
            }
            NumericDate notBefore = claims.getNotBefore();
            if (notBefore != null && now.getValue() < notBefore.getValue() - CLOCK_SKEW_SECONDS) {
                return Reason.NOT_YET_VALID;
            }
            return null;
        } catch (MalformedClaimException e) {
            return Reason.INVALID_CLAIMS;
        }
    }

    private static List<String> audienceOf(Object claim) {
        return switch (claim) {
            case String audience -> List.of(audience);
            case List<?> audiences -> {
                List<String> strings = new ArrayList<>();
                for (Object audience : audiences) {
                    if (audience instanceof String text) strings.add(text);
                }
                yield strings;
            }
            case null, default -> List.of();
        };
    }

    /** What the steps learn along the way, kept for the result's diagnostics whatever step stops. */
    private static final class Facts {
        String idpDomain, idpProtocol, algorithm, keyId, issuer;
        List<String> audience = List.of();
        OfferCheckResult.CpkEncoding cpkEncoding;

        Diagnostics diagnostics() {
            return new Diagnostics(idpDomain, idpProtocol, algorithm, keyId, issuer, audience, cpkEncoding);
        }
    }
}

package io.github.sendablemetatype.netty.util.nethernet;

import com.google.gson.Gson;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.CpkEncoding;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Level;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Reason;
import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.keys.HmacKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

class OfferCheckTest {
    private static final OfferCheck RETAIL = retailCheck();

    private static String offerWith(JwtClaims claims) {
        return offer(identity(RETAIL_IDP, retailToken(claims), proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST);
    }

    @Test
    void retailOfferIsAttestedThroughTheIssuersKeySet() throws Exception {
        try (JwksServer keys = new JwksServer(keySet(), 3600)) {
            OfferCheckResult result = new OfferCheck(List.of(keys.minecraftStandIn())).check(retailOffer());

            assertEquals(Level.ATTESTED, result.level());
            assertNull(result.reason());
            assertArrayEquals(IdentityPublicKey.canonical(CLIENT_KEY.getPublic()), result.clientKey().getEncoded());
            assertEquals(Optional.of("2535400000000000"), result.xuid());
            assertEquals(Optional.of("TestPlayer"), result.displayName());
            assertEquals(new OfferCheckResult.Diagnostics(RETAIL_IDP, "default", "RS256", AUTH_KEY_ID,
                    TrustedIssuer.MINECRAFT_ISSUER, List.of(TrustedIssuer.MINECRAFT_AUDIENCE), CpkEncoding.SPKI),
                    result.diagnostics());
            assertEquals(Optional.empty(), OfferPolicy.REQUIRE_ATTESTED.refusal(result));
        }
    }

    @Test
    void educationOfferProvesItsKeyAndIsAdmittedByDefault() {
        OfferCheckResult result = RETAIL.check(educationOffer());

        assertEquals(Level.KEY_PROVEN, result.level());
        assertEquals(Reason.NO_ISSUER, result.reason());
        assertFalse(result.attested());
        assertArrayEquals(IdentityPublicKey.canonical(CLIENT_KEY.getPublic()), result.clientKey().getEncoded());
        assertEquals("test-mid", result.claims().get("mid"));
        assertEquals(Optional.empty(), result.xuid(), "Education sends an empty xid");
        assertEquals("", result.diagnostics().idpDomain());
        assertEquals("ES384", result.diagnostics().algorithm());
        assertEquals(Optional.empty(), OfferPolicy.REQUIRE_KEY_PROVEN.refusal(result));
        assertEquals(Optional.of(IdentityRefusal.NOT_ATTESTED), OfferPolicy.REQUIRE_ATTESTED.refusal(result));
    }

    @ParameterizedTest
    @EnumSource(CpkEncoding.class)
    void bothCpkEncodingsProveTheSameCanonicalKey(CpkEncoding encoding) {
        JwtClaims claims = retailClaims(CLIENT_KEY.getPublic());
        if (encoding == CpkEncoding.JWK) {
            claims.setClaim("cpk", new EllipticCurveJsonWebKey((ECPublicKey) CLIENT_KEY.getPublic())
                    .toParams(JsonWebKey.OutputControlLevel.PUBLIC_ONLY));
        }
        OfferCheckResult result = RETAIL.check(offerWith(claims));

        assertEquals(Level.ATTESTED, result.level());
        assertEquals(encoding, result.diagnostics().cpkEncoding());
        assertArrayEquals(IdentityPublicKey.canonical(CLIENT_KEY.getPublic()), result.clientKey().getEncoded());
    }

    @Test
    void offerWithoutAnAssertionIsRefusedAsMissing() {
        OfferCheckResult result = RETAIL.check(offer(null, DIGEST));

        assertEquals(Level.NO_ASSERTION, result.level());
        assertEquals(Reason.NO_IDENTITY, result.reason());
        assertNull(result.clientKey());
        assertEquals(Optional.of(IdentityRefusal.IDENTITY_MISSING), OfferPolicy.REQUIRE_KEY_PROVEN.refusal(result));
    }

    @Test
    void assertionReplayedOverOtherFingerprintsFailsTheProof() {
        // A captured assertion on a transport whose certificate its signer never made
        OfferCheckResult tampered = RETAIL.check(retailOffer().replace(DIGEST, OTHER_DIGEST));
        String foreignSigner = offer(identity(RETAIL_IDP, retailToken(retailClaims(CLIENT_KEY.getPublic())),
                proof(keyPair("EC", "secp384r1").getPrivate(), DIGEST)), DIGEST);

        for (OfferCheckResult result : List.of(tampered, RETAIL.check(foreignSigner))) {
            assertEquals(Level.PROOF_FAILED, result.level());
            assertEquals(Reason.PROOF_INVALID, result.reason());
            assertTrue(result.claims().isEmpty());
            assertEquals(Optional.of(IdentityRefusal.IDENTITY_INVALID), OfferPolicy.REQUIRE_KEY_PROVEN.refusal(result));
        }
    }

    @Test
    void allowUnassertedAdmitsOffersWithoutAnAssertionAndStillChecksTheRest() {
        OfferCheckResult unasserted = RETAIL.check(offer(null, DIGEST));
        OfferCheckResult malformed = RETAIL.check(offer("%%%", DIGEST));
        OfferCheckResult failedProof = RETAIL.check(retailOffer().replace(DIGEST, OTHER_DIGEST));
        assertEquals(Level.NO_ASSERTION, unasserted.level());
        assertEquals(Level.MALFORMED, malformed.level());
        assertEquals(Level.PROOF_FAILED, failedProof.level());

        assertEquals(Optional.empty(), OfferPolicy.ALLOW_UNASSERTED.refusal(unasserted));
        assertEquals(Optional.of(IdentityRefusal.IDENTITY_INVALID), OfferPolicy.ALLOW_UNASSERTED.refusal(malformed));
        assertEquals(Optional.of(IdentityRefusal.IDENTITY_INVALID), OfferPolicy.ALLOW_UNASSERTED.refusal(failedProof));
        assertEquals(Optional.empty(), OfferPolicy.ALLOW_UNASSERTED.refusal(RETAIL.check(educationOffer())));
        assertEquals(Optional.empty(), OfferPolicy.ALLOW_UNASSERTED.refusal(RETAIL.check(retailOffer())));
    }

    @Test
    void proofCoversEveryFingerprintInOrder() {
        String sdp = offer(identity(RETAIL_IDP, retailToken(retailClaims(CLIENT_KEY.getPublic())),
                proof(CLIENT_KEY.getPrivate(), DIGEST, OTHER_DIGEST)), DIGEST, OTHER_DIGEST);

        assertEquals(Level.ATTESTED, RETAIL.check(sdp).level());
        assertEquals(Level.PROOF_FAILED, RETAIL.check(offer(identity(RETAIL_IDP,
                retailToken(retailClaims(CLIENT_KEY.getPublic())), proof(CLIENT_KEY.getPrivate(), DIGEST, OTHER_DIGEST)),
                OTHER_DIGEST, DIGEST)).level());
    }

    @ParameterizedTest
    @ValueSource(strings = {"unencoded-payload", "unknown-critical-header", "hmac-with-public-key", "es256"})
    void proofsInOtherSignatureModesFail(String mode) {
        JsonWebSignature signature = new JsonWebSignature();
        signature.setPayload(canonical(DIGEST));
        signature.setKey(CLIENT_KEY.getPrivate());
        signature.setAlgorithmHeaderValue(AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384);
        String proof = switch (mode) {
            case "unencoded-payload" -> {
                signature.getHeaders().setObjectHeaderValue("b64", false);
                signature.setCriticalHeaderNames("b64");
                yield detached(compact(signature));
            }
            case "unknown-critical-header" -> {
                signature.setHeader("unknown", "value");
                signature.setCriticalHeaderNames("unknown");
                yield detached(compact(signature));
            }
            case "hmac-with-public-key" -> detached(sign(canonical(DIGEST),
                    new HmacKey(CLIENT_KEY.getPublic().getEncoded()), AlgorithmIdentifiers.HMAC_SHA256, Map.of()));
            case "es256" -> detached(sign(canonical(DIGEST), keyPair("EC", "secp256r1").getPrivate(),
                    AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, Map.of()));
            default -> throw new AssertionError(mode);
        };
        String sdp = offer(identity(RETAIL_IDP, retailToken(retailClaims(CLIENT_KEY.getPublic())), proof), DIGEST);

        assertEquals(Level.PROOF_FAILED, RETAIL.check(sdp).level());
    }

    @ParameterizedTest
    @ValueSource(strings = {"duplicate-identity", "media-identity", "missing-fingerprint", "indented-line",
            "invalid-digest", "not-base64", "not-json", "duplicate-json-member", "missing-token", "attached-proof",
            "empty-signature"})
    void malformedOffersStopAtParsing(String shape) {
        String valid = retailOffer();
        String identityLine = valid.lines().filter(line -> line.startsWith("a=identity:")).findFirst().orElseThrow();
        String identity = identityLine.substring("a=identity:".length());
        String json = new String(Base64.getDecoder().decode(identity), StandardCharsets.UTF_8);
        String token = retailToken(retailClaims(CLIENT_KEY.getPublic()));
        String sdp = switch (shape) {
            case "duplicate-identity" -> valid.replace(identityLine, identityLine + "\r\n" + identityLine);
            case "media-identity" -> valid.replace(identityLine + "\r\n", "") + identityLine + "\r\n";
            case "missing-fingerprint" -> valid.replace("a=fingerprint:sha-256 " + DIGEST + "\r\n", "");
            case "indented-line" -> valid.replace("a=fingerprint:", " a=fingerprint:");
            case "invalid-digest" -> valid.replace(DIGEST, "not-a-digest");
            case "not-base64" -> valid.replace(identity, "%%%");
            case "not-json" -> valid.replace(identity, Base64.getEncoder().encodeToString("{".getBytes(StandardCharsets.UTF_8)));
            case "duplicate-json-member" -> valid.replace(identity, Base64.getEncoder().encodeToString(
                    json.replace("\"assertion\":", "\"assertion\":\"{}\",\"assertion\":").getBytes(StandardCharsets.UTF_8)));
            case "missing-token" -> offer(identity(RETAIL_IDP, "", proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST);
            case "attached-proof" -> offer(identity(RETAIL_IDP, token, sign(canonical(DIGEST), CLIENT_KEY.getPrivate(),
                    AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384, Map.of())), DIGEST);
            case "empty-signature" -> offer(identity(RETAIL_IDP, token, "header.."), DIGEST);
            default -> throw new AssertionError(shape);
        };
        OfferCheckResult result = RETAIL.check(sdp);

        assertEquals(Level.MALFORMED, result.level(), () -> shape + " gave " + result);
        assertEquals(Optional.of(IdentityRefusal.IDENTITY_INVALID), OfferPolicy.REQUIRE_KEY_PROVEN.refusal(result));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-jws", "missing", "not-a-key", "wrong-curve", "private-jwk"})
    void unreadableTokensAndKeysAreMalformed(String shape) {
        JwtClaims claims = retailClaims(CLIENT_KEY.getPublic());
        String token = switch (shape) {
            case "not-a-jws" -> "not-a-token";
            case "missing" -> {
                claims.unsetClaim("cpk");
                yield retailToken(claims);
            }
            case "not-a-key" -> {
                claims.setClaim("cpk", "bm90LWEta2V5");
                yield retailToken(claims);
            }
            case "wrong-curve" -> {
                claims.setClaim("cpk", spki(keyPair("EC", "secp256r1").getPublic()));
                yield retailToken(claims);
            }
            case "private-jwk" -> {
                Map<String, Object> jwk = new EllipticCurveJsonWebKey((ECPublicKey) CLIENT_KEY.getPublic())
                        .toParams(JsonWebKey.OutputControlLevel.PUBLIC_ONLY);
                jwk.put("d", "private");
                claims.setClaim("cpk", jwk);
                yield retailToken(claims);
            }
            default -> throw new AssertionError(shape);
        };
        OfferCheckResult result = RETAIL.check(offer(identity(RETAIL_IDP, token, proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST));

        assertEquals(Level.MALFORMED, result.level());
        assertEquals(shape.equals("not-a-jws") ? Reason.MALFORMED_TOKEN : Reason.INVALID_CLIENT_KEY, result.reason());
    }

    @Test
    void idpBlockIsNotChecked() {
        // Education sends an empty domain, retail 1.26.50 did too; the block is unsigned and the guide never checks it
        String token = retailToken(retailClaims(CLIENT_KEY.getPublic()));
        String proof = proof(CLIENT_KEY.getPrivate(), DIGEST);
        Gson gson = new Gson();
        String withoutIdp = Base64.getEncoder().encodeToString(gson.toJson(
                Map.of("assertion", gson.toJson(new Identity.Assertion(token, proof)))).getBytes(StandardCharsets.UTF_8));

        assertEquals(Level.ATTESTED, RETAIL.check(offer(identity("not even a domain", token, proof), DIGEST)).level());
        OfferCheckResult noBlock = RETAIL.check(offer(withoutIdp, DIGEST));
        assertEquals(Level.ATTESTED, noBlock.level());
        assertNull(noBlock.diagnostics().idpDomain());
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown-issuer", "wrong-audience", "expired", "not-yet-valid", "missing-subject",
            "missing-expiry", "unsigned", "hmac", "foreign-signer"})
    void attestationFailuresKeepTheProvenKey(String shape) {
        JwtClaims claims = retailClaims(CLIENT_KEY.getPublic());
        long now = Instant.now().getEpochSecond();
        Reason expected;
        String token = null;
        switch (shape) {
            case "unknown-issuer" -> { claims.setIssuer("https://attacker.test/"); expected = Reason.UNKNOWN_ISSUER; }
            case "wrong-audience" -> { claims.setAudience("another-service"); expected = Reason.WRONG_AUDIENCE; }
            case "expired" -> { claims.setExpirationTime(NumericDate.fromSeconds(now - 3600)); expected = Reason.EXPIRED; }
            case "not-yet-valid" -> { claims.setNotBefore(NumericDate.fromSeconds(now + 3600)); expected = Reason.NOT_YET_VALID; }
            case "missing-subject" -> { claims.unsetClaim("sub"); expected = Reason.INVALID_CLAIMS; }
            case "missing-expiry" -> { claims.unsetClaim("exp"); expected = Reason.INVALID_CLAIMS; }
            case "unsigned" -> {
                Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
                token = encoder.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8)) + "."
                        + encoder.encodeToString(claims.toJson().getBytes(StandardCharsets.UTF_8)) + ".";
                expected = Reason.ALGORITHM_NOT_ALLOWED;
            }
            case "hmac" -> {
                token = sign(claims.toJson(), new HmacKey(AUTH_KEY.getPublic().getEncoded()),
                        AlgorithmIdentifiers.HMAC_SHA256, Map.of());
                expected = Reason.ALGORITHM_NOT_ALLOWED;
            }
            case "foreign-signer" -> {
                token = sign(claims.toJson(), keyPair("RSA", null).getPrivate(), AlgorithmIdentifiers.RSA_USING_SHA256, Map.of());
                expected = Reason.BAD_SIGNATURE;
            }
            default -> throw new AssertionError(shape);
        }
        if (token == null) {
            token = retailToken(claims);
        }
        OfferCheckResult result = RETAIL.check(offer(identity(RETAIL_IDP, token, proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST));

        assertEquals(Level.KEY_PROVEN, result.level());
        assertEquals(expected, result.reason());
        assertNotNull(result.clientKey());
        assertEquals(Optional.empty(), OfferPolicy.REQUIRE_KEY_PROVEN.refusal(result));
        assertEquals(Optional.of(IdentityRefusal.NOT_ATTESTED), OfferPolicy.REQUIRE_ATTESTED.refusal(result));
    }

    @Test
    void unreachableKeySetLeavesTheKeyProvenWithItsOwnRefusal() {
        // Port 1 refuses the connection, so the fetch fails before any signature check
        OfferCheck check = new OfferCheck(List.of(TrustedIssuer.jwks(URI.create("https://127.0.0.1:1/keys"),
                TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE)));
        OfferCheckResult result = check.check(retailOffer());

        assertEquals(Level.KEY_PROVEN, result.level());
        assertEquals(Reason.TRUST_SOURCE_UNREACHABLE, result.reason());
        assertEquals(Optional.empty(), OfferPolicy.REQUIRE_KEY_PROVEN.refusal(result));
        assertEquals(Optional.of(IdentityRefusal.TRUST_SOURCE_UNREACHABLE), OfferPolicy.REQUIRE_ATTESTED.refusal(result));
    }

    @Test
    void keySetsMustBeServedOverHttps() {
        assertThrows(IllegalArgumentException.class, () -> TrustedIssuer.jwks(URI.create("http://issuer.test/keys"),
                TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE));
    }

    @Test
    void prefetchSparesTheFirstJoinAndForeignTokensNeverFetch() throws Exception {
        try (JwksServer keys = new JwksServer(keySet(), 3600)) {
            OfferCheck check = new OfferCheck(List.of(keys.minecraftStandIn()));
            check.prefetch();
            assertEquals(1, keys.fetches.get());

            assertEquals(Level.ATTESTED, check.check(retailOffer()).level());
            JwtClaims foreign = retailClaims(CLIENT_KEY.getPublic());
            foreign.setIssuer("https://attacker.test/");
            assertEquals(Reason.UNKNOWN_ISSUER, check.check(offerWith(foreign)).reason());
            JwtClaims expired = retailClaims(CLIENT_KEY.getPublic());
            expired.setExpirationTime(NumericDate.fromSeconds(Instant.now().getEpochSecond() - 3600));
            assertEquals(Reason.EXPIRED, check.check(offerWith(expired)).reason());

            assertEquals(1, keys.fetches.get(), "only the prefetch reached the key set");
        }
    }

    @Test
    void unknownKeyIdsCannotMakeTheServerRefetchAtWill() throws Exception {
        try (JwksServer keys = new JwksServer(keySet(), 3600)) {
            OfferCheck check = new OfferCheck(List.of(keys.minecraftStandIn()));
            check.prefetch();
            String unknownKid = sign(retailClaims(CLIENT_KEY.getPublic()).toJson(), AUTH_KEY.getPrivate(),
                    AlgorithmIdentifiers.RSA_USING_SHA256, Map.of("kid", "rotated-away"));
            String sdp = offer(identity(RETAIL_IDP, unknownKid, proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST);

            assertEquals(Reason.UNKNOWN_KEY, check.check(sdp).reason());
            // Past jose4j's own 300 ms limit, which would let an attacker trigger three fetches a second
            TimeUnit.MILLISECONDS.sleep(400);
            assertEquals(Reason.UNKNOWN_KEY, check.check(sdp).reason());
            assertEquals(1, keys.fetches.get());
        }
    }

    @Test
    void anUnreachableKeySetIsNotRefetchedForEveryOffer() throws Exception {
        try (JwksServer keys = new JwksServer(keySet(), 3600)) {
            keys.failing = true;
            OfferCheck check = new OfferCheck(List.of(keys.minecraftStandIn()));

            for (int i = 0; i < 10; i++) {
                assertEquals(Level.KEY_PROVEN, check.check(retailOffer()).level());
            }
            assertEquals(1, keys.fetches.get());
        }
    }

    @Test
    void cachedKeysOutliveAFailedRefresh() throws Exception {
        OfferCheck check;
        try (JwksServer keys = new JwksServer(keySet(), 1)) {
            // Without the unknown-key refresh limit, so the expired set is really refetched
            check = new OfferCheck(List.of(TrustedIssuer.jwksAt(keys.uri(), TrustedIssuer.MINECRAFT_ISSUER,
                    TrustedIssuer.MINECRAFT_AUDIENCE, 0)));
            check.prefetch();
        }
        // The key set has expired and its server is gone: the refresh fails and the last keys stay in use
        TimeUnit.MILLISECONDS.sleep(1200);
        assertEquals(Level.ATTESTED, check.check(retailOffer()).level());
    }

    @Test
    void diagnosticsAreSafeToLog() {
        String sdp = offer(identity("evil\r\nINFO forged log line", retailToken(retailClaims(CLIENT_KEY.getPublic())),
                proof(CLIENT_KEY.getPrivate(), DIGEST)), DIGEST);
        OfferCheckResult result = RETAIL.check(sdp);

        assertEquals("evilINFO forged log line", result.diagnostics().idpDomain());
    }

    @Test
    void claimsAreAnImmutableSnapshot() {
        Map<String, Object> claims = RETAIL.check(retailOffer()).claims();

        assertThrows(UnsupportedOperationException.class, () -> claims.put("xid", "other"));
    }

    @Test
    void asynchronousCheckRunsOnTheGivenExecutor() {
        Queue<Runnable> queued = new ArrayDeque<>();
        Executor executor = queued::add;

        CompletableFuture<OfferCheckResult> stage = RETAIL.checkAsync(educationOffer(), executor);
        assertFalse(stage.isDone());
        queued.remove().run();

        assertEquals(Level.KEY_PROVEN, stage.join().level());
    }

    @Test
    void stageEndedByItsOwnerWithdrawsTheQueuedCheck() {
        Queue<Runnable> queued = new ArrayDeque<>();
        CompletableFuture<OfferCheckResult> cancelled = RETAIL.checkAsync(retailOffer(), queued::add);
        CompletableFuture<OfferCheckResult> timedOut = RETAIL.checkAsync(retailOffer(), queued::add)
                .orTimeout(1, TimeUnit.MILLISECONDS);

        cancelled.cancel(false);
        assertThrows(Exception.class, timedOut::join);
        queued.forEach(Runnable::run);

        assertThrows(CancellationException.class, cancelled::join);
        assertTrue(queued.stream().allMatch(task -> ((Future<?>) task).isCancelled()),
                "a late check never runs, so it can never deliver a result");
    }

    @Test
    void refusedExecutorFailsTheStage() {
        CompletableFuture<OfferCheckResult> stage = RETAIL.checkAsync(retailOffer(), task -> {
            throw new RejectedExecutionException("full");
        });

        assertTrue(stage.isCompletedExceptionally());
        assertInstanceOf(RejectedExecutionException.class, assertThrows(Exception.class, stage::join).getCause());
    }

    @Test
    void oneIssuerPerIss() {
        TrustedIssuer issuer = TrustedIssuer.fixedKey(AUTH_KEY.getPublic(), "https://issuer.test/", "aud");
        assertThrows(IllegalArgumentException.class, () -> new OfferCheck(List.of(issuer,
                TrustedIssuer.fixedKey(keyPair("RSA", null).getPublic(), "https://issuer.test/", "aud"))));
    }

    private static String compact(JsonWebSignature signature) {
        try {
            return signature.getCompactSerialization();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

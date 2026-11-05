package io.github.sendablemetatype.netty.util.nethernet;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;
import org.jose4j.lang.UnresolvableKeyException;

import java.net.URI;
import java.security.Key;
import java.security.PublicKey;
import java.util.List;
import java.util.Objects;

/**
 * A token issuer whose signature attests an offer token, picked by the token's exact {@code iss}.
 * Its key, or the URL its keys are fetched from, comes from trusted configuration, never from the
 * offer or the token's headers.
 */
public final class TrustedIssuer {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(TrustedIssuer.class);

    /** The issuer of retail multiplayer tokens. */
    public static final String MINECRAFT_ISSUER = "https://authorization.franchise.minecraft-services.net/";
    /** The audience of retail multiplayer tokens. */
    public static final String MINECRAFT_AUDIENCE = "api://auth-minecraft-services/multiplayer";

    private static final int TIMEOUT_MILLIS = 5_000;
    private static final int MAX_KEY_SET_CHARS = 64 * 1024;
    // A refresh that fails after the cache expired keeps the last keys this long, so an outage at
    // the issuer does not stop attestation at the next cache expiry.
    private static final long RETAIN_KEYS_ON_ERROR_SECONDS = 24 * 60 * 60;
    // Any offer can name an unknown kid; this bounds how often that makes us fetch the key set.
    private static final long UNKNOWN_KEY_REFRESH_MILLIS = 60_000;

    private final String issuer;
    private final String audience;
    private final HttpsJwks keySet;
    private final HttpsJwksVerificationKeyResolver keySetResolver;
    private final PublicKey fixedKey;

    private TrustedIssuer(String issuer, String audience, HttpsJwks keySet, PublicKey fixedKey) {
        this.issuer = Objects.requireNonNull(issuer, "issuer");
        this.audience = Objects.requireNonNull(audience, "audience");
        this.keySet = keySet;
        this.keySetResolver = keySet == null ? null : new HttpsJwksVerificationKeyResolver(keySet);
        this.fixedKey = fixedKey;
    }

    /**
     * Minecraft's authorization service, which signs retail multiplayer tokens. Every caller
     * shares one key cache per process.
     */
    public static TrustedIssuer minecraft() {
        return Minecraft.ISSUER;
    }

    /**
     * An issuer whose keys are published as a JWKS. The set is fetched with a 5 s connect and
     * read timeout, no retries and a 64 KiB cap, cached as its response headers say, and kept for
     * a day when a refresh fails.
     *
     * @param keySetUrl the HTTPS location of the issuer's key set
     * @param issuer    the exact {@code iss} its tokens carry
     * @param audience  the {@code aud} its tokens must include
     */
    public static TrustedIssuer jwks(URI keySetUrl, String issuer, String audience) {
        if (!"https".equalsIgnoreCase(keySetUrl.getScheme())) {
            throw new IllegalArgumentException("The key set URL must use HTTPS: " + keySetUrl);
        }
        return jwksAt(keySetUrl, issuer, audience);
    }

    /**
     * An issuer with one fixed signing key, for private issuers.
     *
     * @param key      the issuer's public signing key
     * @param issuer   the exact {@code iss} its tokens carry
     * @param audience the {@code aud} its tokens must include
     */
    public static TrustedIssuer fixedKey(PublicKey key, String issuer, String audience) {
        return new TrustedIssuer(issuer, audience, null, Objects.requireNonNull(key, "key"));
    }

    /** Package-private so tests can serve a key set over plain HTTP. */
    static TrustedIssuer jwksAt(URI keySetUrl, String issuer, String audience) {
        return jwksAt(keySetUrl, issuer, audience, UNKNOWN_KEY_REFRESH_MILLIS);
    }

    static TrustedIssuer jwksAt(URI keySetUrl, String issuer, String audience, long unknownKeyRefreshMillis) {
        Get http = new Get();
        http.setConnectTimeout(TIMEOUT_MILLIS);
        http.setReadTimeout(TIMEOUT_MILLIS);
        http.setRetries(0);
        http.setResponseBodySizeLimit(MAX_KEY_SET_CHARS);
        HttpsJwks keySet = new HttpsJwks(keySetUrl.toString());
        keySet.setSimpleHttpGet(http);
        keySet.setRetainCacheOnErrorDuration(RETAIN_KEYS_ON_ERROR_SECONDS);
        keySet.setRefreshReprieveThreshold(unknownKeyRefreshMillis);
        return new TrustedIssuer(issuer, audience, keySet, null);
    }

    /** @return the exact {@code iss} this issuer's tokens carry */
    public String issuer() {
        return issuer;
    }

    /** @return the {@code aud} this issuer's tokens must include */
    public String audience() {
        return audience;
    }

    /**
     * Fetches the key set ahead of the first join, so that join does not pay for the round trip.
     * Blocks for at most the fetch timeouts; failures are logged, the first join fetches again.
     */
    void prefetch() {
        if (keySet == null) {
            return;
        }
        try {
            keySet.refresh();
        } catch (Exception e) {
            log.warn("Could not prefetch the keys of {}; the first join fetches them again: {}", issuer, e.toString());
        }
    }

    /**
     * @param token the token, with its header read
     * @return the key to verify it with
     * @throws UnresolvableKeyException without a cause when the issuer has no matching key, with
     *                                  the fetch failure as cause when its keys are unreachable
     */
    Key resolve(JsonWebSignature token) throws UnresolvableKeyException {
        return keySetResolver != null ? keySetResolver.resolveKey(token, List.of()) : fixedKey;
    }

    private static final class Minecraft {
        static final TrustedIssuer ISSUER = jwksAt(URI.create(MINECRAFT_ISSUER + ".well-known/keys"),
                MINECRAFT_ISSUER, MINECRAFT_AUDIENCE);
    }
}

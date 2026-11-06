/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.util.nethernet;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jose4j.jwt.JwtClaims;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Objects;

/**
 * The operator identity a server presents in the {@code a=identity} of every answer, on every
 * signaling path. Plain HTTP clients pin its key and ask the player to trust a key they have not
 * seen, so one key per server (or fleet) is kept for as long as possible.
 * <p>
 * There is no default: a server either loads or creates its key at a path
 * ({@link #loadOrCreate}), or explicitly runs with a key that dies with the process
 * ({@link #ephemeral}).
 *
 * @see <a href="https://github.com/Mojang/bedrock-protocol-docs/blob/7330880ab78ef001cad0b9cdfedb3aa3eaa6d4af/NetherNetOnboardingGuide.md#52-producing-the-server-assertion-in-the-answer">NetherNet onboarding guide, section 5.2</a>
 */
public final class ServerIdentity {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(ServerIdentity.class);

    private final AssertionSigner signer;
    private final ECPublicKey publicKey;
    private final String domain;
    private final String token;

    private ServerIdentity(KeyPair pair, String domain) throws GeneralSecurityException {
        this.domain = Objects.requireNonNull(domain, "domain");
        this.signer = new AssertionSigner(pair, "server identity");
        this.publicKey = signer.publicKey();
        this.token = buildToken();
    }

    /**
     * Loads the identity key from a PEM file, creating a new secp384r1 key there when the file
     * does not exist. The file is never replaced once it exists, because clients pin the key.
     * <p>
     * Loading accepts the forms {@link PemKeys} describes, including EduGeyser's key file, and
     * follows a symbolic link (Kubernetes mounts secrets that way). An existing regular file
     * readable by others is tightened to owner-only on POSIX.
     * <p>
     * Creating refuses a symbolic link at the path, writes the key owner-only (POSIX permissions or
     * an owner-only ACL on Windows), syncs it to disk, and links it into place only if no file
     * appeared meanwhile; when another process won that race, its key is loaded.
     *
     * @param keyFile the PEM file to load or create
     * @param domain  the name the identity carries as {@code idp.domain} and {@code iss}; the guide
     *                calls it untrusted display text
     * @return the identity
     * @throws IOException              if the file cannot be read or created
     * @throws GeneralSecurityException if the file holds no usable secp384r1 key
     */
    public static ServerIdentity loadOrCreate(Path keyFile, String domain) throws IOException, GeneralSecurityException {
        Path path = keyFile.toAbsolutePath();
        KeyFile.Origin origin = KeyFile.ensure(path, "the identity key", () -> PemKeys.writeSec1(generate()));
        ServerIdentity identity = new ServerIdentity(
                PemKeys.read(Files.readString(path, StandardCharsets.UTF_8), path.toString()), domain);
        log.info("{} the NetherNet server identity key {} (SHA-256 {})", origin.verb(), path, identity.fingerprint());
        return identity;
    }

    /**
     * An identity whose key exists only in memory. Plain HTTP players are asked to trust the
     * server again after every restart, so use it only where that is acceptable.
     *
     * @param domain the name the identity carries as {@code idp.domain} and {@code iss}
     */
    public static ServerIdentity ephemeral(String domain) {
        try {
            ServerIdentity identity = new ServerIdentity(generate(), domain);
            log.warn("Using an ephemeral NetherNet server identity key (SHA-256 {}); plain HTTP players are asked "
                    + "to trust the server again after every restart", identity.fingerprint());
            return identity;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot generate a server identity key", e);
        }
    }

    /** Builds an identity from a key pair; package-private so tests can use fixed keys. */
    static ServerIdentity of(KeyPair pair, String domain) throws GeneralSecurityException {
        return new ServerIdentity(pair, domain);
    }

    /** @return the key clients pin */
    public ECPublicKey publicKey() {
        return publicKey;
    }

    /** @return the SHA-256 fingerprint to publish for plain HTTP players, see {@link IdentityPublicKey#fingerprint} */
    public String fingerprint() {
        return IdentityPublicKey.fingerprint(publicKey);
    }

    /** @return the name the identity carries */
    public String domain() {
        return domain;
    }

    /**
     * Inserts this identity's assertion into a finished answer, before the first media line as the
     * guide places it. The signature covers only the answer's fingerprint lines, so later edits to
     * other lines (candidates) keep it valid.
     *
     * @param answerSdp the answer without an identity line
     * @return the answer with the identity inserted, line endings kept
     * @throws GeneralSecurityException if the key cannot sign
     * @throws IllegalArgumentException if the answer has no well-formed fingerprint line
     */
    public String augmentAnswer(String answerSdp) throws GeneralSecurityException {
        return signer.withAssertion(answerSdp, token, domain);
    }

    /**
     * The self-signed token clients pin: {@code cpk} as a public JWK object, as the guide
     * specifies (ruling C-8), {@code iat}, and the domain as {@code iss} when it is not blank. It
     * carries no {@code exp}, which the guide allows, so it never needs re-signing.
     */
    private String buildToken() throws GeneralSecurityException {
        JwtClaims claims = new JwtClaims();
        claims.setClaim("cpk", IdentityPublicKey.toJwk(publicKey));
        claims.setIssuedAtToNow();
        if (!domain.isBlank()) {
            claims.setIssuer(domain);
        }
        return signer.sign(claims.toJson());
    }

    private static KeyPair generate() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp384r1"));
        return generator.generateKeyPair();
    }
}

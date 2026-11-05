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
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.lang.JoseException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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
    private static final String ALG = AlgorithmIdentifiers.ECDSA_USING_P384_CURVE_AND_SHA384;
    private static final Set<PosixFilePermission> OWNER_ONLY = PosixFilePermissions.fromString("rw-------");

    private final PrivateKey privateKey;
    private final ECPublicKey publicKey;
    private final String domain;
    private final String token;

    private ServerIdentity(KeyPair pair, String domain) throws GeneralSecurityException {
        this.domain = Objects.requireNonNull(domain, "domain");
        this.privateKey = pair.getPrivate();
        this.publicKey = IdentityPublicKey.fromCanonical(IdentityPublicKey.canonical(pair.getPublic()));
        try {
            this.token = buildToken();
            // A private key that does not belong to the public key would sign answers no client
            // accepts, and players would only see "not invited"; fail at startup instead.
            JsonWebSignature check = new JsonWebSignature();
            check.setCompactSerialization(token);
            check.setKey(publicKey);
            if (!check.verifySignature()) {
                throw new InvalidKeyException("The server identity's private and public keys do not match");
            }
        } catch (JoseException e) {
            throw new GeneralSecurityException("Cannot sign the server identity token", e);
        }
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
        if (Files.exists(path)) {
            tightenExisting(path);
            return loaded(path, domain, "Loaded");
        }
        if (Files.isSymbolicLink(path)) {
            throw new IOException("Refusing to create the identity key through a symbolic link: " + path);
        }
        Path directory = path.getParent();
        Files.createDirectories(directory);
        boolean posix = Files.getFileStore(directory).supportsFileAttributeView(PosixFileAttributeView.class);

        Path temporary = createOwnerOnly(directory, posix);
        boolean created;
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(PemKeys.writeSec1(generate()).getBytes(StandardCharsets.US_ASCII)));
                channel.force(true);
            }
            created = link(temporary, path);
        } finally {
            Files.deleteIfExists(temporary);
        }
        if (posix) {
            syncDirectory(directory);
        }
        return loaded(path, domain, created ? "Created" : "Loaded the concurrently created");
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
     * @throws JoseException            if signing fails
     * @throws IllegalArgumentException if the answer has no well-formed fingerprint line
     */
    public String augmentAnswer(String answerSdp) throws JoseException {
        String line = "a=identity:" + identityValue(answerSdp);
        String eol = answerSdp.contains("\r\n") ? "\r\n" : "\n";
        String[] lines = answerSdp.split("\r\n|\n", -1);
        List<String> out = new ArrayList<>(lines.length + 1);
        boolean inserted = false;
        for (String current : lines) {
            if (!inserted && current.startsWith("m=")) {
                out.add(line);
                inserted = true;
            }
            out.add(current);
        }
        if (!inserted) {
            out.add(line);
        }
        return String.join(eol, out);
    }

    /** The base64 envelope for one answer: the token and a detached signature over its fingerprints. */
    String identityValue(String answerSdp) throws JoseException {
        String[] signed = sign(OfferAssertion.answerFingerprintJson(answerSdp)).split("\\.");
        Identity.Assertion assertion = new Identity.Assertion(token, signed[0] + ".." + signed[2]);
        return new Identity(new Identity.Idp(domain, "default"), assertion).toBase64();
    }

    /**
     * The self-signed token clients pin: {@code cpk} as a public JWK object, as the guide
     * specifies (ruling C-8), {@code iat}, and the domain as {@code iss} when it is not blank. It
     * carries no {@code exp}, which the guide allows, so it never needs re-signing.
     */
    private String buildToken() throws JoseException {
        JwtClaims claims = new JwtClaims();
        claims.setClaim("cpk", IdentityPublicKey.toJwk(publicKey));
        claims.setIssuedAtToNow();
        if (!domain.isBlank()) {
            claims.setIssuer(domain);
        }
        return sign(claims.toJson());
    }

    private String sign(String payload) throws JoseException {
        JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(payload);
        jws.setKey(privateKey);
        jws.setAlgorithmHeaderValue(ALG);
        return jws.getCompactSerialization();
    }

    private static KeyPair generate() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp384r1"));
        return generator.generateKeyPair();
    }

    private static ServerIdentity loaded(Path path, String domain, String how)
            throws IOException, GeneralSecurityException {
        ServerIdentity identity = new ServerIdentity(
                PemKeys.read(Files.readString(path, StandardCharsets.UTF_8), path.toString()), domain);
        log.info("{} the NetherNet server identity key {} (SHA-256 {})", how, path, identity.fingerprint());
        return identity;
    }

    // Tightened only when actually too wide, and never through a link: a linked key usually sits on
    // a read-only mount, such as a Kubernetes secret volume, where the chmod would fail.
    private static void tightenExisting(Path path) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class)) {
            return;
        }
        if (!OWNER_ONLY.containsAll(Files.getPosixFilePermissions(path))) {
            Files.setPosixFilePermissions(path, OWNER_ONLY);
        }
    }

    // The file is empty until it is owner-only, so the key is never readable by others.
    private static Path createOwnerOnly(Path directory, boolean posix) throws IOException {
        Path temporary = directory.resolve(".identity-" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36) + ".tmp");
        if (posix) {
            return Files.createFile(temporary, PosixFilePermissions.asFileAttribute(OWNER_ONLY));
        }
        Files.createFile(temporary);
        AclFileAttributeView acl = Files.getFileAttributeView(temporary, AclFileAttributeView.class);
        if (acl != null) {
            try {
                acl.setAcl(List.of(AclEntry.newBuilder()
                        .setType(AclEntryType.ALLOW)
                        .setPrincipal(Files.getOwner(temporary))
                        .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                        .build()));
            } catch (IOException | RuntimeException e) {
                Files.deleteIfExists(temporary);
                throw e;
            }
        }
        return temporary;
    }

    /**
     * Links the written key into place, which fails if a file appeared at the path meanwhile, so
     * a key is never replaced. A file system without hard links falls back to a move, which can
     * miss a file created in the same instant.
     *
     * @return whether this process created the file, false when another one won
     */
    private static boolean link(Path temporary, Path path) throws IOException {
        try {
            Files.createLink(path, temporary);
            return true;
        } catch (FileAlreadyExistsException race) {
            return false;
        } catch (UnsupportedOperationException | FileSystemException noHardLinks) {
            try {
                Files.move(temporary, path);
                return true;
            } catch (FileAlreadyExistsException race) {
                return false;
            }
        }
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}

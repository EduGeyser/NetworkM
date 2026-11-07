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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.keyPair;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Throwaway keys from {@code openssl ecparam -name secp384r1 -genkey -noout}. Never deploy them. */
class ServerIdentityKeyFileTest {
    private static final String SEC1 = """
            -----BEGIN EC PRIVATE KEY-----
            MIGkAgEBBDDmCV/icghwrAdKuvy8s6iJc7J5SdH9Ks43hq7Bw1JO/d0sgdHGbe30
            mKHzlu5+GW6gBwYFK4EEACKhZANiAAQK7XU8ZiZox723S4u5U01a1Uioo5TMCuT7
            ozPRWmm431vyOC9i+irVFLOTjcvwAY3D3+T7RGG1Y1/F0CPoO8tP3ClHbOeCgINA
            qGQgpH3NW/5D6hvZubCqcEKvV2igcoA=
            -----END EC PRIVATE KEY-----
            """;

    /** The same key run through {@code openssl pkcs8 -topk8 -nocrypt}, which keeps the public point. */
    private static final String PKCS8 = """
            -----BEGIN PRIVATE KEY-----
            MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDDmCV/icghwrAdKuvy8
            s6iJc7J5SdH9Ks43hq7Bw1JO/d0sgdHGbe30mKHzlu5+GW6hZANiAAQK7XU8ZiZo
            x723S4u5U01a1Uioo5TMCuT7ozPRWmm431vyOC9i+irVFLOTjcvwAY3D3+T7RGG1
            Y1/F0CPoO8tP3ClHbOeCgINAqGQgpH3NW/5D6hvZubCqcEKvV2igcoA=
            -----END PRIVATE KEY-----
            """;

    /** The same key with {@code -no_public}: the point has to be derived. */
    private static final String SEC1_NO_PUBLIC = """
            -----BEGIN EC PRIVATE KEY-----
            MD4CAQEEMOYJX+JyCHCsB0q6/LyzqIlzsnlJ0f0qzjeGrsHDUk793SyB0cZt7fSY
            ofOW7n4ZbqAHBgUrgQQAIg==
            -----END EC PRIVATE KEY-----
            """;

    /**
     * EduGeyser's {@code identity-key.pem}: the JDK's PKCS#8 encoding, which has no public point,
     * then a {@code PUBLIC KEY} block, as its {@code NetherNetServerIdentity} writes them.
     */
    private static final String EDUGEYSER = """
            -----BEGIN PRIVATE KEY-----
            ME4CAQAwEAYHKoZIzj0CAQYFK4EEACIENzA1AgEBBDDmCV/icghwrAdKuvy8s6iJ
            c7J5SdH9Ks43hq7Bw1JO/d0sgdHGbe30mKHzlu5+GW4=
            -----END PRIVATE KEY-----
            -----BEGIN PUBLIC KEY-----
            MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAECu11PGYmaMe9t0uLuVNNWtVIqKOUzArk
            +6Mz0VppuN9b8jgvYvoq1RSzk43L8AGNw9/k+0RhtWNfxdAj6DvLT9wpR2zngoCD
            QKhkIKR9zVv+Q+ob2bmwqnBCr1dooHKA
            -----END PUBLIC KEY-----
            """;

    /** {@code openssl ecparam -name secp384r1 -genkey} without {@code -noout} adds this block. */
    private static final String EC_PARAMETERS = """
            -----BEGIN EC PARAMETERS-----
            BgUrgQQAIg==
            -----END EC PARAMETERS-----
            """;

    /** {@code openssl ec -in key.pem -pubout -outform DER | openssl dgst -sha256 -c} for the key above. */
    private static final String FINGERPRINT =
            "aa:82:55:8d:83:46:27:e1:98:b6:3c:6e:5e:b2:0e:2f:33:4f:9b:3b:05:b5:17:b1:67:5f:a4:ce:1a:88:13:17";

    private static Path write(Path dir, String pem) throws IOException {
        Path file = dir.resolve("identity-key.pem");
        Files.writeString(file, pem);
        return file;
    }

    @ParameterizedTest
    @ValueSource(strings = {"sec1", "pkcs8", "sec1-without-public-point", "edugeyser", "with-ec-parameters"})
    void readsEveryStandardFormOfTheSameKey(String form, @TempDir Path dir) throws Exception {
        String pem = switch (form) {
            case "sec1" -> SEC1;
            case "pkcs8" -> PKCS8;
            case "sec1-without-public-point" -> SEC1_NO_PUBLIC;
            case "edugeyser" -> EDUGEYSER;
            case "with-ec-parameters" -> EC_PARAMETERS + SEC1;
            default -> throw new AssertionError(form);
        };
        Path file = write(dir, pem);

        assertEquals(FINGERPRINT, ServerIdentity.loadOrCreate(file, "self").fingerprint());
        assertEquals(pem, Files.readString(file), "an existing key is never rewritten");
    }

    @Test
    void readsAFileTheWayEduGeyserWritesIt(@TempDir Path dir) throws Exception {
        // EduGeyser's writer, verbatim in behavior: JDK encodings in 64 column MIME base64
        KeyPair pair = keyPair("EC", "secp384r1");
        Path file = write(dir, pemBlock(pair.getPrivate().getEncoded(), "PRIVATE KEY")
                + pemBlock(pair.getPublic().getEncoded(), "PUBLIC KEY"));

        assertEquals(IdentityPublicKey.fingerprint(pair.getPublic()), ServerIdentity.loadOrCreate(file, "self").fingerprint());
    }

    @Test
    void refusesAPublicKeyThatIsNotThePrivateKeys(@TempDir Path dir) throws Exception {
        KeyPair other = keyPair("EC", "secp384r1");
        String mismatched = EDUGEYSER.substring(0, EDUGEYSER.indexOf("-----BEGIN PUBLIC KEY-----"))
                + pemBlock(other.getPublic().getEncoded(), "PUBLIC KEY");
        Path file = write(dir, mismatched);

        GeneralSecurityException refused = assertThrows(GeneralSecurityException.class,
                () -> ServerIdentity.loadOrCreate(file, "self"));
        assertTrue(refused.getMessage().contains("do not match"), refused.getMessage());
        assertEquals(mismatched, Files.readString(file), "a failed load keeps the operator's file");
    }

    @Test
    void refusesAnEmbeddedPointThatIsNotThePrivateKeys() throws Exception {
        // The SEC1 fixture's scalar with another key's point: both files end with the point's coordinates
        byte[] der = Base64.getMimeDecoder().decode(body(SEC1));
        byte[] other = IdentityPublicKey.canonical(keyPair("EC", "secp384r1").getPublic());
        System.arraycopy(other, other.length - 96, der, der.length - 96, 96);

        GeneralSecurityException refused = assertThrows(GeneralSecurityException.class,
                () -> PemKeys.read(pemBlock(der, "EC PRIVATE KEY"), "test"));
        assertTrue(refused.getMessage().contains("do not match"), refused.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"integer", "no-length", "length-past-end", "five-byte-length", "indefinite-length",
            "short-length", "empty-sequence", "point-with-unused-bits"})
    void refusesDerItDoesNotKnow(String shape) {
        // A key file is operator supplied, so every malformed shape is refused rather than half read
        byte[] der = switch (shape) {
            case "integer" -> new byte[]{0x02, 0x01, 0x00};
            case "no-length" -> new byte[]{0x30};
            case "length-past-end" -> new byte[]{0x30, 0x05, 0x02, 0x01, 0x01};
            case "five-byte-length" -> new byte[]{0x30, (byte) 0x85, 0x01, 0x01, 0x01, 0x01, 0x01};
            case "indefinite-length" -> new byte[]{0x30, (byte) 0x80};
            case "short-length" -> new byte[]{0x30, (byte) 0x82, 0x01};
            case "empty-sequence" -> new byte[]{0x30, 0x00};
            case "point-with-unused-bits" -> new byte[]{0x30, 0x0b, 0x02, 0x01, 0x01, 0x04, 0x01, 0x01, (byte) 0xa1, 0x03, 0x03, 0x01, 0x07};
            default -> throw new AssertionError(shape);
        };
        assertThrows(GeneralSecurityException.class, () -> PemKeys.read(pemBlock(der, "EC PRIVATE KEY"), "test"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"no-block", "not-base64", "public-key-only", "two-private-keys", "other-curve"})
    void refusesFilesWithoutExactlyOneP384Key(String shape) {
        String pem = switch (shape) {
            case "no-block" -> "# just a comment\nand some text\n";
            case "not-base64" -> "-----BEGIN EC PRIVATE KEY-----\nnot base64!!\n-----END EC PRIVATE KEY-----\n";
            case "public-key-only" -> EDUGEYSER.substring(EDUGEYSER.indexOf("-----BEGIN PUBLIC KEY-----"));
            case "two-private-keys" -> SEC1 + SEC1;
            case "other-curve" -> pemBlock(keyPair("EC", "secp256r1").getPrivate().getEncoded(), "PRIVATE KEY");
            default -> throw new AssertionError(shape);
        };
        assertThrows(GeneralSecurityException.class, () -> PemKeys.read(pem, "test"));
    }

    @Test
    void createsAKeyOnFirstUseAndKeepsIt(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("keys/identity.pem");

        ServerIdentity created = ServerIdentity.loadOrCreate(file, "self");
        String written = Files.readString(file);
        ServerIdentity reloaded = ServerIdentity.loadOrCreate(file, "self");

        assertTrue(written.startsWith("-----BEGIN EC PRIVATE KEY-----"), "SEC1 with its public point, as openssl writes it");
        assertEquals(created.fingerprint(), reloaded.fingerprint(), "replacing the key would re-prompt every player");
        assertEquals(written, Files.readString(file));
        try (var files = Files.list(file.getParent())) {
            assertEquals(List.of(file), files.toList(), "no temporary file is left behind");
        }
    }

    @Test
    void movesAKeyIntoTheFormOpensslWrites(@TempDir Path dir) throws Exception {
        Path old = write(dir, EDUGEYSER);
        Path moved = dir.resolve("identity.pem");

        assertTrue(ServerIdentity.load(old, "self").writeKeyFile(moved));

        // One EC PRIVATE KEY block with the point, which a reader that takes only the first block
        // and requires the point (upstream's) accepts
        assertEquals(SEC1, Files.readString(moved).replace("\r\n", "\n"));
        assertEquals(FINGERPRINT, ServerIdentity.loadOrCreate(moved, "self").fingerprint());
        assertEquals(EDUGEYSER, Files.readString(old), "loading and writing leave the source alone");
    }

    @Test
    void writingTheKeyNeverReplacesAFile(@TempDir Path dir) throws Exception {
        Path existing = write(dir, EDUGEYSER);
        ServerIdentity other = ServerIdentity.of(keyPair("EC", "secp384r1"), "self");

        assertFalse(other.writeKeyFile(existing));
        assertEquals(EDUGEYSER, Files.readString(existing));
    }

    @Test
    void loadingNeverCreatesAKey(@TempDir Path dir) {
        Path missing = dir.resolve("identity.pem");

        assertThrows(IOException.class, () -> ServerIdentity.load(missing, "self"));
        assertFalse(Files.exists(missing));
    }

    @Test
    void concurrentCreatorsAllEndUpWithTheWinnersKey(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("identity.pem");
        int racers = 8;
        CyclicBarrier start = new CyclicBarrier(racers);
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        try {
            Callable<String> racer = () -> {
                start.await();
                return ServerIdentity.loadOrCreate(file, "self").fingerprint();
            };
            List<Future<String>> results = pool.invokeAll(IntStream.range(0, racers).mapToObj(i -> racer).toList());
            Set<String> fingerprints = results.stream().map(result -> {
                try {
                    return result.get();
                } catch (Exception e) {
                    throw new AssertionError("a racer failed instead of loading the winner's key", e);
                }
            }).collect(Collectors.toSet());

            assertEquals(Set.of(ServerIdentity.loadOrCreate(file, "self").fingerprint()), fingerprints);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void createsTheKeyReadableOnlyByItsOwner(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("identity.pem");
        ServerIdentity.loadOrCreate(file, "self");

        if (Files.getFileStore(dir).supportsFileAttributeView(PosixFileAttributeView.class)) {
            assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
        } else {
            List<AclEntry> acl = Files.getFileAttributeView(file, AclFileAttributeView.class).getAcl();
            assertEquals(1, acl.size(), () -> "only the owner may read the key: " + acl);
            assertEquals(Files.getOwner(file), acl.getFirst().principal());
        }
    }

    @Test
    void tightensAnExistingKeyThatOthersCanRead(@TempDir Path dir) throws Exception {
        assumeTrue(Files.getFileStore(dir).supportsFileAttributeView(PosixFileAttributeView.class), "POSIX only");
        Path file = write(dir, SEC1);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-rw-rw-"));

        ServerIdentity.loadOrCreate(file, "self");

        assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
    }

    @Test
    void refusesToCreateTheKeyThroughASymbolicLink(@TempDir Path dir) throws Exception {
        // Following a dangling link would write the key wherever it points
        Path link = symbolicLink(dir.resolve("identity.pem"), dir.resolve("elsewhere.pem"));

        IOException refused = assertThrows(IOException.class, () -> ServerIdentity.loadOrCreate(link, "self"));
        assertTrue(refused.getMessage().contains("symbolic link"), refused.getMessage());
        assertFalse(Files.exists(dir.resolve("elsewhere.pem")));
    }

    @Test
    void loadsAnExistingKeyThroughASymbolicLink(@TempDir Path dir) throws Exception {
        // Kubernetes mounts secrets as links into a read-only volume
        Path target = dir.resolve("..data-identity.pem");
        Files.writeString(target, SEC1);
        Path link = symbolicLink(dir.resolve("identity.pem"), target);

        assertEquals(FINGERPRINT, ServerIdentity.loadOrCreate(link, "self").fingerprint());
    }

    private static Path symbolicLink(Path link, Path target) {
        try {
            return Files.createSymbolicLink(link, target);
        } catch (IOException | UnsupportedOperationException e) {
            // Windows needs Developer Mode or elevation to create one
            assumeTrue(false, "cannot create symbolic links here: " + e);
            throw new AssertionError(e);
        }
    }

    private static String body(String pem) {
        return pem.lines().filter(line -> !line.startsWith("-----")).collect(Collectors.joining());
    }

    private static String pemBlock(byte[] der, String label) {
        return "-----BEGIN " + label + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(der)
                + "\n-----END " + label + "-----\n";
    }
}

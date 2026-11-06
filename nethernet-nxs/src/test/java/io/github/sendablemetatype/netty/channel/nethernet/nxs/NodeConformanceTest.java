package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticVectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPrivateKeySpec;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The byte-exact bar: upstream's Node verifiers, an implementation independent of this one, run on a copy of the
 * specification that carries this module's output. The canonical payloads must equal the fixtures byte for byte,
 * Node must verify the fresh Java signatures and diagnostic credentials, and the fixtures' hashes must hold.
 *
 * <p>Needs Node.js; {@code -PnxsNode=<path>} picks the binary, {@code node} on the path otherwise.
 */
class NodeConformanceTest {
    private static final Path SPECIFICATION = Path.of(System.getProperty("nxs.specification"));
    private static final String NODE = System.getProperty("nxs.node", "node");

    @Test
    void nodeAcceptsWhatTheJavaSignerAndCodecsProduce(@TempDir Path copy) throws Exception {
        copyTree(SPECIFICATION, copy);
        Path fixtures = copy.resolve("fixtures");

        JsonObject nxs = read(fixtures.resolve("nxs-v1.fixtures.json"));
        signedByJava(nxs);
        Files.writeString(fixtures.resolve("nxs-v1.fixtures.json"),
                new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(nxs));

        statelessTokenMatches(read(fixtures.resolve("stateless-admission-v1.fixtures.json")));

        Path diagnostic = copy.resolve("diagnostic-java.json");
        Files.writeString(diagnostic, DiagnosticVectors.issued(read(fixtures.resolve("diagnostic-v1.fixtures.json")))
                .toString());

        node(copy, "tests/nxs-v1.fixtures.mjs");
        assertTrue(node(copy, "tests/diagnostic-v1.fixtures.mjs", "--java=" + diagnostic)
                .contains("and fresh Java output"));
        node(copy, "tests/origins.fixtures.mjs");
    }

    /** Replaces the fixture's signatures with fresh ones, after checking the payloads Java builds are the same bytes. */
    private static void signedByJava(JsonObject f) throws Exception {
        JsonObject challenge = f.getAsJsonObject("challenge"), publicJwk = f.getAsJsonObject("publicKeyJwk");
        assertEquals(challenge.get("thumbprint").getAsString(), ProviderCrypto.thumbprint(publicJwk));
        assertEquals(challenge.get("contextDigest").getAsString(),
                ProviderCrypto.contextDigest(challenge.getAsJsonObject("context")));
        String proof = ProviderCrypto.proof(challenge, f.get("proofNonce").getAsString(),
                f.get("idempotencyKey").getAsString());

        JsonObject i = f.getAsJsonObject("request").getAsJsonObject("input");
        String request = ProviderCrypto.request(i.get("audience").getAsString(), i.get("method").getAsString(),
                i.get("path").getAsString(), i.get("timestamp").getAsLong(), i.get("instanceId").getAsString(),
                i.get("keyId").getAsString(), i.get("idempotencyKey").getAsString(), i.get("generation").getAsLong(),
                i.get("sequence").getAsLong(), i.get("body").getAsString());

        JsonObject rotation = f.getAsJsonObject("rotation"), r = rotation.getAsJsonObject("input");
        String rotate = Registrar.rotationPayload(r.get("audience").getAsString(), r.get("instanceId").getAsString(),
                r.get("oldKeyId").getAsString(), rotation.getAsJsonObject("replacementPublicKeyJwk"),
                r.get("generation").getAsLong(), r.get("idempotencyKey").getAsString());

        PrivateKey machine = privateKey(f.getAsJsonObject("privateKeyJwk"));
        sign(f.getAsJsonObject("proof"), proof, machine);
        sign(f.getAsJsonObject("request"), request, machine);
        // The replacement key signs its own rotation proof
        sign(rotation, rotate, privateKey(rotation.getAsJsonObject("replacementPrivateKeyJwk")));
    }

    private static void sign(JsonObject vector, String payload, PrivateKey key) throws Exception {
        assertEquals(vector.get("payload").getAsString(), payload, "Canonical bytes");
        String signature = ProviderCrypto.sign(key, payload);
        assertNotEquals(vector.get("signature").getAsString(), signature, "ECDSA signatures are fresh");
        vector.addProperty("signature", signature);
    }

    /** The fixture's token is deterministic, so issuing it again in Java must give exactly what Node checks. */
    private static void statelessTokenMatches(JsonObject f) {
        JsonObject context = f.getAsJsonObject("context"), c = f.getAsJsonObject("claims");
        var claims = new StatelessAdmissionCodec.Claims(c.get("expiresAt").getAsLong(),
                c.get("clientFingerprintHex").getAsString(), c.get("clientSctpPort").getAsInt(),
                c.get("clientMaxMessageSize").getAsInt(), c.get("identityBindingHex").getAsString(),
                c.get("networkId").getAsString(), c.get("clientIcePwd").getAsString(), new byte[0]);
        var issued = StatelessAdmissionCodec.issue(context.get("keyId").getAsString(),
                context.get("secret").getAsString(), context.get("audience").getAsString(),
                f.get("clientIceUfrag").getAsString(), claims, HexFormat.of().parseHex(f.get("nonceHex").getAsString()));
        JsonObject expected = f.getAsJsonObject("expected");
        assertEquals(expected.get("localUfrag").getAsString(), issued.localUfrag());
        assertEquals(expected.get("icePwd").getAsString(), issued.icePwd());
    }

    /** A P-384 private key from a JWK's {@code d}, which is all a signer needs. */
    private static PrivateKey privateKey(JsonObject jwk) throws Exception {
        AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
        parameters.init(new ECGenParameterSpec("secp384r1"));
        return KeyFactory.getInstance("EC").generatePrivate(new ECPrivateKeySpec(
                new BigInteger(1, ProviderCrypto.decode(jwk.get("d").getAsString())),
                parameters.getParameterSpec(ECParameterSpec.class)));
    }

    private static String node(Path directory, String script, String... arguments) throws Exception {
        List<String> command = new ArrayList<>(List.of(NODE, script));
        command.addAll(List.of(arguments));
        Process process;
        try {
            process = new ProcessBuilder(command).directory(directory.toFile()).redirectErrorStream(true).start();
        } catch (IOException missing) {
            throw new AssertionError("Node.js is needed for the conformance check; install it or pass "
                    + "-PnxsNode=<path to node>", missing);
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), script + " finished");
        assertEquals(0, process.exitValue(), script + ":\n" + output);
        return output;
    }

    private static JsonObject read(Path file) throws IOException {
        return JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    }

    private static void copyTree(Path source, Path target) throws IOException {
        try (Stream<Path> files = Files.walk(source)) {
            for (Path file : files.toList()) {
                Path destination = target.resolve(source.relativize(file).toString());
                if (Files.isDirectory(file)) {
                    Files.createDirectories(destination);
                } else {
                    Files.copy(file, destination);
                }
            }
        }
    }
}

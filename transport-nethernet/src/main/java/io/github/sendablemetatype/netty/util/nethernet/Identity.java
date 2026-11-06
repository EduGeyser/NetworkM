package io.github.sendablemetatype.netty.util.nethernet;

import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * The {@code a=identity} envelope as a server writes it: the {@code idp} block and the assertion,
 * which travels as a JSON string inside the envelope's JSON.
 */
public record Identity(Idp idp, Assertion assertion) {
    private static final Gson GSON = new Gson();

    private record Raw(Idp idp, String assertion) {}

    public record Idp(String domain, String protocol) {}

    public record Assertion(String token, String fingerprints) {}

    public String toJson() {
        return GSON.toJson(new Raw(idp, GSON.toJson(assertion)));
    }

    public String toBase64() {
        return Base64.getEncoder().encodeToString(toJson().getBytes(StandardCharsets.UTF_8));
    }
}

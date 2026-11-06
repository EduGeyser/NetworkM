package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/** The diagnostic fixture's vectors as this module issues them, in the form the Node verifier's --java check reads. */
public final class DiagnosticVectors {
    private static final Gson GSON = new Gson();

    private DiagnosticVectors() {
    }

    /** One {@code {name, localUfrag, icePwd}} per vector, issued with the vector's nonce. */
    public static JsonArray issued(JsonObject fixture) {
        var context = GSON.fromJson(fixture.get("context"), DiagnosticAdmissionCodec.Context.class);
        var key = GSON.fromJson(fixture.get("key"), DiagnosticAdmissionCodec.Key.class);
        long now = fixture.get("now").getAsLong(), parent = fixture.get("parentExpiresAt").getAsLong();
        var clock = new DiagnosticAdmissionCodec.Clock(() -> now, () -> 1_000_000_000L);
        JsonArray issued = new JsonArray();
        for (JsonElement entry : fixture.getAsJsonArray("vectors")) {
            JsonObject vector = entry.getAsJsonObject();
            var credentials = DiagnosticAdmissionCodec.issueWithNonce(context, key,
                    GSON.fromJson(vector.get("claims"), DiagnosticAdmissionCodec.Claims.class),
                    vector.get("remoteUfrag").getAsString(),
                    DiagnosticAdmissionCodec.utf8(vector.get("offer").getAsString()), parent, clock,
                    DiagnosticAdmissionCodec.unhex(vector.get("nonceHex").getAsString(), 12));
            JsonObject result = new JsonObject();
            result.add("name", vector.get("name"));
            result.addProperty("localUfrag", credentials.localUfrag());
            result.addProperty("icePwd", credentials.icePwd());
            issued.add(result);
        }
        return issued;
    }
}

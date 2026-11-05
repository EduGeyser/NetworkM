package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult.Reason;
import org.jose4j.json.JsonUtil;
import org.jose4j.lang.JoseException;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Step 1 of the offer check: the identity assertion read out of an offer, strictly, without
 * trusting anything in it yet.
 * <p>
 * The {@code idp} block is read only for diagnostics (ruling C-1). The guide's validation steps
 * never check it, it is unsigned, and Education 26.33 sends an empty domain (captured 2026-11-05),
 * as retail 1.26.50 did (reported by go-nethernet).
 *
 * @param idpDomain        the envelope's {@code idp.domain}, or null
 * @param idpProtocol      the envelope's {@code idp.protocol}, or null
 * @param token            the compact token
 * @param fingerprintProof the detached compact JWS over the fingerprints, {@code header..signature}
 * @param fingerprintJson  the canonical fingerprint JSON the proof signs, rebuilt from the SDP
 */
record OfferAssertion(String idpDomain, String idpProtocol, String token, String fingerprintProof,
                      String fingerprintJson) {
    /** Bounds parsing work on any signaling path; real offers measured 2.0 to 2.5 KB (2026-11-05). */
    static final int MAX_OFFER_CHARS = 1 << 20;

    private static final String IDENTITY_PREFIX = "a=identity:";
    private static final String FINGERPRINT_PREFIX = "a=fingerprint:";

    /** A step that cannot go on, with the reason the result reports. */
    static final class Stop extends Exception {
        final Reason reason;

        Stop(Reason reason) {
            super(reason.name(), null, false, false);
            this.reason = reason;
        }
    }

    /**
     * @param sdp the offer as signaled, identity line included
     * @return the assertion
     * @throws Stop with {@link Reason#NO_IDENTITY} when the offer carries none, or a
     *              {@link OfferCheckResult.Level#MALFORMED} reason
     */
    static OfferAssertion parse(String sdp) throws Stop {
        if (sdp == null || sdp.length() > MAX_OFFER_CHARS) {
            throw new Stop(Reason.MALFORMED_SDP);
        }
        String identity = null;
        boolean media = false;
        List<String> fingerprints = new ArrayList<>();
        for (String line : sdp.split("\\r?\\n", -1)) {
            if (line.isEmpty()) continue;
            if (line.length() < 2 || line.charAt(1) != '=' || line.charAt(0) < 'a'
                    || line.charAt(0) > 'z' || line.indexOf('\r') >= 0) {
                throw new Stop(Reason.MALFORMED_SDP);
            }
            if (line.startsWith("m=")) media = true;
            if (line.startsWith(IDENTITY_PREFIX)) {
                // The guide places the attribute at session level, and real offers carry exactly one there.
                if (identity != null || media) {
                    throw new Stop(Reason.MALFORMED_SDP);
                }
                identity = line.substring(IDENTITY_PREFIX.length());
            } else if (line.startsWith(FINGERPRINT_PREFIX)) {
                fingerprints.add(fingerprintEntry(line).orElseThrow(() -> new Stop(Reason.MALFORMED_SDP)));
            }
        }
        if (identity == null) {
            throw new Stop(Reason.NO_IDENTITY);
        }
        if (identity.isEmpty() || fingerprints.isEmpty()) {
            throw new Stop(Reason.MALFORMED_SDP);
        }

        Map<String, Object> envelope = json(decode(identity));
        String domain = null, protocol = null;
        if (envelope.get("idp") instanceof Map<?, ?> idp) {
            domain = idp.get("domain") instanceof String value ? value : null;
            protocol = idp.get("protocol") instanceof String value ? value : null;
        }
        Map<String, Object> assertion = json(string(envelope, "assertion"));
        String proof = string(assertion, "fingerprints");
        String[] parts = proof.split("\\.", -1);
        if (parts.length != 3 || parts[0].isEmpty() || !parts[1].isEmpty() || parts[2].isEmpty()) {
            throw new Stop(Reason.MALFORMED_ASSERTION);
        }
        return new OfferAssertion(domain, protocol, string(assertion, "token"), proof, canonicalJson(fingerprints));
    }

    /**
     * The canonical fingerprint JSON over an answer's fingerprint lines, for the server's own
     * assertion. The answer comes from the local engine, so this is lenient about other lines.
     *
     * @throws IllegalArgumentException if the answer has no fingerprint or a malformed one
     */
    static String answerFingerprintJson(String answerSdp) {
        List<String> fingerprints = new ArrayList<>();
        for (String line : answerSdp.split("\\r?\\n", -1)) {
            if (line.startsWith(FINGERPRINT_PREFIX)) {
                fingerprints.add(fingerprintEntry(line)
                        .orElseThrow(() -> new IllegalArgumentException("Malformed fingerprint line in the answer")));
            }
        }
        if (fingerprints.isEmpty()) {
            throw new IllegalArgumentException("The answer has no fingerprint line");
        }
        return canonicalJson(fingerprints);
    }

    // The algorithm and digest charsets need no JSON escaping, so the text is canonical as built.
    private static Optional<String> fingerprintEntry(String line) {
        String[] fields = line.substring(FINGERPRINT_PREFIX.length()).trim().split("[ \\t]+", -1);
        if (fields.length != 2 || !fields[0].matches("[A-Za-z0-9-]+") || !isHexDigest(fields[1])) {
            return Optional.empty();
        }
        return Optional.of("{\"algorithm\":\"" + fields[0] + "\",\"digest\":\"" + fields[1] + "\"}");
    }

    private static String canonicalJson(List<String> entries) {
        return "{\"fingerprint\":[" + String.join(",", entries) + "]}";
    }

    private static boolean isHexDigest(String value) {
        if (value.length() % 3 != 2) return false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (i % 3 == 2 ? c != ':' : !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F')) {
                return false;
            }
        }
        return true;
    }

    private static String decode(String base64) throws Stop {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .decode(ByteBuffer.wrap(Base64.getDecoder().decode(base64))).toString();
        } catch (IllegalArgumentException | CharacterCodingException e) {
            throw new Stop(Reason.MALFORMED_ASSERTION);
        }
    }

    // jose4j's parser refuses duplicate member names, so no field can be read two ways.
    private static Map<String, Object> json(String text) throws Stop {
        try {
            return JsonUtil.parseJson(text);
        } catch (JoseException | RuntimeException e) {
            throw new Stop(Reason.MALFORMED_ASSERTION);
        }
    }

    private static String string(Map<String, Object> object, String key) throws Stop {
        if (!(object.get(key) instanceof String value) || value.isEmpty()) {
            throw new Stop(Reason.MALFORMED_ASSERTION);
        }
        return value;
    }
}

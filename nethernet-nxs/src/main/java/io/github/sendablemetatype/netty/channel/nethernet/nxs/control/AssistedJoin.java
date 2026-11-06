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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.control;

import com.google.gson.JsonObject;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.util.nethernet.IdentityPublicKey;
import io.netty.util.NetUtil;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

/**
 * The only unsolicited NXS message, accepted only on the provider's authenticated WebSocket. The record holds what
 * the frame's schema allows; {@link #parseOffer()} and {@link #canonicalCpk()} check what the host can serve.
 *
 * @param id          The attempt ID, 32 lowercase hex digits, which is also the join's ticket ID
 * @param expiresAt   The provider's deadline, epoch milliseconds on the provider's clock
 * @param networkId   The player's NetherNet network ID as an unsigned decimal, {@code 0} for a diagnostic
 * @param cpk         The provider-verified canonical client key, null for a diagnostic
 * @param localUfrag  The host's ICE ufrag for this join, an {@code NXS1} token
 * @param offer       The client's SDP with the candidates the provider selected
 */
public record AssistedJoin(
        String id,
        String instanceId,
        long generation,
        String incarnation,
        String keyId,
        DtlsFingerprint hostFingerprint,
        long expiresAt,
        String networkId,
        String cpk,
        String localUfrag,
        String localPassword,
        String offer) {
    /** The largest frame accepted for a join, before parsing. */
    public static final int MAX_BYTES = 73728;
    /** The largest offer the schema allows. */
    public static final int MAX_OFFER_BYTES = 16384;

    public AssistedJoin {
        Objects.requireNonNull(hostFingerprint, "hostFingerprint");
        if (id == null
                || !id.matches("[0-9a-f]{32}")
                || instanceId == null
                || !instanceId.matches("[A-Za-z0-9_-]{1,128}")
                || generation < 1
                || generation > ControlJson.MAX_SAFE_INTEGER
                || incarnation == null
                || !incarnation.matches("[0-9a-f]{32}")
                || keyId == null
                || !keyId.matches("[A-Z0-9]{4}")
                || expiresAt < 1
                || expiresAt > ControlJson.MAX_SAFE_INTEGER
                || !ice(localUfrag, 8)
                || !localUfrag.startsWith("NXS1")
                || !ice(localPassword, 22)
                || offer == null
                || offer.getBytes(StandardCharsets.UTF_8).length > MAX_OFFER_BYTES) {
            throw new IllegalArgumentException("Invalid assisted join");
        }
        if (networkId == null || !networkId.matches("0|[1-9][0-9]{0,19}")) {
            throw new IllegalArgumentException("Invalid assisted network ID");
        }
        Long.parseUnsignedLong(networkId);
        if ("0".equals(networkId)) {
            if (cpk != null || !localUfrag.startsWith("NXS1" + keyId)) {
                throw new IllegalArgumentException("Invalid assisted diagnostic purpose");
            }
        } else if (cpk == null || !cpk.matches("[A-Za-z0-9+/]{160}")) {
            throw new IllegalArgumentException("Invalid assisted player identity");
        }
    }

    /** Whether this is a connectivity check rather than a player. */
    public boolean diagnostic() {
        return "0".equals(networkId);
    }

    /**
     * Reads an {@code assisted-join} frame.
     *
     * @throws IllegalArgumentException If the frame does not have the frame's shape, which closes the socket
     */
    public static AssistedJoin decode(String wire) {
        JsonObject o = ControlJson.parse(wire, MAX_BYTES);
        ControlJson.required(o, "kind", "version", "id", "instanceId", "generation", "incarnation", "keyId",
                "hostFingerprint", "expiresAt", "networkId", "localUfrag", "localPassword", "offer");
        boolean diagnostic = "0".equals(ControlJson.string(o, "networkId"));
        if (diagnostic == o.has("cpk")) {
            throw new IllegalArgumentException("Invalid assisted join identity");
        }
        if (!ControlJson.string(o, "kind").equals("assisted-join") || ControlJson.number(o, "version") != 1) {
            throw new IllegalArgumentException("Invalid assisted join kind");
        }
        return new AssistedJoin(
                ControlJson.string(o, "id"),
                ControlJson.string(o, "instanceId"),
                ControlJson.number(o, "generation"),
                ControlJson.string(o, "incarnation"),
                ControlJson.string(o, "keyId"),
                DtlsFingerprint.parse(ControlJson.string(o, "hostFingerprint")),
                ControlJson.number(o, "expiresAt"),
                ControlJson.string(o, "networkId"),
                diagnostic ? null : ControlJson.string(o, "cpk"),
                ControlJson.string(o, "localUfrag"),
                ControlJson.string(o, "localPassword"),
                ControlJson.string(o, "offer"));
    }

    /**
     * The client's ICE credentials, DTLS fingerprint and candidates, in the order the offer lists them.
     *
     * @param candidates 1 to 32 numeric UDP endpoints for component 1
     */
    public record Offer(String ufrag, String password, DtlsFingerprint fingerprint, List<InetSocketAddress> candidates) {
        public Offer {
            candidates = List.copyOf(candidates);
        }

        @Override
        public String toString() {
            return "AssistedJoin.Offer[candidates=" + candidates.size() + "]";
        }
    }

    /**
     * Checks the offer against the profile control-v1 allows: one application section with
     * {@code UDP/DTLS/SCTP webrtc-datachannel}, {@code a=setup:actpass}, SCTP port 5000, maximum message size 262144,
     * single ICE credentials and fingerprint, no {@code a=ice-lite}, and 1 to 32 numeric UDP candidates for
     * component 1.
     *
     * @throws IllegalArgumentException If the host cannot serve the offer, which refuses the join
     */
    public Offer parseOffer() {
        List<String> lines = offer.lines().toList();
        if (lines.size() > 256
                || lines.stream().anyMatch(l -> l.indexOf('\0') >= 0)
                || !one(lines, "m=").matches("application [0-9]{1,5} UDP/DTLS/SCTP webrtc-datachannel")
                || !one(lines, "a=setup:").equals("actpass")
                || !one(lines, "a=sctp-port:").equals("5000")
                || !one(lines, "a=max-message-size:").equals("262144")
                || lines.contains("a=ice-lite")) {
            throw new IllegalArgumentException("Unsupported assisted offer");
        }
        String ufrag = one(lines, "a=ice-ufrag:"), password = one(lines, "a=ice-pwd:");
        if (!ice(ufrag, 4) || !ice(password, 22)) {
            throw new IllegalArgumentException("Invalid assisted ICE credentials");
        }
        DtlsFingerprint fingerprint = DtlsFingerprint.parse(one(lines, "a=fingerprint:"));
        List<InetSocketAddress> candidates = new ArrayList<>();
        for (String line : lines) {
            if (!line.startsWith("a=candidate:")) {
                continue;
            }
            String[] c = line.substring(12).split(" ");
            if (c.length < 8
                    || c.length > 24
                    || !c[1].equals("1")
                    || !c[2].equalsIgnoreCase("UDP")
                    || !c[6].equals("typ")
                    || !Set.of("host", "srflx", "prflx", "relay").contains(c[7])
                    || !c[5].matches("[0-9]{1,5}")
                    || !(NetUtil.isValidIpV4Address(c[4]) || NetUtil.isValidIpV6Address(c[4]))) {
                throw new IllegalArgumentException("Invalid assisted candidate");
            }
            int port = Integer.parseInt(c[5]);
            if (port < 1 || port > 65535 || candidates.size() == 32) {
                throw new IllegalArgumentException("Assisted candidate limit");
            }
            candidates.add(new InetSocketAddress(NetUtil.createInetAddressFromIpAddressString(c[4]), port));
        }
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("Assisted offer needs a numeric candidate");
        }
        return new Offer(ufrag, password, fingerprint, candidates);
    }

    /**
     * The provider must send the key in its canonical form: standard base64 of the
     * {@link IdentityPublicKey#canonical canonical} SPKI DER. Any other spelling of a key is refused rather than
     * normalized.
     *
     * @return The player's key as canonical SPKI DER, 120 bytes
     * @throws IllegalArgumentException If this is a diagnostic or the key is not a canonical P-384 key
     */
    public byte[] canonicalCpk() {
        if (cpk == null) {
            throw new IllegalArgumentException("A diagnostic join carries no client key");
        }
        byte[] der;
        PublicKey key;
        try {
            der = Base64.getDecoder().decode(cpk);
            key = KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(der));
        } catch (GeneralSecurityException | IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid assisted CPK", invalid);
        }
        if (!Arrays.equals(der, IdentityPublicKey.canonical(key))
                || !Base64.getEncoder().encodeToString(der).equals(cpk)) {
            throw new IllegalArgumentException("Noncanonical assisted CPK");
        }
        return der;
    }

    private static String one(List<String> lines, String prefix) {
        var found = lines.stream().filter(l -> l.startsWith(prefix)).toList();
        if (found.size() != 1) {
            throw new IllegalArgumentException("Ambiguous assisted offer");
        }
        return found.getFirst().substring(prefix.length());
    }

    private static boolean ice(String s, int min) {
        return s != null && s.length() >= min && s.length() <= 256 && s.matches("[A-Za-z0-9+/]+");
    }

    @Override
    public String toString() {
        return "AssistedJoin[id=" + id + "]";
    }
}

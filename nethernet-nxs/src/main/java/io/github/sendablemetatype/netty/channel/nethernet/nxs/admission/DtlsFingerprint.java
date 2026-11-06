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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * A SHA-256 certificate fingerprint in its SDP form, {@code sha-256} followed by 32 colon-separated uppercase hex
 * bytes. Published and accepted with one spelling.
 *
 * @param value The SDP form
 */
public record DtlsFingerprint(String value) {
    private static final Pattern CANONICAL = Pattern.compile("sha-256 ([0-9A-F]{2}:){31}[0-9A-F]{2}");
    private static final Pattern ANY_CASE = Pattern.compile("sha-256 ([0-9A-Fa-f]{2}:){31}[0-9A-Fa-f]{2}");
    private static final HexFormat SDP = HexFormat.ofDelimiter(":").withUpperCase();

    public DtlsFingerprint {
        if (value == null || !CANONICAL.matcher(value).matches()) {
            throw new IllegalArgumentException("Expected sha-256 and 32 uppercase hex bytes");
        }
    }

    /**
     * @param sha256 The certificate's SHA-256 digest, 32 bytes
     */
    public static DtlsFingerprint of(byte[] sha256) {
        if (sha256.length != 32) {
            throw new IllegalArgumentException("A SHA-256 digest has 32 bytes");
        }
        return new DtlsFingerprint("sha-256 " + SDP.formatHex(sha256));
    }

    /**
     * Reads a fingerprint whose hex digits may be in either case, as SDP and the assisted-join frame allow.
     *
     * @throws IllegalArgumentException If it is not a SHA-256 fingerprint
     */
    public static DtlsFingerprint parse(String value) {
        if (value == null || !ANY_CASE.matcher(value).matches()) {
            throw new IllegalArgumentException("Expected a sha-256 fingerprint");
        }
        return new DtlsFingerprint("sha-256 " + value.substring(8).toUpperCase(Locale.ROOT));
    }

    /** The digest as 64 lowercase hex digits, as admission tokens carry it. */
    public String hex() {
        return value.substring(8).replace(":", "").toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return value;
    }
}

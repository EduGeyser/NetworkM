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

import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.spec.ECFieldFp;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;

// Replaced by the transport's IdentityPublicKey when the lanes merge.
/**
 * The canonical client key ({@code cpk}) of an assisted join: standard base64 of the 120-byte named secp384r1 SPKI
 * DER with an uncompressed point that lies on the curve. Any other spelling of a key is refused rather than
 * normalized, since the provider must send the canonical form.
 */
final class CanonicalCpk {
    private static final byte[] PREFIX = HexFormat.of().parseHex("3076301006072a8648ce3d020106052b8104002203620004");
    private static final ECParameterSpec CURVE;

    static {
        try {
            AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
            parameters.init(new ECGenParameterSpec("secp384r1"));
            CURVE = parameters.getParameterSpec(ECParameterSpec.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private CanonicalCpk() {
    }

    /**
     * @return The canonical SPKI DER, 120 bytes
     * @throws IllegalArgumentException If the value is not a canonical P-384 key
     */
    static byte[] decode(String cpk) {
        byte[] der;
        try {
            der = Base64.getDecoder().decode(cpk);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid assisted CPK", invalid);
        }
        if (der.length != 120 || !Arrays.equals(der, 0, PREFIX.length, PREFIX, 0, PREFIX.length)
                || !Base64.getEncoder().encodeToString(der).equals(cpk)) {
            throw new IllegalArgumentException("Noncanonical assisted CPK");
        }
        BigInteger x = new BigInteger(1, Arrays.copyOfRange(der, PREFIX.length, PREFIX.length + 48));
        BigInteger y = new BigInteger(1, Arrays.copyOfRange(der, PREFIX.length + 48, 120));
        BigInteger p = ((ECFieldFp) CURVE.getCurve().getField()).getP();
        if (x.compareTo(p) >= 0 || y.compareTo(p) >= 0 || !y.multiply(y).mod(p).equals(
                x.pow(3).add(CURVE.getCurve().getA().multiply(x)).add(CURVE.getCurve().getB()).mod(p))) {
            throw new IllegalArgumentException("Assisted CPK is not a secp384r1 point");
        }
        return der;
    }
}

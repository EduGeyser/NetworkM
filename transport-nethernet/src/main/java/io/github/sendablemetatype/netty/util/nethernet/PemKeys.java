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

import org.bouncycastle.asn1.ASN1BitString;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.sec.ECPrivateKey;
import org.bouncycastle.asn1.sec.SECObjectIdentifiers;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.ECNamedCurveTable;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers;
import org.bouncycastle.math.ec.FixedPointCombMultiplier;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemObject;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes the server identity key as standard PEM.
 * <p>
 * Reading accepts an unencrypted secp384r1 private key as SEC1 ({@code EC PRIVATE KEY}) or PKCS#8
 * ({@code PRIVATE KEY}), optionally followed or preceded by its {@code PUBLIC KEY} block, and
 * ignores an {@code EC PARAMETERS} block as {@code openssl ecparam -genkey} writes one. The public
 * point is always derived from the private scalar; a point the file states, embedded in the key or
 * in its own block, must equal it. That reads EduGeyser's key file, a JDK PKCS#8 key (which omits
 * the point) followed by a {@code PUBLIC KEY} block.
 */
final class PemKeys {
    private static final int FIELD_BYTES = 48;
    private static final X9ECParameters CURVE = ECNamedCurveTable.getByOID(SECObjectIdentifiers.secp384r1);
    private static final AlgorithmIdentifier EC_P384 =
            new AlgorithmIdentifier(X9ObjectIdentifiers.id_ecPublicKey, SECObjectIdentifiers.secp384r1);

    private PemKeys() {
    }

    /**
     * @param armour the file's text
     * @param source names the file in error messages
     * @return the key pair, its public half derived from the private scalar
     * @throws GeneralSecurityException if the text holds no usable secp384r1 key, or a stated public
     *                                  key that does not belong to it
     */
    static KeyPair read(String armour, String source) throws GeneralSecurityException {
        List<Object> blocks = new ArrayList<>();
        try (PEMParser parser = new PEMParser(new StringReader(armour))) {
            for (Object block; (block = parser.readObject()) != null; ) {
                blocks.add(block);
            }
        } catch (IOException | RuntimeException malformed) {
            throw new GeneralSecurityException("Cannot read the PEM in " + source, malformed);
        }
        if (blocks.isEmpty()) {
            throw new GeneralSecurityException("No PEM block found in " + source);
        }
        PrivateKeyInfo privateKey = null;
        SubjectPublicKeyInfo statedPublic = null;
        for (Object block : blocks) {
            if (block instanceof ASN1ObjectIdentifier) {
                continue; // EC PARAMETERS; the key's own parameters decide the curve
            }
            if (privateKey == null && block instanceof PEMKeyPair pair) {
                privateKey = pair.getPrivateKeyInfo();
            } else if (privateKey == null && block instanceof PrivateKeyInfo info) {
                privateKey = info;
            } else if (statedPublic == null && block instanceof SubjectPublicKeyInfo info) {
                statedPublic = info;
            } else {
                throw new GeneralSecurityException("Expected one unencrypted EC private key and at most one public key in "
                        + source + ", found " + block.getClass().getSimpleName());
            }
        }
        if (privateKey == null) {
            throw new GeneralSecurityException("No EC private key in " + source);
        }

        BigInteger scalar;
        byte[] derivedPoint;
        try {
            ECPrivateKey sec1 = ECPrivateKey.getInstance(privateKey.parsePrivateKey());
            requireP384(privateKey.getPrivateKeyAlgorithm(), sec1.getParametersObject(), source);
            scalar = sec1.getKey();
            if (scalar.signum() <= 0 || scalar.compareTo(CURVE.getN()) >= 0) {
                throw new GeneralSecurityException("The private key in " + source + " is out of range");
            }
            derivedPoint = new FixedPointCombMultiplier().multiply(CURVE.getG(), scalar).normalize().getEncoded(false);
            ASN1BitString embedded = sec1.getPublicKey();
            if (embedded != null && !MessageDigest.isEqual(embedded.getOctets(), derivedPoint)) {
                throw mismatch(source);
            }
            if (statedPublic != null && (!EC_P384.equals(statedPublic.getAlgorithm())
                    || !MessageDigest.isEqual(statedPublic.getPublicKeyData().getOctets(), derivedPoint))) {
                throw mismatch(source);
            }
        } catch (IOException | RuntimeException malformed) {
            throw new GeneralSecurityException("Cannot read the EC private key in " + source, malformed);
        }

        try {
            KeyFactory factory = KeyFactory.getInstance("EC");
            byte[] pkcs8 = new PrivateKeyInfo(EC_P384,
                    new ECPrivateKey(FIELD_BYTES * 8, scalar, SECObjectIdentifiers.secp384r1)).getEncoded();
            byte[] spki = new SubjectPublicKeyInfo(EC_P384, derivedPoint).getEncoded();
            return new KeyPair(factory.generatePublic(new X509EncodedKeySpec(spki)),
                    factory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8)));
        } catch (IOException e) {
            throw new GeneralSecurityException("Cannot encode the key from " + source, e);
        }
    }

    /**
     * Writes SEC1 with the public point embedded, the form {@code openssl ecparam -genkey -noout}
     * writes, rather than the JDK's PKCS#8, which drops the point.
     */
    static String writeSec1(KeyPair pair) throws IOException {
        SubjectPublicKeyInfo publicKey = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
        ECPrivateKey scalar = ECPrivateKey.getInstance(
                PrivateKeyInfo.getInstance(pair.getPrivate().getEncoded()).parsePrivateKey());

        StringWriter out = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(out)) {
            writer.writeObject(new PemObject("EC PRIVATE KEY", new ECPrivateKey(FIELD_BYTES * 8,
                    scalar.getKey(), publicKey.getPublicKeyData(), SECObjectIdentifiers.secp384r1).getEncoded()));
        }
        return out.toString();
    }

    private static void requireP384(AlgorithmIdentifier algorithm, ASN1Encodable sec1Parameters, String source)
            throws GeneralSecurityException {
        boolean outer = X9ObjectIdentifiers.id_ecPublicKey.equals(algorithm.getAlgorithm())
                && SECObjectIdentifiers.secp384r1.equals(algorithm.getParameters());
        boolean inner = sec1Parameters == null || SECObjectIdentifiers.secp384r1.equals(sec1Parameters);
        if (!outer || !inner) {
            throw new GeneralSecurityException("The key in " + source + " is not a named secp384r1 (P-384) key");
        }
    }

    private static GeneralSecurityException mismatch(String source) {
        return new GeneralSecurityException("The private and public keys in " + source + " do not match");
    }
}

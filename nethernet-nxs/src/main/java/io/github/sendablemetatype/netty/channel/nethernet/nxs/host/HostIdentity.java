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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.engine.DtlsIdentity;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The one DTLS certificate every peer of an NXS host presents. Stateless clients build the host's answer themselves
 * from the published fingerprint, so the endpoint cannot let the engine make a certificate per peer. The host
 * supplies the certificate and key as PEM; this class checks them once, when it loads them, and never creates a
 * peer to learn the fingerprint. How a host makes one is not ruled yet (H-7).
 */
public final class HostIdentity {
    /** The most characters a PEM file may have. */
    static final int MAX_PEM_LENGTH = 65536;
    private static final Pattern PRIVATE_KEY =
            Pattern.compile("-----BEGIN PRIVATE KEY-----([A-Za-z0-9+/=\\s]+)-----END PRIVATE KEY-----");
    /**
     * The signature algorithms allowed, by OID: libwebrtc takes the fingerprint's digest from the certificate's
     * signature algorithm ({@code rtc_base/ssl_fingerprint.cc}), and NXS publishes a SHA-256 fingerprint, so a
     * certificate signed with another digest would make the answer name another fingerprint than the profile.
     */
    private static final Map<String, String> SHA256_SIGNATURES = Map.of(
            "1.2.840.10045.4.3.2", "SHA256withECDSA",
            "1.2.840.113549.1.1.11", "SHA256withRSA");

    private final DtlsIdentity dtls;
    private final DtlsFingerprint fingerprint;

    private HostIdentity(DtlsIdentity dtls, DtlsFingerprint fingerprint) {
        this.dtls = dtls;
        this.fingerprint = fingerprint;
    }

    /**
     * Reads the identity from PEM files: an X.509 certificate and its PKCS#8 private key.
     *
     * @throws IOException              If a file cannot be read or is larger than 64 KiB
     * @throws GeneralSecurityException If the certificate or key cannot be read, see {@link #of}
     */
    public static HostIdentity load(Path certificate, Path privateKey) throws IOException, GeneralSecurityException {
        if (Files.size(certificate) > MAX_PEM_LENGTH || Files.size(privateKey) > MAX_PEM_LENGTH) {
            throw new IOException("Oversized PEM identity");
        }
        return of(Files.readString(certificate, StandardCharsets.US_ASCII),
                Files.readString(privateKey, StandardCharsets.US_ASCII));
    }

    /**
     * Checks an identity given as PEM text.
     *
     * @param certificatePem An X.509 certificate signed with SHA-256, ECDSA or RSA
     * @param privateKeyPem  The certificate's private key, PKCS#8 ({@code BEGIN PRIVATE KEY})
     * @throws GeneralSecurityException If the certificate or key cannot be read, the certificate is not signed with
     *                                  SHA-256, or the key is not the certificate's
     */
    public static HostIdentity of(String certificatePem, String privateKeyPem) throws GeneralSecurityException {
        if (certificatePem.length() > MAX_PEM_LENGTH || privateKeyPem.length() > MAX_PEM_LENGTH) {
            throw new GeneralSecurityException("Oversized PEM identity");
        }
        X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(certificatePem.getBytes(StandardCharsets.US_ASCII)));
        String signature = SHA256_SIGNATURES.get(certificate.getSigAlgOID());
        if (signature == null) {
            throw new GeneralSecurityException("The DTLS certificate is signed with " + certificate.getSigAlgName()
                    + "; it must be signed with SHA256withECDSA or SHA256withRSA, as libwebrtc takes the fingerprint's "
                    + "digest from the signature and NXS publishes a SHA-256 fingerprint");
        }
        PrivateKey key = privateKey(privateKeyPem, certificate.getPublicKey().getAlgorithm());
        requirePair(certificate, key, signature);
        byte[] der = certificate.getEncoded();
        DtlsFingerprint fingerprint = DtlsFingerprint.of(MessageDigest.getInstance("SHA-256").digest(der));
        return new HostIdentity(new DtlsIdentity(certificatePem, privateKeyPem), fingerprint);
    }

    private static PrivateKey privateKey(String pem, String algorithm) throws GeneralSecurityException {
        Matcher block = PRIVATE_KEY.matcher(pem);
        if (!block.find()) {
            throw new GeneralSecurityException("A PKCS#8 PEM private key (BEGIN PRIVATE KEY) is required");
        }
        byte[] der;
        try {
            der = Base64.getMimeDecoder().decode(block.group(1));
        } catch (IllegalArgumentException e) {
            throw new GeneralSecurityException("Unreadable PEM private key", e);
        }
        try {
            return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
        } finally {
            Arrays.fill(der, (byte) 0);
        }
    }

    /** Signs a random challenge with the key and checks it with the certificate. */
    private static void requirePair(X509Certificate certificate, PrivateKey key, String algorithm)
            throws GeneralSecurityException {
        byte[] challenge = new byte[32];
        new SecureRandom().nextBytes(challenge);
        Signature signer = Signature.getInstance(algorithm);
        signer.initSign(key);
        signer.update(challenge);
        byte[] signed = signer.sign();
        signer.initVerify(certificate.getPublicKey());
        signer.update(challenge);
        if (!signer.verify(signed)) {
            throw new GeneralSecurityException("The DTLS private key does not belong to the certificate");
        }
    }

    /** The certificate and key, as every peer of the endpoint presents them. */
    public DtlsIdentity dtls() {
        return dtls;
    }

    /** The certificate's SHA-256 fingerprint, as the host profile publishes it. */
    public DtlsFingerprint fingerprint() {
        return fingerprint;
    }

    @Override
    public String toString() {
        return "HostIdentity[fingerprint=" + fingerprint + "]";
    }
}

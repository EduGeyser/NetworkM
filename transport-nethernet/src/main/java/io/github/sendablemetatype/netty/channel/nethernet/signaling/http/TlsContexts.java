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

package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * Builds server TLS contexts for HTTP signaling from files, with errors that name the file and
 * the problem. Load once and serve the result from a {@link TlsContextSelector}; load again to
 * rotate a certificate.
 */
public final class TlsContexts {

    private TlsContexts() {
    }

    /**
     * Reads a PEM certificate chain and its private key, the form most certificate authorities
     * hand out.
     *
     * @param certificateChain the chain, leaf first
     * @param privateKey       the leaf's private key, PKCS#8
     * @param keyPassword      the key's password, or null when it is not encrypted
     * @return the context
     * @throws IllegalArgumentException if either file cannot be read or they do not form a pair
     */
    public static SslContext fromPem(Path certificateChain, Path privateKey, String keyPassword) {
        // Netty reports an unreadable file as missing, so each is opened here first to say which and why
        requireReadable(certificateChain, "certificate chain");
        requireReadable(privateKey, "private key");
        try {
            return SslContextBuilder.forServer(certificateChain.toFile(), privateKey.toFile(), keyPassword).build();
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot use the TLS certificate chain " + certificateChain
                    + " with the private key " + privateKey + ": " + e.getMessage(), e);
        }
    }

    /**
     * Reads a PKCS12 keystore holding the certificate chain and its key.
     *
     * @param keystore the keystore
     * @param password its password, empty when it has none
     * @return the context
     * @throws IllegalArgumentException if the keystore cannot be read or opened
     */
    public static SslContext fromPkcs12(Path keystore, String password) {
        requireReadable(keystore, "keystore");
        char[] secret = password.toCharArray();
        try (InputStream input = Files.newInputStream(keystore)) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(input, secret);
            KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(store, secret);
            return SslContextBuilder.forServer(factory).build();
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot read the TLS keystore " + keystore + ": " + e.getMessage(), e);
        }
    }

    private static void requireReadable(Path file, String what) {
        try (InputStream ignored = Files.newInputStream(file)) {
            // Readable
        } catch (NoSuchFileException e) {
            throw new IllegalArgumentException("The TLS " + what + " " + file + " does not exist", e);
        } catch (AccessDeniedException e) {
            throw new IllegalArgumentException("The TLS " + what + " " + file
                    + " cannot be read with the permissions this server runs with", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read the TLS " + what + " " + file + ": " + e.getMessage(), e);
        }
    }
}

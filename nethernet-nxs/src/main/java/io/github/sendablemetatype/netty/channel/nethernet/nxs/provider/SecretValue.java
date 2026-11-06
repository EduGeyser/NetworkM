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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.provider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * A secret a host configured: the value itself, or {@code file:} followed by the path of a file holding it, relative
 * to the configuration directory unless absolute. A configuration file is read by anyone who can read its directory
 * and ends up in support requests; a file reference keeps the secret out of it.
 *
 * <p>Only the prefix makes a value a file reference. A value without it that looks like a path is refused instead of
 * being used as the secret, so a reference that lost its prefix never reaches the provider as a bearer token. The
 * test must not refuse a real secret. The secret is a bearer token, an RFC 6750 {@code b64token}: letters, digits and
 * {@code - . _ ~ + /}, then {@code =} padding. It may start with {@code /} or {@code ~/}, so no prefix of that
 * alphabet marks a path. A value is refused only when, taken as a path, it names a file or directory that exists, or
 * when it starts with a Windows drive ({@code C:\} or {@code C:/}), which has a {@code :} no {@code b64token} has.
 */
final class SecretValue {
    /** Above this a file is something other than a secret, and reading it is a waste. */
    private static final long MAX_BYTES = 16384;
    private static final String FILE = "file:";
    private static final Pattern DRIVE = Pattern.compile("[A-Za-z]:[\\\\/].*");

    private SecretValue() {
    }

    /**
     * The file's trailing line terminator is dropped, since a file written by {@code echo} or an editor has one and it
     * is never part of the secret.
     *
     * @param directory The directory a relative path is resolved against; null to resolve absolute paths only
     * @return The secret: the file's content for a reference, else the value unchanged
     * @throws IOException If the value names a file that cannot be read, or looks like a path without the prefix. The
     *                     message never contains the value.
     */
    static String resolve(String value, Path directory) throws IOException {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (!trimmed.startsWith(FILE)) {
            if (looksLikePath(trimmed, directory)) {
                throw new IOException("looks like a file path; write file:<path> to read the secret from a file");
            }
            return value;
        }
        try {
            Path source = path(trimmed.substring(FILE.length()), directory);
            if (!Files.isRegularFile(source)) {
                throw new IOException("not a readable file");
            }
            if (Files.size(source) > MAX_BYTES) {
                throw new IOException("larger than " + MAX_BYTES + " bytes");
            }
            return stripLineTerminator(Files.readString(source));
        } catch (IOException | RuntimeException e) {
            // The value is a path here rather than the secret, but the path may be malformed in ways that echo it
            throw new IOException("names a file that is missing, unreadable or larger than " + MAX_BYTES
                    + " bytes (" + e.getClass().getSimpleName() + ")");
        }
    }

    private static boolean looksLikePath(String value, Path directory) {
        if (value.isEmpty()) {
            return false;
        }
        if (DRIVE.matcher(value).matches()) {
            return true;
        }
        try {
            Path path = Path.of(value);
            return (path.isAbsolute() || directory != null) && Files.exists(path(value, directory));
        } catch (InvalidPathException | SecurityException notAPath) {
            return false;
        }
    }

    private static Path path(String value, Path directory) {
        Path path = Path.of(value);
        return (path.isAbsolute() || directory == null ? path : directory.resolve(path)).normalize();
    }

    private static String stripLineTerminator(String value) {
        if (value.endsWith("\r\n")) {
            return value.substring(0, value.length() - 2);
        }
        if (value.endsWith("\n") || value.endsWith("\r")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }
}

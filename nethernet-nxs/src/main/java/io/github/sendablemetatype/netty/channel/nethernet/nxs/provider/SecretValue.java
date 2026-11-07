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

/**
 * A secret a host configured: the value itself, or a file that holds it. A configuration file is read by anyone who
 * can read its directory and ends up in support requests; a file keeps the secret out of it.
 *
 * <p>The rule is upstream CloudburstMC/Network's, so a configuration means the same with either library: a value
 * names a file when it starts with {@code file:}, {@code /}, {@code ./} or {@code ../}, or is an absolute path on this
 * platform (such as {@code C:\secret} on Windows). Any other value is the secret itself. A value that starts with
 * {@code /} can therefore not be given inline; put it in a file.
 */
final class SecretValue {
    /** Above this a file is something other than a secret, and reading it is a waste. */
    private static final long MAX_BYTES = 16384;
    private static final String FILE = "file:";

    private SecretValue() {
    }

    /**
     * The file's trailing line terminator is dropped, since a file written by {@code echo} or an editor has one and it
     * is never part of the secret.
     *
     * @param directory The directory a relative path is resolved against; null to resolve it against the working
     *                  directory
     * @return The secret: the content of the file the value names, else the value unchanged
     * @throws IOException If the value names a file that is not a regular file of at most 16 KiB or cannot be read.
     *                     The message names the path, never the value.
     */
    static String resolve(String value, Path directory) throws IOException {
        if (value == null || value.isBlank()) {
            return value;
        }
        String trimmed = value.trim();
        if (!isFileReference(trimmed)) {
            return value;
        }
        Path source;
        try {
            source = path(trimmed.startsWith(FILE) ? trimmed.substring(FILE.length()) : trimmed, directory);
        } catch (InvalidPathException e) {
            // Its message repeats the input, which is a path here, but one that names no file
            throw new IOException("names no valid file path");
        }
        try {
            if (!Files.isRegularFile(source)) {
                throw new IOException("not a regular file");
            }
            if (Files.size(source) > MAX_BYTES) {
                throw new IOException("larger than " + MAX_BYTES + " bytes");
            }
            return stripLineTerminator(Files.readString(source));
        } catch (IOException | RuntimeException e) {
            throw new IOException("names the file " + source + ", which cannot be read (" + e.getMessage() + ")", e);
        }
    }

    private static boolean isFileReference(String trimmed) {
        return trimmed.startsWith(FILE) || trimmed.startsWith("/") || trimmed.startsWith("./")
                || trimmed.startsWith("../") || isAbsolutePath(trimmed);
    }

    private static boolean isAbsolutePath(String value) {
        try {
            return Path.of(value).isAbsolute();
        } catch (InvalidPathException notAPath) {
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

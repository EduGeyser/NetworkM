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
import java.nio.file.Path;

/**
 * A secret a host configured: the value itself, or {@code file:} followed by the path of a file holding it. Only the
 * prefix makes a value a file reference, since a literal secret may well start with {@code /}. A configuration file
 * is read by anyone who can read its directory and ends up in support requests; a file reference keeps the secret
 * out of it.
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
     * @param directory The directory a relative path is resolved against
     * @return The secret, or the value unchanged when it names no file
     * @throws IOException If the value names a file that cannot be read; the message never contains the secret
     */
    static String resolve(String value, Path directory) throws IOException {
        if (value == null || !value.trim().startsWith(FILE)) {
            return value;
        }
        try {
            Path path = Path.of(value.trim().substring(FILE.length()));
            Path source = (path.isAbsolute() || directory == null ? path : directory.resolve(path)).normalize();
            if (!Files.isRegularFile(source)) {
                throw new IOException("not a readable file");
            }
            if (Files.size(source) > MAX_BYTES) {
                throw new IOException("larger than " + MAX_BYTES + " bytes");
            }
            return stripLineTerminator(Files.readString(source));
        } catch (IOException | RuntimeException e) {
            // The value is a path here rather than the secret, but the path may be malformed in ways that echo it
            throw new IOException("Could not read the secret file: " + e.getClass().getSimpleName());
        }
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

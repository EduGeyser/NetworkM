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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/**
 * One logical instance owns this directory and key; never share or clone it across live instances. The
 * directory and the state file are readable by their owner only, through POSIX modes or, on Windows, an ACL.
 */
public final class ProviderStateStore implements AutoCloseable {
    private final Path directory, stateFile;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private boolean closed;

    /**
     * Opens the state directory, creating it readable by its owner only, and locks it against a second client.
     *
     * @throws IOException If the directory cannot be created or another client holds it
     */
    public ProviderStateStore(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath();
        this.stateFile = this.directory.resolve("provider-state.json");
        OwnerOnlyFiles.createDirectories(this.directory);
        if (Files.isSymbolicLink(this.directory) || Files.isSymbolicLink(stateFile)) {
            throw new IOException("State paths must not be symbolic links");
        }
        OwnerOnlyFiles.restrict(this.directory);
        Path lockFile = this.directory.resolve("provider.lock");
        if (Files.isSymbolicLink(lockFile)) {
            throw new IOException("Lock must not be a symbolic link");
        }
        lockChannel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired;
        try {
            acquired = lockChannel.tryLock();
        } catch (OverlappingFileLockException e) {
            lockChannel.close();
            throw new IOException("State directory is already active", e);
        }
        if (acquired == null) {
            lockChannel.close();
            throw new IOException("State directory is already active");
        }
        lock = acquired;
    }

    synchronized JsonObject read() throws IOException {
        requireOwnership();
        if (!Files.exists(stateFile)) {
            return new JsonObject();
        }
        if (Files.size(stateFile) > 262144) {
            throw new IOException("State exceeds limit");
        }
        OwnerOnlyFiles.restrict(stateFile);
        return JsonParser.parseString(Files.readString(stateFile)).getAsJsonObject();
    }

    synchronized void write(JsonObject state) throws IOException {
        requireOwnership();
        Path tmp = OwnerOnlyFiles.createTempFile(directory, "provider-state-", ".tmp");
        try {
            byte[] bytes = new GsonBuilder().disableHtmlEscaping().create().toJson(state)
                    .getBytes(StandardCharsets.UTF_8);
            try (FileChannel file = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ByteBuffer b = ByteBuffer.wrap(bytes);
                while (b.hasRemaining()) {
                    file.write(b);
                }
                file.force(true);
            }
            Files.move(tmp, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            if (OwnerOnlyFiles.POSIX) {
                // Persists the rename itself. Windows cannot open a directory as a file to ask for it.
                try (FileChannel dir = FileChannel.open(directory, StandardOpenOption.READ)) {
                    dir.force(true);
                }
            }
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private void requireOwnership() throws IOException {
        if (closed || !lock.isValid()) {
            throw new IOException("Provider state ownership is closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        // Queued callbacks may drain after client shutdown. Serialize the entire IO operation with
        // lock release so they can never read or replace a successor owner's state file.
        closed = true;
        try {
            lock.release();
        } finally {
            lockChannel.close();
        }
    }
}

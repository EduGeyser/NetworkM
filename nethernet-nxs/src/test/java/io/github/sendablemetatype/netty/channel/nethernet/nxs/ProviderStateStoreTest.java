package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ProviderStateStoreTest {
    /** Readable by its owner alone: POSIX modes where there are any, otherwise one ACL entry for the owner. */
    static void assertOwnerOnly(Path path) throws IOException {
        boolean directory = Files.isDirectory(path);
        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            assertEquals(directory ? "rwx------" : "rw-------",
                    PosixFilePermissions.toString(Files.getPosixFilePermissions(path)), path.toString());
            return;
        }
        var acl = Files.getFileAttributeView(path, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        assertNotNull(acl, "Neither POSIX modes nor ACLs");
        var entries = acl.getAcl();
        assertEquals(1, entries.size(), entries.toString());
        assertEquals(AclEntryType.ALLOW, entries.getFirst().type());
        assertEquals(acl.getOwner(), entries.getFirst().principal());
    }

    @Test
    void theDirectoryAndTheStateAreReadableByTheirOwnerOnly(@TempDir Path parent) throws Exception {
        Path directory = parent.resolve("state");
        try (var store = new ProviderStateStore(directory)) {
            store.write(state(1));
            assertOwnerOnly(directory);
            assertOwnerOnly(directory.resolve("provider-state.json"));
            store.write(state(2));
            assertOwnerOnly(directory.resolve("provider-state.json"));
        }
    }

    private static JsonObject state(int revision) {
        var value = new JsonObject();
        value.addProperty("revision", revision);
        return value;
    }

    @Test
    void releasedStoreCannotReadOrOverwriteSuccessor(@TempDir Path directory) throws Exception {
        var previous = new ProviderStateStore(directory);
        previous.write(state(1));
        previous.close();
        try (var successor = new ProviderStateStore(directory)) {
            successor.write(state(2));
            assertThrows(IOException.class, () -> previous.write(state(1)));
            assertThrows(IOException.class, previous::read);
            assertDoesNotThrow(previous::close);
            assertEquals(state(2), successor.read());
        }
    }

    @Test
    void ownershipIsNotReleasedDuringAnInFlightWrite(@TempDir Path directory) throws Exception {
        var serializing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var closing = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        var previous = new ProviderStateStore(directory);
        try {
            var value = state(1);
            value.addProperty(
                    "barrier",
                    new Number() {
                        @Override
                        public int intValue() {
                            return 1;
                        }

                        @Override
                        public long longValue() {
                            return 1;
                        }

                        @Override
                        public float floatValue() {
                            return 1;
                        }

                        @Override
                        public double doubleValue() {
                            return 1;
                        }

                        @Override
                        public String toString() {
                            serializing.countDown();
                            try {
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("write barrier timed out");
                                }
                            } catch (InterruptedException error) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(error);
                            }
                            return "1";
                        }
                    });
            var writing =
                    executor.submit(
                            () -> {
                                previous.write(value);
                                return null;
                            });
            assertTrue(serializing.await(5, TimeUnit.SECONDS));
            var closed =
                    executor.submit(
                            () -> {
                                closing.countDown();
                                previous.close();
                                return null;
                            });
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> closed.get(150, TimeUnit.MILLISECONDS));
            assertThrows(
                    IOException.class,
                    () -> {
                        try (var ignored = new ProviderStateStore(directory)) {}
                    });
            release.countDown();
            writing.get(5, TimeUnit.SECONDS);
            closed.get(5, TimeUnit.SECONDS);
            try (var successor = new ProviderStateStore(directory)) {
                successor.write(state(2));
                assertThrows(IOException.class, () -> previous.write(state(1)));
                assertEquals(state(2), successor.read());
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            previous.close();
        }
    }
}

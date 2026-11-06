package io.github.sendablemetatype.netty.util.nethernet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A PEM file holding a private key that a server creates once and never replaces, because clients pin what it
 * holds. {@link ServerIdentity} and {@link DtlsCertificate} keep their keys this way.
 */
final class KeyFile {
    private static final Set<PosixFilePermission> OWNER_ONLY = PosixFilePermissions.fromString("rw-------");

    /** How {@link #ensure} found the file, worded for a log line. */
    enum Origin {
        EXISTING("Loaded"),
        CREATED("Created"),
        CONCURRENTLY_CREATED("Loaded the concurrently created");

        private final String verb;

        Origin(String verb) {
            this.verb = verb;
        }

        String verb() {
            return verb;
        }
    }

    /** Makes the text of a new file; asked only when the file does not exist. */
    @FunctionalInterface
    interface Contents {
        String create() throws IOException, GeneralSecurityException;
    }

    private KeyFile() {
    }

    /**
     * Makes sure the file exists, creating it from {@code contents} when it does not. The caller then reads it.
     * <p>
     * An existing file is used as it is and may be a symbolic link (Kubernetes mounts secrets that way); a regular
     * file readable by others is tightened to owner-only on POSIX. Creating refuses a symbolic link at the path,
     * writes the text owner-only (POSIX permissions, or an owner-only ACL on Windows), syncs it to disk, and links it
     * into place only if no file appeared meanwhile; when another process or thread won that race, its file stays.
     *
     * @param path     the file, absolute
     * @param what     names the file in error messages, such as "the identity key"
     * @param contents makes the text of a new file
     * @return how the file was found
     * @throws IOException              if the file cannot be created
     * @throws GeneralSecurityException if {@code contents} fails
     */
    static Origin ensure(Path path, String what, Contents contents) throws IOException, GeneralSecurityException {
        if (Files.exists(path)) {
            tightenExisting(path);
            return Origin.EXISTING;
        }
        if (Files.isSymbolicLink(path)) {
            throw new IOException("Refusing to create " + what + " through a symbolic link: " + path);
        }
        Path directory = path.getParent();
        Files.createDirectories(directory);
        boolean posix = Files.getFileStore(directory).supportsFileAttributeView(PosixFileAttributeView.class);

        Path temporary = createOwnerOnly(directory, path.getFileName().toString(), posix);
        boolean created;
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer text = ByteBuffer.wrap(contents.create().getBytes(StandardCharsets.US_ASCII));
                while (text.hasRemaining()) {
                    channel.write(text);
                }
                channel.force(true);
            }
            created = link(temporary, path, what);
        } finally {
            Files.deleteIfExists(temporary);
        }
        if (posix) {
            syncDirectory(directory);
        }
        return created ? Origin.CREATED : Origin.CONCURRENTLY_CREATED;
    }

    // Tightened only when actually too wide, and never through a link: a linked key usually sits on
    // a read-only mount, such as a Kubernetes secret volume, where the chmod would fail.
    private static void tightenExisting(Path path) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class)) {
            return;
        }
        if (!OWNER_ONLY.containsAll(Files.getPosixFilePermissions(path))) {
            Files.setPosixFilePermissions(path, OWNER_ONLY);
        }
    }

    // The file is empty until it is owner-only, so the key is never readable by others.
    private static Path createOwnerOnly(Path directory, String name, boolean posix) throws IOException {
        Path temporary = directory.resolve("." + name + "-"
                + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36) + ".tmp");
        if (posix) {
            return Files.createFile(temporary, PosixFilePermissions.asFileAttribute(OWNER_ONLY));
        }
        if (!Files.getFileStore(directory).supportsFileAttributeView(AclFileAttributeView.class)) {
            return Files.createFile(temporary);
        }
        // Set at creation, not after: a handle another account opened in between would keep its
        // access after the ACL is tightened. An ACL given at creation also inherits nothing from
        // the directory (checked on Windows 11).
        UserPrincipal owner = directory.getFileSystem().getUserPrincipalLookupService()
                .lookupPrincipalByName(System.getProperty("user.name"));
        List<AclEntry> ownerOnly = List.of(AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(owner)
                .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                .build());
        return Files.createFile(temporary, new FileAttribute<List<AclEntry>>() {
            @Override
            public String name() {
                return "acl:acl";
            }

            @Override
            public List<AclEntry> value() {
                return ownerOnly;
            }
        });
    }

    /**
     * Links the written file into place, which fails if a file appeared at the path meanwhile, so
     * a key is never replaced. Without hard links there is no way to publish a complete file
     * without that risk (a move may replace a file created in the same instant), so creation fails.
     *
     * @return whether this call created the file, false when another one won
     */
    private static boolean link(Path temporary, Path path, String what) throws IOException {
        try {
            Files.createLink(path, temporary);
            return true;
        } catch (FileAlreadyExistsException race) {
            return false;
        } catch (UnsupportedOperationException noHardLinks) {
            throw new IOException("Cannot create " + what + " at " + path + " safely: its file system has no hard "
                    + "links. Create it elsewhere and point to it", noHardLinks);
        }
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}

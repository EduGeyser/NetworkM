package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.List;

/**
 * Owner-only access for files that hold private keys and admission secrets. POSIX file systems get modes
 * {@code rwx------} and {@code rw-------}. File systems with ACLs instead (Windows) get one entry that allows the
 * owner everything, which replaces whatever the parent folder would pass down; the directory's entry is inherited
 * by the files later created in it.
 */
final class OwnerOnlyFiles {
    static final boolean POSIX = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private OwnerOnlyFiles() {
    }

    static void createDirectories(Path directory) throws IOException {
        if (POSIX) {
            Files.createDirectories(directory,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        } else {
            Files.createDirectories(directory);
        }
    }

    static Path createTempFile(Path directory, String prefix, String suffix) throws IOException {
        FileAttribute<?>[] attributes = POSIX
                ? new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))}
                : new FileAttribute<?>[0];
        Path file = Files.createTempFile(directory, prefix, suffix, attributes);
        try {
            restrict(file);
        } catch (IOException failure) {
            Files.deleteIfExists(file);
            throw failure;
        }
        return file;
    }

    /** Applies owner-only access to an existing directory or file, never following a link. */
    static void restrict(Path path) throws IOException {
        boolean directory = Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
        if (POSIX) {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(directory ? "rwx------" : "rw-------"));
            return;
        }
        AclFileAttributeView acl = Files.getFileAttributeView(path, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (acl == null) {
            // Neither model is available; there is nothing finer to set
            return;
        }
        AclEntry.Builder owner = AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(acl.getOwner())
                .setPermissions(EnumSet.allOf(AclEntryPermission.class));
        if (directory) {
            owner.setFlags(AclEntryFlag.FILE_INHERIT, AclEntryFlag.DIRECTORY_INHERIT);
        }
        acl.setAcl(List.of(owner.build()));
    }
}

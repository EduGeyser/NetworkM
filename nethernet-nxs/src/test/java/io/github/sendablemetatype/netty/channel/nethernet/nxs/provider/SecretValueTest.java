package io.github.sendablemetatype.netty.channel.nethernet.nxs.provider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A configured secret: the value itself, or a file it names, by upstream CloudburstMC/Network's rule. */
class SecretValueTest {

    @Test
    void readsTheFileAReferenceNames(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("token"), "from-file\r\n");
        assertEquals("from-file", SecretValue.resolve("file:token", dir));
        assertEquals("from-file", SecretValue.resolve(" file:" + dir.resolve("token") + " ", null));
    }

    @Test
    void readsABarePathThatStartsAtTheRoot(@TempDir Path dir) throws Exception {
        Path token = Files.writeString(dir.resolve("token"), "from-file\n");
        // An absolute path on POSIX; on Windows the root of the configuration directory's drive
        String rooted = "/" + token.getRoot().relativize(token).toString().replace('\\', '/');

        assertEquals("from-file", SecretValue.resolve(rooted, dir));
    }

    @Test
    void readsBarePathsRelativeToTheConfigurationDirectory(@TempDir Path dir) throws Exception {
        Path config = Files.createDirectory(dir.resolve("config"));
        Files.writeString(config.resolve("token"), "inside");
        Files.writeString(dir.resolve("outside"), "outside");

        assertEquals("inside", SecretValue.resolve("./token", config));
        assertEquals("outside", SecretValue.resolve("../outside", config));
    }

    @Test
    void readsAWindowsDrivePathWhereThePlatformHasDrives(@TempDir Path dir) throws Exception {
        String drivePath = "C:\\nxs\\token";
        if (!Path.of(drivePath).isAbsolute()) {
            // Not absolute elsewhere, so it is the secret itself, as upstream reads it
            assertEquals(drivePath, SecretValue.resolve(drivePath, dir));
            return;
        }
        Path token = Files.writeString(dir.resolve("token"), "from-file");
        assertTrue(token.toString().matches("[A-Za-z]:\\\\.*"), token::toString);

        assertEquals("from-file", SecretValue.resolve(token.toString(), null));
    }

    @Test
    void takesAnyOtherValueAsTheSecret(@TempDir Path dir) throws Exception {
        // Even when a file of that name exists: only the forms above name a file
        Files.writeString(dir.resolve("token"), "from-file");
        for (String value : List.of("token", "~/abc.def_ghi-jkl", "abc/def==", "file")) {
            assertEquals(value, SecretValue.resolve(value, dir), value);
        }
    }

    @Test
    void namesTheUnreadablePathButNeverTheValue(@TempDir Path dir) throws Exception {
        IOException missing = assertThrows(IOException.class, () -> SecretValue.resolve("file:absent-x7", dir));
        assertTrue(missing.getMessage().contains(dir.resolve("absent-x7").toString()), missing.getMessage());
        assertFalse(missing.getMessage().contains("file:"), missing.getMessage());

        // A base64 secret that starts with '/' names a file, so it has to be put in one; with nothing there, the
        // value may be the secret itself and is never shown, not even through the cause
        IOException slash = assertThrows(IOException.class, () -> SecretValue.resolve("/9j4AAQSkZJRgABAQ+/token", dir));
        assertTrue(slash.getMessage().contains("no file is there"), slash.getMessage());
        assertFalse(slash.getMessage().contains("9j4AAQ"), slash.getMessage());
        assertNull(slash.getCause());

        Files.writeString(dir.resolve("huge"), "x".repeat(16385));
        IOException huge = assertThrows(IOException.class, () -> SecretValue.resolve("./huge", dir));
        assertTrue(huge.getMessage().contains("larger than 16384 bytes"), huge.getMessage());

        IOException folder = assertThrows(IOException.class, () -> SecretValue.resolve("file:", dir));
        assertTrue(folder.getMessage().contains("not a regular file"), folder.getMessage());
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.nxs.provider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A configured secret: a value, or a file reference, and never a path taken for the value. */
class SecretValueTest {

    @Test
    void readsTheFileAReferenceNames(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("token"), "from-file\r\n");
        assertEquals("from-file", SecretValue.resolve("file:token", dir));
        assertEquals("from-file", SecretValue.resolve(" file:" + dir.resolve("token") + " ", null));
    }

    @Test
    void refusesAValueThatNamesAnExistingFileOrDirectoryWithoutThePrefix(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("token"), "from-file");
        Files.createDirectory(dir.resolve("secrets"));
        for (String value : List.of("token", "./token", "secrets", dir.resolve("token").toString(),
                dir.resolve("secrets").toString())) {
            IOException refused = assertThrows(IOException.class, () -> SecretValue.resolve(value, dir), value);
            assertTrue(refused.getMessage().contains("file:<path>"), refused.getMessage());
            assertFalse(refused.getMessage().contains(value), "the message names the value");
        }
        // An absolute path needs no configuration directory to be recognized
        assertThrows(IOException.class, () -> SecretValue.resolve(dir.resolve("token").toString(), null));
    }

    @Test
    void refusesAWindowsDrivePathEvenWhenNothingIsThere(@TempDir Path dir) {
        // A bearer token has no ':', so a drive is never a token's start
        for (String value : List.of("C:\\nxs\\missing-token", "d:/nxs/missing-token")) {
            IOException refused = assertThrows(IOException.class, () -> SecretValue.resolve(value, dir), value);
            assertFalse(refused.getMessage().contains(value));
        }
    }

    @Test
    void keepsSecretsThatStartLikePathsButNameNothing(@TempDir Path dir) throws Exception {
        // Base64 and b64token secrets may start with '/' or '~/', and '~' is never expanded to a home directory
        for (String value : List.of("/9j4AAQSkZJRgABAQ+/token", "~/abc.def_ghi-jkl", "abc/def==", "token")) {
            assertEquals(value, SecretValue.resolve(value, dir), value);
        }
        assertEquals("token", SecretValue.resolve("token", null), "a relative value needs a directory to name a file");
    }

    @Test
    void failsWithoutTheValueWhenAReferencedFileCannotBeRead(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("huge"), "x".repeat(16385));
        for (String value : List.of("file:absent-x7", "file:", "file:huge")) {
            IOException refused = assertThrows(IOException.class, () -> SecretValue.resolve(value, dir), value);
            assertFalse(refused.getMessage().contains("absent-x7") || refused.getMessage().contains("huge"));
        }
    }
}

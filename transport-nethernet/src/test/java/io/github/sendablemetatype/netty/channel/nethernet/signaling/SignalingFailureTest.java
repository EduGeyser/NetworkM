package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SignalingFailureTest {

    @Test
    void remoteTextIsCutAndReducedToPrintableAscii() {
        String quoted = SignalingFailure.quote("bad\u0000\nthingé" + "x".repeat(300));

        assertTrue(quoted.startsWith(": bad  thing x"), quoted);
        assertEquals(202, quoted.length());
        assertEquals("", SignalingFailure.quote("  "));
    }
}

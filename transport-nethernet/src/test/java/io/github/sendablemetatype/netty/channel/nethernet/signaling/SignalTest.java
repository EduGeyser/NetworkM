package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SignalTest {

    @Test
    void theTypesKeepTheirWireNamesBothWays() {
        assertEquals(List.of("CONNECTREQUEST", "CONNECTRESPONSE", "CANDIDATEADD", "CONNECTERROR"),
                List.of(SignalType.values()).stream().map(SignalType::wireName).toList());
        for (SignalType type : SignalType.values()) {
            Signal signal = new Signal(type, "00042", "a b c");
            assertEquals(type.wireName() + " 00042 a b c", signal.wire());
            assertEquals(Optional.of(signal), Signal.parse(signal.wire()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN 42 data", "connectrequest 42 v=0", "CONNECTREQUEST\u0007 42 v=0", "", " 42 v=0"})
    void aSignalOfAnyOtherTypeIsDroppedAtParseTime(String raw) {
        assertEquals(Optional.empty(), Signal.parse(raw));
    }

    @Test
    void anUnknownTypeReachesALogOnlyEscapedAndCutShort() {
        assertEquals("A\\u001b[31m\\\"x\\\\\\u00e9", Signal.escaped("A\u001b[31m\"x\\é"));
        assertEquals("X".repeat(32) + "...", Signal.escaped("X".repeat(33) + "\n"));
    }
}

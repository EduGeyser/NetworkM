package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Join;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.Signal;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.SignalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionIdValidationTest {
    @ParameterizedTest
    @ValueSource(strings = {"", "a\nb", "é"})
    void signalsAndJoinsBuiltInCodeRefuseIdsTheWireWouldDrop(String connectionId) {
        assertThrows(IllegalArgumentException.class,
                () -> new Signal(SignalType.CONNECT_ERROR, connectionId, "1"));
        assertThrows(IllegalArgumentException.class,
                () -> new Join(connectionId, "peer", "v=0", SourcedAddress.unknown(), new FakeJoinSource.Reply(false)));
    }

    @Test
    void theRefusalDoesNotEchoTheId() {
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> new Signal(SignalType.CONNECT_ERROR, "x".repeat(65) + "\n", "1"));
        assertFalse(refused.getMessage().contains("xxx"));
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosticChannelsTest {
    @Test
    void callbackOwnsOneFrameAndDoesNotReopenAfterConsumption() {
        DiagnosticChannels channels = new DiagnosticChannels();
        ByteBuffer borrowed = ByteBuffer.allocateDirect(DiagnosticExchange.FRAME_BYTES);
        byte[] original = new byte[DiagnosticExchange.FRAME_BYTES];
        Arrays.fill(original, (byte) 42);
        borrowed.put(original).flip();
        channels.received(true, borrowed);
        borrowed.clear().put(new byte[DiagnosticExchange.FRAME_BYTES]);
        assertArrayEquals(original, channels.poll());
        assertNull(channels.poll());
        assertEquals(1, channels.receivedFrames());
        assertEquals(DiagnosticExchange.FRAME_BYTES, channels.receivedBytes());
        channels.received(true, ByteBuffer.wrap(original));
        assertTrue(channels.protocolFailed());
        assertNull(channels.poll());
    }

    @Test
    void invalidSizeChannelAndTextCannotQueueApplicationData() {
        for (int size : new int[] {0, 55, 57, 256}) {
            DiagnosticChannels channels = new DiagnosticChannels();
            channels.received(true, ByteBuffer.allocate(size));
            assertTrue(channels.protocolFailed());
            assertNull(channels.poll());
            assertEquals(0, channels.receivedBytes());
        }
        DiagnosticChannels unreliable = new DiagnosticChannels();
        unreliable.received(false, ByteBuffer.allocate(DiagnosticExchange.FRAME_BYTES));
        assertTrue(unreliable.protocolFailed());
        assertNull(unreliable.poll());
        DiagnosticChannels text = new DiagnosticChannels();
        text.receivedText();
        text.received(true, ByteBuffer.allocate(DiagnosticExchange.FRAME_BYTES));
        assertTrue(text.protocolFailed());
        assertNull(text.poll());
    }

    @Test
    void aBurstCannotGrowTheSingleFrameMailbox() {
        DiagnosticChannels channels = new DiagnosticChannels();
        channels.received(true, ByteBuffer.allocate(DiagnosticExchange.FRAME_BYTES));
        for (int index = 0; index < 100; index++) {
            channels.received(true, ByteBuffer.allocate(DiagnosticExchange.FRAME_BYTES));
        }
        assertTrue(channels.protocolFailed());
        assertEquals(DiagnosticExchange.FRAME_BYTES, channels.poll().length);
        assertNull(channels.poll());
        assertEquals(1, channels.receivedFrames());
    }

    @Test
    void onlyTheTwoNetherNetChannelsWithTheirReliabilityOpen() {
        DiagnosticChannels channels = new DiagnosticChannels();
        channels.opened(DiagnosticChannels.RELIABLE);
        channels.connected();
        assertFalse(channels.ready(), "both channels must open");
        channels.opened(DiagnosticChannels.UNRELIABLE);
        assertTrue(channels.ready());
        assertThrows(IllegalArgumentException.class, () -> channels.opened(DiagnosticChannels.RELIABLE),
                "a channel opens once");
        assertTrue(channels.protocolFailed());
        assertFalse(channels.ready());

        OptionalInt none = OptionalInt.empty();
        for (var channel : List.of(
                ChannelParameters.reliable("Other"),
                new ChannelParameters("ReliableDataChannel", true, none, none, false, "x"),
                new ChannelParameters("ReliableDataChannel", false, none, none, false, ""),
                new ChannelParameters("ReliableDataChannel", true, OptionalInt.of(3), none, false, ""),
                new ChannelParameters("ReliableDataChannel", true, none, none, true, ""),
                new ChannelParameters("UnreliableDataChannel", true, OptionalInt.of(0), none, false, ""),
                new ChannelParameters("UnreliableDataChannel", false, none, OptionalInt.of(100), false, ""))) {
            DiagnosticChannels fresh = new DiagnosticChannels();
            assertThrows(IllegalArgumentException.class, () -> fresh.opened(channel), channel.toString());
            assertTrue(fresh.protocolFailed());
        }
    }
}

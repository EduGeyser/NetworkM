package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

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
        channels.opened(DiagnosticChannels.Channel.reliable());
        channels.connected();
        assertFalse(channels.ready(), "both channels must open");
        channels.opened(DiagnosticChannels.Channel.unreliable());
        assertTrue(channels.ready());
        assertThrows(IllegalArgumentException.class, () -> channels.opened(DiagnosticChannels.Channel.reliable()),
                "a channel opens once");
        assertTrue(channels.protocolFailed());
        assertFalse(channels.ready());

        for (var channel : java.util.List.of(
                new DiagnosticChannels.Channel("Other", "", true, null, null),
                new DiagnosticChannels.Channel("ReliableDataChannel", "x", true, null, null),
                new DiagnosticChannels.Channel("ReliableDataChannel", "", false, null, null),
                new DiagnosticChannels.Channel("ReliableDataChannel", "", true, 3, null),
                new DiagnosticChannels.Channel("UnreliableDataChannel", "", true, 0, null),
                new DiagnosticChannels.Channel("UnreliableDataChannel", "", false, null, 100))) {
            DiagnosticChannels fresh = new DiagnosticChannels();
            assertThrows(IllegalArgumentException.class, () -> fresh.opened(channel), channel.toString());
            assertTrue(fresh.protocolFailed());
        }
    }
}

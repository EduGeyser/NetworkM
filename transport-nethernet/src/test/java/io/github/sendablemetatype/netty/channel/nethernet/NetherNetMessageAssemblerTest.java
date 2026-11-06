package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.TransportDisconnectReason;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NetherNetMessageAssemblerTest {
    private static final int CAP = 1024;

    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    private final NetherNetMessageAssembler assembler = new NetherNetMessageAssembler();
    private final CountingMetrics metrics = new CountingMetrics();

    @Test
    void aSingleSegmentIsCopiedOutOfTheEngineBuffer() {
        ByteBuffer segment = segment(0, 1, 2, 3);
        ByteBuf message = decode(segment);
        segment.clear().put(new byte[segment.capacity()]);
        try {
            assertArrayEquals(new byte[]{1, 2, 3}, ByteBufUtil.getBytes(message));
        } finally {
            message.release();
        }
        assertReleased();
    }

    @Test
    void aSplitMessageIsJoinedInCountdownOrder() {
        assertNull(decode(segment(2, 1, 2)));
        assertNull(decode(segment(1, 3)));
        ByteBuf message = decode(segment(0, 4, 5));
        try {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, ByteBufUtil.getBytes(message));
        } finally {
            message.release();
        }
        assertEquals(2, metrics.fragments);
        assertEquals(0, metrics.failures);
        assertReleased();
    }

    @Test
    void twoHundredFiftySixSegmentsMakeOneMessage() {
        ByteBuf message = null;
        for (int countdown = 255; countdown >= 0; countdown--) {
            message = decode(segment(countdown, (byte) countdown));
            if (countdown > 0) {
                assertNull(message);
            }
        }
        try {
            assertEquals(256, message.readableBytes());
            assertEquals((byte) 255, message.getByte(0));
            assertEquals(0, message.getByte(255));
        } finally {
            message.release();
        }
        assertReleased();
    }

    @Test
    void aBrokenCountdownEndsTheStreamAndReleasesWhatWasAssembled() {
        assertNull(decode(segment(2, 1)));
        assertBroken(TransportDisconnectReason.BAD_PACKET, segment(0, 2));
        assertReleased();
        assertNull(decode(segment(0, 3)), "nothing is delivered after a break");
        assertEquals(1, metrics.failures);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void aSegmentOfOneByteOrLessEndsTheStream(int length) {
        assertBroken(TransportDisconnectReason.BAD_PACKET, ByteBuffer.allocateDirect(length));
        assertNull(decode(segment(0, 7)));
    }

    @Test
    void aBareHeaderInsideASplitMessageEndsTheStream() {
        assertNull(decode(segment(1, 7)));
        assertBroken(TransportDisconnectReason.BAD_PACKET, segment(0));
        assertReleased();
    }

    @Test
    void aMessageOverTheCapEndsTheStreamWithSplitQueueTooLong() {
        assertNull(decode(segment(1, new byte[CAP - 1])));
        assertBroken(TransportDisconnectReason.SPLIT_QUEUE_TOO_LONG, segment(0, 1, 2));
        assertReleased();
    }

    @Test
    void aSingleSegmentOverTheCapEndsTheStream() {
        assertBroken(TransportDisconnectReason.SPLIT_QUEUE_TOO_LONG, segment(0, new byte[CAP + 1]));
    }

    @Test
    void theTailOfAMessageThatBrokeTheCapIsNeverDelivered() {
        assertNull(decode(segment(2, new byte[CAP])));
        assertBroken(TransportDisconnectReason.SPLIT_QUEUE_TOO_LONG, segment(1, 1));
        // The rest of the dropped message, including its final segment, must not
        // reach the application as a message of its own.
        assertNull(decode(segment(0, 2)));
        assertNull(decode(segment(0, 3)));
        assertReleased();
    }

    @Test
    void aFailingFragmentMetricKeepsTheSplitMessageWhole() {
        NetherChannelMetrics failing = new NetherChannelMetrics() {
            @Override
            public void fragmentsIn(int count) {
                throw new IllegalStateException("metrics down");
            }
        };
        assertNull(assembler.decode(segment(1, 1), allocator, CAP, failing));
        ByteBuf message = assembler.decode(segment(0, 2), allocator, CAP, failing);
        try {
            assertArrayEquals(new byte[]{1, 2}, ByteBufUtil.getBytes(message));
        } finally {
            message.release();
        }
        assertReleased();
    }

    @Test
    void aFailingFailureMetricStillEndsTheStream() {
        NetherChannelMetrics failing = new NetherChannelMetrics() {
            @Override
            public void decodeFail(int count) {
                throw new IllegalStateException("metrics down");
            }
        };
        NetherNetFramingException broken = assertThrows(NetherNetFramingException.class,
                () -> assembler.decode(segment(0), allocator, CAP, failing));
        assertEquals(TransportDisconnectReason.BAD_PACKET, broken.reason());
    }

    @Test
    void closingReleasesAPartialMessageAndIgnoresLaterSegments() {
        assertNull(decode(segment(1, 42)));
        assembler.close();
        assertReleased();
        assertNull(decode(segment(0, 43)));
    }

    private ByteBuf decode(ByteBuffer segment) {
        return assembler.decode(segment, allocator, CAP, metrics);
    }

    private void assertBroken(TransportDisconnectReason reason, ByteBuffer segment) {
        NetherNetFramingException broken = assertThrows(NetherNetFramingException.class, () -> decode(segment));
        assertEquals(reason, broken.reason());
    }

    private void assertReleased() {
        assertEquals(0, allocator.metric().usedHeapMemory(), "a copied segment leaked");
    }

    private static ByteBuffer segment(int countdown, int... payload) {
        ByteBuffer segment = ByteBuffer.allocateDirect(1 + payload.length).put((byte) countdown);
        for (int b : payload) {
            segment.put((byte) b);
        }
        return segment.flip();
    }

    private static ByteBuffer segment(int countdown, byte[] payload) {
        return ByteBuffer.allocateDirect(1 + payload.length).put((byte) countdown).put(payload).flip();
    }

    private static final class CountingMetrics implements NetherChannelMetrics {
        int fragments;
        int failures;

        @Override
        public void fragmentsIn(int count) {
            fragments += count;
        }

        @Override
        public void decodeFail(int count) {
            failures += count;
        }
    }
}

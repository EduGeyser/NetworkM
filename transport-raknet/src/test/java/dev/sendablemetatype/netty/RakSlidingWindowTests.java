package dev.sendablemetatype.netty;

import io.netty.buffer.Unpooled;
import dev.sendablemetatype.netty.channel.raknet.RakReliability;
import dev.sendablemetatype.netty.channel.raknet.RakSlidingWindow;
import dev.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;
import dev.sendablemetatype.netty.channel.raknet.packet.RakDatagramPacket;
import dev.sendablemetatype.netty.util.RakSequence;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RakSlidingWindowTests {
    @Test
    void acknowledgementsAfterTheWireWrapOpenANewCongestionPeriod() {
        RakSlidingWindow window = new RakSlidingWindow(100);
        acknowledge(window, 1, 2);
        acknowledge(window, 2, 3);
        acknowledge(window, 3, 4);
        assertEquals(400, window.getTransmissionBandwidth());
        window.onResend(RakSequence.MASK);
        assertEquals(100, window.getTransmissionBandwidth());
        acknowledge(window, 0, (long) RakSequence.MASK + 2);
        acknowledge(window, 1, (long) RakSequence.MASK + 3);
        assertEquals(233, window.getTransmissionBandwidth());
        window.onResend((long) RakSequence.MASK + 3);
        assertEquals(100, window.getTransmissionBandwidth(), "The new period must allow congestion backoff again");
    }

    private static void acknowledge(RakSlidingWindow window, int sequenceIndex, long nextSequenceIndex) {
        RakDatagramPacket datagram = RakDatagramPacket.newInstance();
        EncapsulatedPacket packet = EncapsulatedPacket.newInstance();
        packet.setReliability(RakReliability.RELIABLE);
        packet.setBuffer(Unpooled.buffer(1).writeByte(1));
        datagram.getPackets().add(packet);
        datagram.setSequenceIndex(sequenceIndex);
        try {
            window.onReliableSend(datagram);
            window.onAck(1, datagram, nextSequenceIndex);
        } finally {
            datagram.release();
        }
    }
}

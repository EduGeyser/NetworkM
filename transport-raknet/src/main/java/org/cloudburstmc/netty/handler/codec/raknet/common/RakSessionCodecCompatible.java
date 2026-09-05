package org.cloudburstmc.netty.handler.codec.raknet.common;

import org.cloudburstmc.netty.channel.raknet.*;
import org.cloudburstmc.netty.channel.raknet.packet.EncapsulatedPacket;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class RakSessionCodecCompatible extends RakSessionCodec {
    public static final String NAME = "rak-session-codec";

    public RakSessionCodecCompatible(RakChannel channel) {
        super(channel);
    }

    RakSessionCodecCompatible(RakChannel channel, LongSupplier clock) {
        super(channel, clock);
    }

    @Override
    EncapsulatedPacket createEncapsulatedPacket() {
        return EncapsulatedPacket.newInstance();
    }

    @Override
    long pingTimestamp() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
}

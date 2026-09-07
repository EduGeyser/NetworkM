package dev.sendablemetatype.netty.handler.codec.raknet.common;

import dev.sendablemetatype.netty.channel.raknet.*;
import dev.sendablemetatype.netty.channel.raknet.packet.EncapsulatedPacket;
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

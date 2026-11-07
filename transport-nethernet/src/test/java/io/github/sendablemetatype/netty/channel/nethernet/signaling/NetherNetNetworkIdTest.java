package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherNetAddress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NetherNetNetworkIdTest {
    @Test
    void addressPreservesOpaqueIds() {
        String opaque = "peer /?#+%";
        NetherNetAddress address = new NetherNetAddress(opaque);
        assertEquals(opaque, address.getNetworkId());
        assertEquals(opaque, address.toString());
        // A numeric-looking ID stays as written, leading zeros included
        assertEquals("00042", new NetherNetAddress("00042").getNetworkId());
    }
}

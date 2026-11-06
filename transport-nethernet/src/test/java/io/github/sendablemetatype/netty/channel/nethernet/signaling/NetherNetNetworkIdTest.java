package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherNetAddress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NetherNetNetworkIdTest {
    @Test
    void addressPreservesOpaqueIdsAndRetainsExplicitNumericConversion() {
        String opaque = "peer /?#+%";
        NetherNetAddress address = new NetherNetAddress(opaque);
        assertEquals(opaque, address.getNetworkId());
        assertEquals(opaque, address.toString());
        assertThrows(NumberFormatException.class, address::getNetworkIdAsLong);

        NetherNetAddress numeric = new NetherNetAddress("00042");
        assertEquals("00042", numeric.getNetworkId());
        assertEquals(42, numeric.getNetworkIdAsLong());
        assertEquals("18446744073709551615", new NetherNetAddress(-1L).getNetworkId());
    }
}

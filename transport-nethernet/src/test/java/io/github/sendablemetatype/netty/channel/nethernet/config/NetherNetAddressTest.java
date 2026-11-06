package io.github.sendablemetatype.netty.channel.nethernet.config;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetherNetAddressTest {

    @Test
    void addressesWithOneNetworkIdAreEqualHoweverTheyWereMade() {
        NetherNetAddress numeric = new NetherNetAddress(-1L);
        NetherNetAddress text = new NetherNetAddress("18446744073709551615");
        assertEquals(numeric, text);
        assertEquals(numeric.hashCode(), text.hashCode());
        assertEquals("value", Map.of(numeric, "value").get(text));
        assertTrue(Set.of(new NetherNetAddress("A1B2C3")).contains(new NetherNetAddress("A1B2C3")));
    }

    @Test
    void networkIdsAreComparedExactly() {
        assertNotEquals(new NetherNetAddress("abc"), new NetherNetAddress("ABC"));
        assertNotEquals(new NetherNetAddress("1"), new NetherNetAddress("01"));
        assertNotEquals(new NetherNetAddress("1"), InetSocketAddress.createUnresolved("1", 0));
    }

    @Test
    void aNetworkIdIsRequired() {
        assertThrows(NullPointerException.class, () -> new NetherNetAddress(null));
    }
}

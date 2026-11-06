package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpConnectionLimitsTest {

    @Test
    void ipv6ClientsCountByTheirSlash64() throws Exception {
        HttpConnectionLimits limits = new HttpConnectionLimits(1024, 2);

        assertNotNull(limits.admit(InetAddress.getByName("2001:db8:1:2::1")));
        assertNotNull(limits.admit(InetAddress.getByName("2001:db8:1:2:ffff:ffff:ffff:ffff")));

        assertNull(limits.admit(InetAddress.getByName("2001:db8:1:2:abcd::9")), "one host commonly holds a whole /64");
        assertNotNull(limits.admit(InetAddress.getByName("2001:db8:1:3::1")), "the next /64 is someone else");
    }

    @Test
    void ipv4ClientsCountByTheirAddress() throws Exception {
        HttpConnectionLimits limits = new HttpConnectionLimits(1024, 1);

        InetAddress key = limits.admit(InetAddress.getByName("192.0.2.1"));
        assertNotNull(key);
        assertNull(limits.admit(InetAddress.getByName("192.0.2.1")));
        assertNotNull(limits.admit(InetAddress.getByName("192.0.2.2")));

        limits.release(key);
        assertNotNull(limits.admit(InetAddress.getByName("192.0.2.1")), "a closed connection frees its place");
    }

    @Test
    void globalCapCountsEveryConnection() {
        HttpConnectionLimits limits = new HttpConnectionLimits(2, 100);

        assertTrue(limits.open());
        assertTrue(limits.open());
        assertFalse(limits.open());
        limits.closed();
        assertTrue(limits.open());
    }

    @Test
    void keyOfAnIpv6AddressIsItsNetwork() throws Exception {
        assertEquals(InetAddress.getByName("2001:db8:1:2::"),
                HttpConnectionLimits.key(InetAddress.getByName("2001:db8:1:2:3:4:5:6")));
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.address;

import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlaceholderAddressTest {

    @Test
    void handsOutUniqueReservedAddressesOnPortZero() {
        Set<InetSocketAddress> seen = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            InetSocketAddress placeholder = PlaceholderAddress.next();
            assertTrue(seen.add(placeholder), placeholder.toString());
            assertEquals(0, placeholder.getPort());
            assertEquals(0xF0, placeholder.getAddress().getAddress()[0] & 0xF0, placeholder.toString());
            // Nothing that classifies addresses mistakes it for a LAN or a real peer
            assertEquals(EndpointAddress.Scope.UNUSABLE, EndpointAddress.scope(placeholder.getAddress()));
        }
    }

    @Test
    void skipsBroadcastAndWrapsWithinTheBlock() {
        AtomicInteger counter = new AtomicInteger(0x0FFFFFFE);

        assertEquals("255.255.255.254", PlaceholderAddress.next(counter).getAddress().getHostAddress());
        assertEquals("240.0.0.0", PlaceholderAddress.next(counter).getAddress().getHostAddress());

        AtomicInteger overflow = new AtomicInteger(Integer.MAX_VALUE - 1);
        assertEquals("255.255.255.254", PlaceholderAddress.next(overflow).getAddress().getHostAddress(),
                "the int counter overflowing stays inside the block");
        assertEquals("240.0.0.0", PlaceholderAddress.next(overflow).getAddress().getHostAddress());
    }
}

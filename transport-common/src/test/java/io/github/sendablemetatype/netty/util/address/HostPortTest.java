package io.github.sendablemetatype.netty.util.address;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HostPortTest {

    @Test
    void readsNamesAndPortsWithoutLookingThemUp() {
        assertEquals(Optional.of(InetSocketAddress.createUnresolved("stun.example.test", 3478)),
                HostPort.parse("stun.example.test", 3478));
        assertEquals(Optional.of(InetSocketAddress.createUnresolved("stun.example.test", 19302)),
                HostPort.parse("stun.example.test:19302", 3478));
        assertEquals(Optional.of(InetSocketAddress.createUnresolved("2001:db8::1", 3479)),
                HostPort.parse("[2001:db8::1]:3479", 3478));
        assertTrue(HostPort.parse("stun.example.test", 3478).orElseThrow().isUnresolved());
    }

    @Test
    void refusesAnythingElse() {
        for (String value : List.of("", "user@host.test", "host.test/path", "host.test:0", "host.test:65536",
                "host.test:", "host.test:port", "2001:db8::1", "[2001:db8::1", "[2001:db8::1]x", "[host.test]:1")) {
            assertTrue(HostPort.parse(value, 3478).isEmpty(), value);
        }
    }
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeIceEndpoint;
import io.github.sendablemetatype.netty.util.address.ProxyList;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.github.sendablemetatype.netty.util.nethernet.OfferFixtures;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.ANSWER;
import static io.github.sendablemetatype.netty.channel.nethernet.EndpointHarness.OFFER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** STUN discovery on the shared ICE port: what answers announce, and what binding refuses. */
@Timeout(15)
class NetherNetServerStunTest {
    private static final InetSocketAddress SERVER = new InetSocketAddress("192.0.2.7", 3478);
    /** What the engine gathered on a host with a private interface address only. */
    private static final String GATHERED = "v=0\r\nm=application 5000 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "c=IN IP4 10.0.0.5\r\na=candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host\r\n"
            + "a=fingerprint:sha-256 " + OfferFixtures.DIGEST + "\r\na=mid:data\r\na=end-of-candidates\r\n";

    private final EndpointHarness harness = new EndpointHarness();

    @AfterEach
    void tearDown() {
        harness.close();
    }

    @Test
    void answersAnnounceTheFreshMappingAndItsIcePortTwin() throws Exception {
        harness.engine.answerWith(ANSWER);
        harness.engine.gatherWith(GATHERED);
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_STUN_SERVERS, List.of(SERVER));
        NetherNetServerChannel server = harness.bind();
        FakeIceEndpoint endpoint = harness.engine.endpoints().getFirst();
        assertEquals(List.of(SERVER), endpoint.stunMonitors(), "the monitor opens with the port");
        FakeJoinSource source = harness.source(false);

        String before = source.offer("1", OFFER).answer();
        assertFalse(before.contains("typ srflx"), "nothing is announced before the server answered: " + before);
        assertEquals(List.of(), server.discoveredAddresses());

        // The router took another port on the way out; the mapping must be public to be announced
        InetSocketAddress mapped = new InetSocketAddress("93.184.216.40", 1454);
        endpoint.mapStun(SERVER, mapped);
        awaitReads(endpoint, 2);
        String answer = source.offer("2", OFFER).answer();

        int icePort = endpoint.port();
        assertEquals(List.of(mapped, new InetSocketAddress(mapped.getAddress(), icePort)),
                server.discoveredAddresses(), "the mapping, then its ICE port twin");
        assertEquals(List.of(), server.advertisedAddresses());
        assertTrue(answer.contains("a=candidate:1 1 udp 2122260223 10.0.0.5 5000 typ host\r\n"), answer);
        assertTrue(answer.contains(" 93.184.216.40 1454 typ srflx raddr 10.0.0.5 rport 5000\r\n"), answer);
        assertTrue(answer.contains(" 93.184.216.40 " + icePort + " typ srflx raddr 10.0.0.5 rport 5000\r\n"), answer);
        assertTrue(priority(answer, "1454") > priority(answer, Integer.toString(icePort)),
                "the mapping ranks above its twin, both below the host candidate");
        assertTrue(priority(answer, Integer.toString(icePort)) < 2122260223L);

        server.close().sync();
        assertEquals(List.of(), server.discoveredAddresses(), "a closed endpoint announces nothing");
    }

    /** Waits until the endpoint's monitors were read this often, which a refresh does once per second. */
    private void awaitReads(FakeIceEndpoint endpoint, int reads) throws InterruptedException {
        int start = endpoint.stunReads();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (endpoint.stunReads() < start + reads && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(endpoint.stunReads() >= start + reads, "the monitors were not read");
        harness.sync();
    }

    private static long priority(String answer, String port) {
        return answer.lines()
                .filter(line -> line.contains(" 93.184.216.40 " + port + " typ srflx"))
                .mapToLong(line -> Long.parseLong(line.split(" ")[3]))
                .findFirst().orElseThrow();
    }

    @Test
    void discoveryAndAdvertisedAddressesCannotBothBeSet() {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_STUN_SERVERS, List.of(SERVER))
                .option(NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES,
                        List.of(new InetSocketAddress("203.0.113.7", 0)));
        assertBindFails();
    }

    @Test
    void discoveryNeedsTheSharedPort() {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_STUN_SERVERS, List.of(SERVER))
                .option(NetherChannelOption.NETHER_SERVER_SHARED_ICE_PORT, false);
        assertBindFails();
    }

    @Test
    void discoveryIsRefusedBehindProxyRelays() {
        harness.bootstrap.option(NetherChannelOption.NETHER_SERVER_STUN_SERVERS, List.of(SERVER))
                .option(NetherChannelOption.NETHER_SERVER_TRUSTED_PROXIES,
                        new TrustedProxies(TrustedProxies.parseList(List.of("192.0.2.10")), ProxyList.NONE));
        assertBindFails();
    }

    private void assertBindFails() {
        ChannelFuture bind = harness.bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).awaitUninterruptibly();
        assertInstanceOf(IllegalStateException.class, bind.cause());
        assertTrue(harness.engine.endpoints().isEmpty(), "no port was opened");
    }
}

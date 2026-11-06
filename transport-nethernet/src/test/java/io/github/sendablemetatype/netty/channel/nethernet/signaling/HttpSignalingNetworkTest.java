package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.Response;
import io.github.sendablemetatype.netty.util.address.IpRangeSet;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.pkitesting.X509Bundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.github.sendablemetatype.netty.channel.nethernet.signaling.SocketHttp.get;
import static io.github.sendablemetatype.netty.channel.nethernet.signaling.SocketHttp.post;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP signaling on real sockets: TLS next to plain HTTP (E-1, E-2), proxies (E-3) and the
 * connection caps (E-5).
 */
@Timeout(30)
class HttpSignalingNetworkTest {
    private static final String OFFER = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n";
    private static final TrustedProxies PROXY_PROTOCOL_FROM_LOOPBACK =
            new TrustedProxies(IpRangeSet.parse(List.of("127.0.0.1")), IpRangeSet.empty());
    private static final TrustedProxies FORWARDED_FOR_FROM_LOOPBACK =
            new TrustedProxies(IpRangeSet.empty(), IpRangeSet.parse(List.of("127.0.0.1")));

    private static X509Bundle authority;
    private static SslContext serverContext;

    private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final RecordingSink sink = new RecordingSink();
    private final List<Socket> sockets = new ArrayList<>();
    private NetherNetHttpSignaling signaling;
    private InetSocketAddress address;

    @BeforeAll
    static void certificates() throws Exception {
        authority = TestCertificates.authority();
        serverContext = TestCertificates.serverContext(TestCertificates.server(authority, "play.example.test"));
    }

    @AfterEach
    void tearDown() throws IOException {
        for (Socket socket : sockets) {
            socket.close();
        }
        if (signaling != null) {
            signaling.close();
        }
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private void start(NetherNetHttpSignaling.Builder builder) throws Exception {
        signaling = builder.build();
        signaling.bind(new InetSocketAddress("127.0.0.1", 0));
        signaling.start(sink);
        address = signaling.boundAddress();
    }

    private Socket plain() throws IOException {
        Socket socket = SocketHttp.open(address);
        sockets.add(socket);
        return socket;
    }

    /** A TLS connection, asking for that server name, or none when null. */
    private SSLSocket tls(String serverName) throws Exception {
        return tls(plain(), serverName);
    }

    private static SSLSocket tls(Socket under, String serverName) throws Exception {
        // A context of its own: resuming a cached session would send that session's server name
        SSLSocket socket = (SSLSocket) TestCertificates.trusting(authority).getSocketFactory()
                .createSocket(under, "127.0.0.1", under.getPort(), true);
        if (serverName != null) {
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setServerNames(List.of(new SNIHostName(serverName)));
            socket.setSSLParameters(parameters);
        }
        socket.setSoTimeout(10_000);
        socket.startHandshake();
        return socket;
    }

    private static Response exchange(Socket socket, String request) throws IOException {
        SocketHttp.write(socket, request);
        return SocketHttp.read(socket);
    }

    @Test
    void servesTlsAndPlainHttpOnOnePort() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).tls(name -> serverContext));

        assertEquals(200, exchange(tls(null), get("/v1/join", "Connection: close\r\n")).status());
        assertEquals(200, exchange(plain(), get("/v1/join", "Connection: close\r\n")).status(),
                "plain HTTP stays reachable for clients whose TLS attempt was refused");
    }

    @Test
    void eachJoinSaysWhetherItsConnectionSpokeTls() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).tls(name -> serverContext));

        SocketHttp.write(tls(null), post("1", OFFER));
        assertTrue(sink.next().addressed().tls());
        SocketHttp.write(plain(), post("2", OFFER));
        assertFalse(sink.next().addressed().tls());
    }

    @Test
    void tlsSelectorIsAskedWithTheRequestedServerName() throws Exception {
        List<String> asked = new CopyOnWriteArrayList<>();
        start(NetherNetHttpSignaling.builder(worker).tls(name -> {
            asked.add(name);
            return serverContext;
        }));

        exchange(tls("Play.Example.Test"), get("/v1/join", "Connection: close\r\n"));
        exchange(tls(null), get("/v1/join", "Connection: close\r\n"));
        exchange(plain(), get("/v1/join", "Connection: close\r\n"));

        assertEquals(Arrays.asList("play.example.test", null), asked,
                "lowercased; none for an IP address; plain HTTP never asks");
    }

    @Test
    void withdrawnTlsClosesTheConnectionAndLeavesPlainHttp() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).tls(name -> "play.example.test".equals(name) ? null : serverContext));

        assertThrows(SSLException.class, () -> tls("play.example.test"), "withdrawn for this name");
        assertEquals(200, exchange(tls(null), get("/v1/join", "Connection: close\r\n")).status());
        assertEquals(200, exchange(plain(), get("/v1/join", "Connection: close\r\n")).status());
    }

    @Test
    void tlsWithoutASelectorIsClosedBeforeAnyResponse() throws Exception {
        start(NetherNetHttpSignaling.builder(worker));
        Socket socket = plain();

        // The first five bytes of a TLS 1.2 ClientHello record
        SocketHttp.write(socket, new byte[]{0x16, 0x03, 0x01, 0x00, 0x05});

        assertTrue(SocketHttp.closedByServer(socket), "an HTTP response would be garbage to a TLS client");
    }

    @Test
    void requiresTlsRefusesPlainHttpOnly() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).tls(name -> serverContext).requiresTls(true));

        Response refused = exchange(plain(), get("/v1/join"));
        assertEquals(426, refused.status());
        assertEquals("TLS/1.2, HTTP/1.1", refused.headers().get("upgrade"));
        assertEquals(200, exchange(tls(null), get("/v1/join", "Connection: close\r\n")).status());
    }

    @Test
    void requiresTlsServesNothingWhileTlsIsWithdrawn() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).tls(name -> null).requiresTls(true));

        assertThrows(SSLException.class, () -> tls(null));
        assertEquals(426, exchange(plain(), get("/v1/join")).status(), "no downgrade to plain HTTP");
    }

    @Test
    void proxyProtocolHeadersDeclareTheSource() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).trustedProxies(PROXY_PROTOCOL_FROM_LOOPBACK));

        Socket v1 = plain();
        SocketHttp.write(v1, SocketHttp.proxyV1("203.0.113.7", 51000), post("1", OFFER).getBytes());
        SourcedAddress first = sink.next().source();
        Socket v2 = plain();
        SocketHttp.write(v2, SocketHttp.proxyV2("198.51.100.4", 52000), post("2", OFFER).getBytes());
        SourcedAddress second = sink.next().source();

        assertEquals(new InetSocketAddress("203.0.113.7", 51000), first.address());
        assertEquals(AddressSource.SIGNALING_PROXY_PROTOCOL, first.source());
        assertEquals("127.0.0.1", first.via().getAddress().getHostAddress());
        assertEquals(new InetSocketAddress("198.51.100.4", 52000), second.address());
        assertEquals(AddressSource.SIGNALING_PROXY_PROTOCOL, second.source());
    }

    @Test
    void proxyHeaderPrecedesTls() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).trustedProxies(PROXY_PROTOCOL_FROM_LOOPBACK)
                .tls(name -> serverContext));

        Socket under = plain();
        SocketHttp.write(under, SocketHttp.proxyV2("203.0.113.7", 51000));
        SSLSocket socket = tls(under, null);
        SocketHttp.write(socket, post("1", OFFER));

        assertEquals(new InetSocketAddress("203.0.113.7", 51000), sink.next().source().address());
    }

    @Test
    void proxyHeaderFromAnUntrustedPeerIsNotRead() throws Exception {
        start(NetherNetHttpSignaling.builder(worker));
        Socket socket = plain();

        SocketHttp.write(socket, SocketHttp.proxyV1("203.0.113.7", 51000), post("1", OFFER).getBytes());

        Response response = SocketHttp.read(socket);
        assertTrue(response == null || response.status() == 400, "the header is no HTTP, so nothing is served");
        assertTrue(sink.joins.isEmpty());
    }

    @Test
    void forwardedForFromATrustedForwarderNamesTheClient() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).trustedProxies(FORWARDED_FOR_FROM_LOOPBACK));

        // A proxy that adds its own header line must not leave the client's line to decide
        SocketHttp.write(plain(), post("1", OFFER, "X-Forwarded-For: 198.51.100.1\r\n",
                "X-Forwarded-For: 203.0.113.9:40000\r\n"));
        Join join = sink.next();

        assertEquals(new InetSocketAddress("203.0.113.9", 40000), join.source().address());
        assertEquals(AddressSource.SIGNALING_FORWARDED_FOR, join.source().source());
        assertTrue(join.addressed().viaTrustedProxy(), "a forwarder's Host names the forwarder");
    }

    @Test
    void forwardedForFromAnyoneElseIsIgnored() throws Exception {
        start(NetherNetHttpSignaling.builder(worker));

        SocketHttp.write(plain(), post("1", OFFER, "X-Forwarded-For: 203.0.113.9\r\n"));
        Join join = sink.next();

        assertEquals("127.0.0.1", join.source().address().getAddress().getHostAddress());
        assertEquals(AddressSource.SIGNALING, join.source().source());
        assertNull(join.source().via());
    }

    @Test
    void proxyProtocolWinsOverForwardedFor() throws Exception {
        IpRangeSet loopback = IpRangeSet.parse(List.of("127.0.0.1"));
        start(NetherNetHttpSignaling.builder(worker).trustedProxies(new TrustedProxies(loopback, loopback)));

        SocketHttp.write(plain(), SocketHttp.proxyV1("203.0.113.7", 51000),
                post("1", OFFER, "X-Forwarded-For: 198.51.100.1\r\n").getBytes());

        assertEquals(new InetSocketAddress("203.0.113.7", 51000), sink.next().source().address());
    }

    @Test
    void capsConnectionsPerAddress() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).maxConnectionsPerAddress(2));

        kept();
        kept();
        Socket refused = plain();

        assertTrue(SocketHttp.closedByServer(refused), "the third is closed unanswered");
    }

    @Test
    void capsOpenConnections() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).maxConnections(2));

        kept();
        kept();
        Socket refused = plain();

        assertTrue(SocketHttp.closedByServer(refused));
    }

    @Test
    void countsProxiedClientsByTheSourceTheProxyDeclares() throws Exception {
        start(NetherNetHttpSignaling.builder(worker).trustedProxies(PROXY_PROTOCOL_FROM_LOOPBACK)
                .maxConnectionsPerAddress(1));

        Socket first = plain();
        SocketHttp.write(first, SocketHttp.proxyV1("203.0.113.7", 51000), get("/v1/join").getBytes());
        assertEquals(200, SocketHttp.read(first).status());

        Socket sameClient = plain();
        SocketHttp.write(sameClient, SocketHttp.proxyV1("203.0.113.7", 51001), get("/v1/join").getBytes());
        assertTrue(SocketHttp.closedByServer(sameClient));

        Socket otherClient = plain();
        SocketHttp.write(otherClient, SocketHttp.proxyV1("203.0.113.8", 51000), get("/v1/join").getBytes());
        assertEquals(200, SocketHttp.read(otherClient).status(), "the proxy's own address is not what counts");
    }

    @Test
    void answersAnOfferOverTheNetwork() throws Exception {
        String answer = "v=0\r\no=- 3 4 IN IP4 127.0.0.1\r\n";
        sink.onJoin = join -> join.reply().answer(answer);
        start(NetherNetHttpSignaling.builder(worker));
        Socket socket = plain();

        Response response = exchange(socket, post("42", OFFER));

        assertEquals(200, response.status());
        assertEquals(answer, response.body());
        assertTrue(response.closes());
        assertTrue(SocketHttp.closedByServer(socket));
    }

    /** A connection that made its status request and may still make one more. */
    private void kept() throws IOException {
        Socket socket = plain();
        Response response = exchange(socket, get("/v1/join"));
        assertEquals(200, response.status());
    }
}

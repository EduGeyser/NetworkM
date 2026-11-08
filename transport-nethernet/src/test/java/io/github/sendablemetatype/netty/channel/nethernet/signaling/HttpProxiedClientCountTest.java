package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.util.address.ProxyList;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clients behind a trusted {@code X-Forwarded-For} proxy count against the per-address cap by the
 * source the proxy declares, on embedded connections from the proxy.
 */
class HttpProxiedClientCountTest {
    private static final TrustedProxies FORWARDER = new TrustedProxies(ProxyList.NONE,
            TrustedProxies.parseList(List.of(EmbeddedHttp.CLIENT.getAddress().getHostAddress())));

    private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final RecordingSink sink = new RecordingSink();
    private final List<EmbeddedHttp> connections = new ArrayList<>();
    private NetherNetHttpSignaling signaling;

    @AfterEach
    void tearDown() {
        connections.forEach(EmbeddedHttp::close);
        if (signaling != null) {
            signaling.close();
        }
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private EmbeddedHttp connect() {
        if (signaling == null) {
            signaling = NetherNetHttpSignaling.builder(worker).trustedProxies(FORWARDER).maxConnectionsPerAddress(1)
                    .build();
            signaling.start(sink);
        }
        EmbeddedHttp connection = new EmbeddedHttp(signaling);
        connections.add(connection);
        return connection;
    }

    /** The head of a POST whose body the proxy is still streaming. */
    private static String unfinishedPost(String client) {
        return "POST /v1/join/1 HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 1000\r\nX-Forwarded-For: " + client
                + "\r\n\r\nv=0\r\n";
    }

    @Test
    void aDeclaredClientCountsOnceItsHeadersArrive() {
        EmbeddedHttp first = connect();
        first.send(unfinishedPost("203.0.113.9"));
        EmbeddedHttp second = connect();
        second.send(unfinishedPost("203.0.113.9"));

        assertTrue(first.isOpen());
        assertFalse(second.isOpen(), "the same client holds its share before any body is complete");

        EmbeddedHttp other = connect();
        other.send(unfinishedPost("203.0.113.10"));
        assertTrue(other.isOpen(), "the proxy's own address is not what counts");
    }

    @Test
    void aRequestForAnotherClientMovesTheCount() {
        EmbeddedHttp reused = connect();
        reused.send(EmbeddedHttp.get("/v1/join", "X-Forwarded-For: 203.0.113.9\r\n"));
        assertEquals(200, reused.response().status());
        EmbeddedHttp busy = connect();
        busy.send(unfinishedPost("203.0.113.10"));

        reused.send(unfinishedPost("203.0.113.10"));

        assertFalse(reused.isOpen(), "the second client's own share is taken");
        EmbeddedHttp first = connect();
        first.send(unfinishedPost("203.0.113.9"));
        assertTrue(first.isOpen(), "the first client's count went with the request");
    }
}

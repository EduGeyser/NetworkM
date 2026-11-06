package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherServerMetrics;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.get;
import static io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.post;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What HTTP signaling reports to the server metrics and logs per request (resolves H16). */
class HttpSignalingMetricsTest {
    private static final String OFFER = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n";

    private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final RecordingSink sink = new RecordingSink();
    private final List<EmbeddedHttp> connections = new ArrayList<>();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private final NetherServerMetrics metrics = new NetherServerMetrics() {
        @Override
        public void joinRefused(int httpStatus) {
            events.add("refused " + httpStatus);
        }

        @Override
        public void addressRefused(InetSocketAddress address) {
            events.add("address " + address);
        }

        @Override
        public void plaintextRefused(InetSocketAddress address) {
            events.add("plaintext " + address);
        }
    };
    private NetherNetHttpSignaling signaling;

    @AfterEach
    void tearDown() {
        connections.forEach(EmbeddedHttp::close);
        if (signaling != null) {
            signaling.close();
        }
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private EmbeddedHttp connect(NetherNetHttpSignaling.Builder builder) {
        if (signaling == null) {
            signaling = builder.build();
            signaling.start(sink);
        }
        EmbeddedHttp connection = new EmbeddedHttp(signaling);
        connections.add(connection);
        return connection;
    }

    @Test
    void refusalsOfTheSourceItselfAreReportedAndTheEndpointsAreNot() throws Exception {
        NetherNetHttpSignaling.Builder builder = NetherNetHttpSignaling.builder(worker).metrics(metrics);
        connect(builder).send(post("42", ""));
        EmbeddedHttp pending = connect(builder);
        pending.send(post("43", OFFER));
        Join join = sink.next();
        pending.advance(NetherNetHttpSignaling.DEFAULT_NEGOTIATION_TIMEOUT.toMillis());
        EmbeddedHttp refused = connect(builder);
        refused.send(post("44", OFFER));
        sink.next().reply().refuse(JoinRefusal.HOST_REFUSED).toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertEquals(List.of("refused 400", "refused 504"), events,
                "the endpoint reports the refusals it decides itself");
        assertTrue(join.reply().abandoned().toCompletableFuture().isDone());
        assertEquals(JoinRefusal.HOST_REFUSED.httpStatus(), refused.response().status());
    }

    /** The aggregator answers these itself, before the offer is read whole. */
    @Test
    void offersTheAggregatorTurnsAwayAreReportedAndStatusRequestsAreNot() {
        NetherNetHttpSignaling.Builder builder = NetherNetHttpSignaling.builder(worker).metrics(metrics)
                .maxRequestBodyBytes(64);
        EmbeddedHttp declared = connect(builder);
        declared.send("POST /v1/join/42 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 65\r\n\r\n");
        EmbeddedHttp streamed = connect(builder);
        streamed.send("POST /v1/join/43 HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
                + "41\r\n" + "x".repeat(65) + "\r\n0\r\n\r\n");
        EmbeddedHttp continued = connect(builder);
        continued.send("POST /v1/join/44 HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n"
                + "Content-Length: 65\r\n\r\n");
        EmbeddedHttp expectation = connect(builder);
        expectation.send(post("45", OFFER, "Expect: x\r\n"));
        EmbeddedHttp status = connect(builder);
        status.send(get("/v1/join", "Expect: x\r\n"));

        for (EmbeddedHttp connection : List.of(declared, streamed, continued)) {
            assertEquals(413, connection.response().status());
        }
        assertEquals(417, expectation.response().status());
        assertEquals(417, status.response().status());
        assertEquals(List.of("refused 413", "refused 413", "refused 413", "refused 417"), events);
        assertTrue(sink.joins.isEmpty());
    }

    @Test
    void plainHttpRefusedUnderRequiresTlsIsReportedWithTheClient() {
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker).requiresTls(true).metrics(metrics));
        http.send(get("/v1/join"));

        assertEquals(426, http.response().status());
        assertEquals(List.of("plaintext " + EmbeddedHttp.CLIENT), events);
    }

    @Test
    void aConnectionOverItsAddressShareIsReported() {
        NetherNetHttpSignaling.Builder builder = NetherNetHttpSignaling.builder(worker).maxConnectionsPerAddress(1)
                .metrics(metrics);
        assertTrue(connect(builder).isOpen());
        EmbeddedHttp second = connect(builder);

        assertFalse(second.isOpen());
        assertEquals(List.of("address " + EmbeddedHttp.CLIENT), events);
    }

    @Test
    void aMetricsHookThatThrowsNeverBreaksSignaling() {
        NetherServerMetrics broken = new NetherServerMetrics() {
            @Override
            public void joinRefused(int httpStatus) {
                throw new IllegalStateException("metrics backend down");
            }
        };
        EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker).metrics(broken));
        http.send(post("42", ""));

        assertEquals(JoinRefusal.BAD_OFFER.httpStatus(), http.response().status());
        assertFalse(http.isOpen());
    }

    @Test
    void everyRequestLogsOneLineWithoutItsQueryOrBody() throws Exception {
        Logger backend = Logger.getLogger(HttpAccessLog.class.getName());
        Level previousLevel = backend.getLevel();
        List<String> lines = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                lines.add(record.getMessage());
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        backend.setLevel(Level.ALL);
        backend.addHandler(capture);
        try {
            EmbeddedHttp http = connect(NetherNetHttpSignaling.builder(worker));
            http.send(get("/v1/join?version=1.26.62&protocol=2225&platform=8&id=secret"));
            http.send(post("42?token=secret", OFFER));
            http.advance(1500);
            sink.next().reply().refuse(JoinRefusal.CAPACITY).toCompletableFuture().get(1, TimeUnit.SECONDS);
            http.responses();

            assertEquals(List.of(EmbeddedHttp.CLIENT + " GET /v1/join -> 200 (0 ms)",
                    EmbeddedHttp.CLIENT + " POST /v1/join/42 -> 503 (1500 ms)"), lines);
        } finally {
            backend.removeHandler(capture);
            backend.setLevel(previousLevel);
        }
    }

    @Test
    void loggedPathsAreBoundedAndPrintable() {
        assertEquals("/v1/join/a?b", HttpAccessLog.path("/v1/join/a\u0007b?c=d"));
        assertEquals("/" + "x".repeat(127) + "...", HttpAccessLog.path("/" + "x".repeat(300)));
    }
}

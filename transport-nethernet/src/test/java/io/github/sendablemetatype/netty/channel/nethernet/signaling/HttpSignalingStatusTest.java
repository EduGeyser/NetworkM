package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;
import io.github.sendablemetatype.netty.channel.nethernet.address.AddressSource;
import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.Response;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.ClientInfo;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusAnswer;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusProvider;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.http.StatusRequest;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How {@code GET /v1/join} is answered (ruling E-6). */
class HttpSignalingStatusTest {
    private static final NetherNetServerStatus FIRST = NetherNetServerStatus.builder().name("First").players(1).build();
    private static final NetherNetServerStatus SECOND = NetherNetServerStatus.builder().name("Second").build();

    private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final RecordingSink sink = new RecordingSink();
    private final List<EmbeddedHttp> connections = new ArrayList<>();
    private final List<StatusRequest> asked = new CopyOnWriteArrayList<>();
    private NetherNetHttpSignaling signaling;

    @AfterEach
    void tearDown() {
        connections.forEach(EmbeddedHttp::close);
        if (signaling != null) {
            signaling.close();
        }
        worker.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private void start(StatusProvider provider, Executor executor) {
        NetherNetHttpSignaling.Builder builder = NetherNetHttpSignaling.builder(worker).statusExecutor(executor);
        if (provider != null) {
            builder.statusProvider(request -> {
                asked.add(request);
                return provider.status(request);
            });
        }
        signaling = builder.statusTimeout(Duration.ofMillis(500)).build();
        signaling.start(sink);
    }

    private Response status(String uri, String... headers) {
        EmbeddedHttp http = new EmbeddedHttp(signaling);
        connections.add(http);
        http.send(get(uri, headers));
        return http.response();
    }

    @Test
    void withoutAProviderTheStatusIsAnEmpty200() {
        start(null, Runnable::run);

        Response response = status("/v1/join");

        assertEquals(200, response.status());
        assertTrue(response.body().isEmpty());
        assertNull(response.headers().get("content-type"));
    }

    @Test
    void servesTheProvidersDocument() {
        start(request -> StatusAnswer.serve(FIRST), Runnable::run);

        Response response = status("/v1/join");

        assertEquals(200, response.status());
        assertEquals("application/json", response.headers().get("content-type"));
        assertEquals(FIRST.toJson(), response.body());
    }

    @Test
    void providerSeesHostSourceAndWhatTheClientSays() {
        start(request -> StatusAnswer.serve(FIRST), Runnable::run);

        status("/v1/join?version=1.26.62&protocol=2225&platform=8&id=7250431262158236744", "Host: mc7x.org:19132\r\n");

        StatusRequest request = asked.getFirst();
        assertEquals("mc7x.org:19132", request.host());
        assertEquals(new SourcedAddress(EmbeddedHttp.CLIENT, AddressSource.SIGNALING, null), request.source());
        assertEquals(new ClientInfo("1.26.62", 2225, 8), request.client());
        assertEquals("7250431262158236744", request.networkId());
    }

    @Test
    void clientInfoIsAllOrNothing() {
        start(request -> StatusAnswer.serve(FIRST), Runnable::run);

        status("/v1/join?version=1.100.100.100.10&protocol=2223&platform=8");
        status("/v1/join");
        status("/v1/join?version=1.26.60.29&platform=8");
        status("/v1/join?version=1.26.60.29&protocol=latest&platform=8");
        status("/v1/join?version=1.100.100.100.100&protocol=2223&platform=8");
        status("/v1/join?version=1.26.60%0A29&protocol=2223&platform=8");
        status("/v1/join?version=%zz&protocol=2223&platform=8");

        assertEquals(Arrays.asList(new ClientInfo("1.100.100.100.10", 2223, 8), null, null, null, null, null, null),
                asked.stream().map(StatusRequest::client).toList(),
                "all of it, or nothing when any of it is missing, malformed or past the version cap");
    }

    @Test
    void declineAnswers404AndEndsTheConnection() {
        start(request -> StatusAnswer.decline(), Runnable::run);

        Response response = status("/v1/join");

        assertEquals(404, response.status(), "the status that sends a client back to RakNet");
        assertTrue(response.closes());
    }

    @Test
    void declinesWithoutAskingWhenTheEndpointWouldRefuseTheJoin() {
        start(request -> StatusAnswer.serve(FIRST), Runnable::run);
        sink.admits = false;

        Response response = status("/v1/join");

        assertEquals(404, response.status());
        assertTrue(asked.isEmpty());
        assertEquals(List.of(new SourcedAddress(EmbeddedHttp.CLIENT, AddressSource.SIGNALING, null)), sink.checked);
    }

    @Test
    void failingProviderServesTheLastDocumentForThatHost() {
        AtomicInteger calls = new AtomicInteger();
        start(request -> switch (calls.getAndIncrement()) {
            case 0 -> StatusAnswer.serve(FIRST);
            case 1 -> throw new IllegalStateException("status source down");
            default -> null;
        }, Runnable::run);

        assertEquals(FIRST.toJson(), status("/v1/join", "Host: A.example:19132\r\n").body());
        Response failed = status("/v1/join", "Host: a.example:19132\r\n");
        Response nullAnswer = status("/v1/join", "Host: b.example:19132\r\n");

        assertEquals(200, failed.status(), "never an error, which would cost the client NetherNet");
        assertEquals(FIRST.toJson(), failed.body(), "Host matches without regard to case");
        assertEquals(200, nullAnswer.status());
        assertTrue(nullAnswer.body().isEmpty(), "no document was served for this Host yet");
    }

    @Test
    void slowProviderGetsTheLastDocumentServedAtTheDeadline() {
        List<Runnable> calls = new ArrayList<>();
        AtomicInteger answers = new AtomicInteger();
        start(request -> StatusAnswer.serve(answers.getAndIncrement() == 0 ? FIRST : SECOND), calls::add);

        EmbeddedHttp first = new EmbeddedHttp(signaling);
        connections.add(first);
        first.send(get("/v1/join"));
        calls.removeFirst().run();
        assertEquals(FIRST.toJson(), first.response().body());

        EmbeddedHttp slow = new EmbeddedHttp(signaling);
        connections.add(slow);
        slow.send(get("/v1/join"));
        slow.advance(499);
        assertTrue(slow.responses().isEmpty());
        slow.advance(1);

        Response response = slow.response();
        assertEquals(200, response.status());
        assertEquals(FIRST.toJson(), response.body());
        assertFalse(response.closes(), "a fallback still serves NetherNet, so the join may follow");

        // A late answer still updates what the next fallback serves
        calls.removeFirst().run();
        EmbeddedHttp next = new EmbeddedHttp(signaling);
        connections.add(next);
        next.send(get("/v1/join"));
        next.advance(500);
        assertEquals(SECOND.toJson(), next.response().body());
    }

    @Test
    void callsBeyondTheConcurrencyCapFallBackAtOnce() {
        HttpStatusService service = new HttpStatusService(request -> StatusAnswer.serve(FIRST), Duration.ofSeconds(1),
                task -> {
                });
        EmbeddedChannel loop = new EmbeddedChannel();
        try {
            StatusRequest request = new StatusRequest(null, SourcedAddress.unknown(), null, null);
            for (int i = 0; i < HttpStatusService.MAX_CONCURRENT_CALLS; i++) {
                assertFalse(service.query(request, loop.eventLoop()).isDone());
            }

            CompletableFuture<HttpStatusService.Reply> overflow = service.query(request, loop.eventLoop());

            assertTrue(overflow.isDone());
            assertInstanceOf(HttpStatusService.Reply.Empty.class, overflow.join());
        } finally {
            loop.finishAndReleaseAll();
        }
    }
}

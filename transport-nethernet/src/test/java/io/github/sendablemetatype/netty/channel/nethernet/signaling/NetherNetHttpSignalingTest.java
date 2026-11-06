package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheckResult;
import io.github.sendablemetatype.netty.util.nethernet.OfferFixtures;
import io.github.sendablemetatype.netty.util.nethernet.OfferPolicy;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.ssl.SslContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Exercises the HTTP signaling front end against the real netty stack and
 * endpoint, with the WebRTC engine replaced by the fake engine: an admitted
 * offer produces a canned full ICE answer on a separate thread, mirroring how
 * the engine reports the description after gathering completes. Offers that
 * carry no assertion are admitted unless a test asks for the default policy.
 */
class NetherNetHttpSignalingTest {

    // The built-in server identity signs the fingerprint, as every answer must carry one.
    private static final String CANNED_ANSWER = "v=0\r\no=- 0 2 IN IP4 127.0.0.1\r\na=candidate:1 1 udp 2122260223 127.0.0.1 55555 typ host\r\n"
            + "a=fingerprint:sha-256 " + "AB:".repeat(31) + "AB\r\n";

    private static MultiThreadIoEventLoopGroup group;
    private static NetherNetHttpSignaling signaling;
    private static Channel serverChannel;
    private static FakeEngine engine;
    private static HttpClient client;
    private static String baseUrl;
    private static final LinkedBlockingQueue<NetherNetChildChannel> checkedChildren = new LinkedBlockingQueue<>();
    private static volatile OfferPolicy policy = result -> Optional.empty();

    @BeforeAll
    static void setUp() throws Exception {
        group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        engine = new FakeEngine();
        engine.answerWith("v=0\r\n");
        engine.gatherWith(CANNED_ANSWER);
        signaling = new NetherNetHttpSignaling((SslContext) null, group);
        signaling.bind(new InetSocketAddress("127.0.0.1", 0));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(group, group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, OfferFixtures.retailCheck())
                .childHandler(new ChannelInitializer<NetherNetChildChannel>() {
                    @Override protected void initChannel(NetherNetChildChannel channel) {
                        if (channel.getOfferCheck().keyProven()) checkedChildren.add(channel);
                    }
                });
        serverChannel = bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        ((NetherNetServerChannel) serverChannel).addSource(signaling, result -> policy.refusal(result));

        InetSocketAddress bound = signaling.boundAddress();
        baseUrl = "http://127.0.0.1:" + bound.getPort();
        client = HttpClient.newHttpClient();
    }

    @AfterAll
    static void tearDown() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (group != null) {
            group.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void checksBeforePeerCreationAndBindsTheChild() throws Exception {
        policy = OfferPolicy.REQUIRE_KEY_PROVEN;
        try {
            String valid = OfferFixtures.retailOffer();
            int previousAccepts = engine.peers().size();
            assertEquals(401, post("/v1/join/opaque-peer",
                    valid.replace(OfferFixtures.DIGEST, OfferFixtures.OTHER_DIGEST)).statusCode());
            assertEquals(previousAccepts, engine.peers().size());
            assertEquals(200, post("/v1/join/opaque-peer", valid).statusCode());
            NetherNetChildChannel child = checkedChildren.poll(3, TimeUnit.SECONDS);
            assertNotNull(child);
            assertEquals(OfferCheckResult.Level.ATTESTED, child.getOfferCheck().level());
            assertEquals(Optional.of("2535400000000000"), child.getOfferCheck().xuid());
            assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                    TransportIdentityBinding.verifyLogin(child, OfferFixtures.CLIENT_KEY.getPublic()));
            assertEquals(401, post("/v1/join/opaque-peer", "v=0\r\n").statusCode(), "No assertion");
        } finally {
            policy = result -> Optional.empty();
        }
    }

    @Test
    void joinProbeAnswers200() throws Exception {
        assertEquals(200, get("/v1/join").statusCode());
    }

    @Test
    void joinProbeCarriesTheServerStatusWhenSupplied() throws Exception {
        try {
            // No supplier: the historical empty body, which every client
            // accepts since the guide defines it as ignored.
            HttpResponse<String> bare = get("/v1/join");
            assertEquals(200, bare.statusCode());
            assertEquals("", bare.body());

            signaling.setStatusSupplier(() -> NetherNetServerStatus.builder()
                    .name("Test Server")
                    .protocol(1001)
                    .version("1.26.40")
                    .level("Test Level")
                    .players(3)
                    .maxPlayers(20)
                    .gameType(NetherNetServerStatus.GAME_TYPE_SURVIVAL)
                    .build());
            HttpResponse<String> described = get("/v1/join");
            assertEquals(200, described.statusCode());
            assertEquals("application/json", described.headers().firstValue("content-type").orElse(""));
            assertEquals("{\"name\":\"Test Server\",\"protocol\":1001,\"version\":\"1.26.40\","
                    + "\"level\":\"Test Level\",\"players\":3,\"maxPlayers\":20,\"gameType\":0}", described.body());

            // A failing supplier must not turn the capability check negative,
            // or the client stops attempting NetherNet entirely.
            signaling.setStatusSupplier(() -> {
                throw new IllegalStateException("no status available");
            });
            HttpResponse<String> degraded = get("/v1/join");
            assertEquals(200, degraded.statusCode());
            assertEquals("", degraded.body());
        } finally {
            signaling.setStatusSupplier(null);
        }
    }

    @Test
    void unknownPathAnswers404() throws Exception {
        assertEquals(404, get("/v1/other").statusCode());
        assertEquals(404, post("/v2/join/123", "x").statusCode());
    }

    @Test
    void offerProducesFullIceAnswer() throws Exception {
        HttpResponse<String> response = post("/v1/join/12345678901234567890", "v=0\r\nfake offer\r\n");
        assertEquals(200, response.statusCode());
        assertEquals("application/sdp", response.headers().firstValue("content-type").orElse(""));
        // The endpoint's identity signs the answer: the canned SDP comes
        // back intact with the a=identity assertion attached.
        assertTrue(response.body().startsWith(CANNED_ANSWER));
        assertTrue(response.body().contains("a=identity:"));
        assertTrue(response.headers().firstValue("connection").orElse("").equalsIgnoreCase("close"));
        // The channel must have waited for the gathered description.
        assertTrue(engine.peers().stream().allMatch(peer -> peer.gatheredRequests() == 1));
    }

    @Test
    void nonNumericNetworkIdProducesAnAnswerThroughTheServerChannel() throws Exception {
        HttpResponse<String> response = post("/v1/join/not-a-number", "v=0\r\n");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().startsWith(CANNED_ANSWER));
    }

    @Test
    void emptyOfferAnswers400() throws Exception {
        assertEquals(400, post("/v1/join/42", "").statusCode());
    }
}

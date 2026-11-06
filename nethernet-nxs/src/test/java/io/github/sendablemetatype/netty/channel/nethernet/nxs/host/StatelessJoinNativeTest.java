package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CloseCause;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.Peer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerListener;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;
import io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.WebRtcEngine;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionValidator;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostCandidate;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.HostProfile;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OutcomeStage;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.TransportOutcome;
import io.github.sendablemetatype.netty.util.nethernet.DtlsCertificate;
import io.github.sendablemetatype.netty.util.nethernet.IdentityPublicKey;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A stateless NXS join on real libwebrtc: a client that knows only the host's published profile and a ticket the
 * test mints as a provider would, joins the endpoint's shared ICE port with the token as the host's ufrag and the
 * derived password, before the host created anything for it.
 */
@EnabledIfSystemProperty(named = "webrtc.nativeTests", matches = "true")
@Timeout(90)
class StatelessJoinNativeTest {
    private static final long WAIT_SECONDS = 20;
    private static final String KEY_ID = "K001";
    private static final String SECRET = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final NetherNetEngine engine = WebRtcEngine.builder().factories(2).build();
    private final BlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> serverReads = new LinkedBlockingQueue<>();
    private NetherNetServerChannel server;
    private HostTransport transport;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        engine.close().toCompletableFuture().get(20, TimeUnit.SECONDS);
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aClientWithOnlyTheProfileAndATicketJoinsTheSharedPort(@TempDir Path directory) throws Exception {
        // The client's answer carries only the published fingerprint, so DTLS completes only if the peer presents
        // the very certificate the host generated
        DtlsCertificate certificate = DtlsCertificate.loadOrCreate(directory.resolve("dtls-certificate.pem"));
        server = (NetherNetServerChannel) new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel child) {
                        children.add((NetherNetChildChannel) child);
                        child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object message) {
                                ByteBuf data = (ByteBuf) message;
                                serverReads.add(data.toString(StandardCharsets.UTF_8));
                                ctx.writeAndFlush(data);
                            }
                        });
                    }
                })
                .bind(new InetSocketAddress("0.0.0.0", 0)).sync().channel();
        transport = new HostTransport(new HostTransport.Settings(HostTransport.Publication.FIXED, List.of(), false,
                false, false, TicketLedger.Limits.defaults()), certificate, server.eventLoop());
        server.addSource(transport.source());
        transport.started().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        transport.installAdmissionKeys(List.of(new AdmissionKey(KEY_ID, SECRET))).toCompletableFuture()
                .get(WAIT_SECONDS, TimeUnit.SECONDS);
        HostProfile profile = transport.captureHostProfile().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS)
                .profile();
        assertFalse(profile.candidates().isEmpty(), "the host gathered no address on its shared port");

        Client client = new Client();
        Peer peer = engine.createPeer(new PeerConfig(PeerConfig.Role.OFFERER, List.of(), CandidatePolicy.DEFAULT,
                null, null, null), client);
        DataLink reliable = peer.openChannel(ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL));
        peer.openChannel(ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL));
        String offer = peer.offer().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        // The provider's part: a ticket for this client, and the answer the client builds from the profile
        KeyPair loginKey = loginKey();
        String audience = StatelessAdmissionValidator.audience(transport.incarnation());
        StatelessAdmissionCodec.Credentials ticket = ticket(offer, audience, loginKey);
        peer.applyAnswer(answer(offer, profile, ticket)).toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.open(NetherNetConstants.RELIABLE_CHANNEL_LABEL).get(WAIT_SECONDS, TimeUnit.SECONDS));
        NetherNetChildChannel child = children.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(child, "the endpoint exposed no child");

        // One message each way, as single NetherNet segments: a zero count of segments to follow, then the payload
        byte[] ping = {0, 'p', 'i', 'n', 'g'};
        reliable.send(ByteBuffer.wrap(ping), failure -> { });
        assertEquals("ping", serverReads.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertArrayEquals(ping, client.messages.poll(WAIT_SECONDS, TimeUnit.SECONDS), "the endpoint's echo");

        assertEquals(TransportIdentityBinding.Result.ACCEPTED,
                TransportIdentityBinding.verifyLogin(child, loginKey.getPublic()));
        List<TransportOutcome> outcomes = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (outcomes.size() < 2 && System.nanoTime() < deadline) {
            outcomes.addAll(transport.pollOutcomes(256));
            Thread.sleep(50);
        }
        assertEquals(List.of(OutcomeStage.ICE_SEEN, OutcomeStage.DATA_CHANNELS_OPEN),
                outcomes.stream().map(TransportOutcome::stage).toList());
        assertNotNull(outcomes.get(1).remote(), "the selected pair's remote side");
        peer.close().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static KeyPair loginKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp384r1"));
        return generator.generateKeyPair();
    }

    /** What a provider mints for the client's offer, binding the login to its key. */
    private static StatelessAdmissionCodec.Credentials ticket(String offer, String audience, KeyPair loginKey)
            throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] binding = Arrays.copyOf(mac.doFinal(("nxs-identity-binding-v1\0" + audience + "\0"
                + Base64.getEncoder().encodeToString(IdentityPublicKey.canonical(loginKey.getPublic())))
                .getBytes(StandardCharsets.UTF_8)), 16);
        long expiresAt = (System.currentTimeMillis() / 1000 + 60) * 1000;
        String fingerprint = field(offer, "fingerprint");
        if (!fingerprint.startsWith("sha-256 ")) {
            throw new AssertionError("libwebrtc offered another fingerprint: " + fingerprint);
        }
        byte[] nonce = new byte[12];
        new SecureRandom().nextBytes(nonce);
        return StatelessAdmissionCodec.issue(KEY_ID, SECRET, audience, field(offer, "ice-ufrag"),
                new StatelessAdmissionCodec.Claims(expiresAt,
                        fingerprint.substring(8).replace(":", "").toLowerCase(Locale.ROOT), 5000, 262144,
                        HexFormat.of().formatHex(binding), "42", field(offer, "ice-pwd"), new byte[0]),
                nonce);
    }

    /** The host's answer as the client builds it: the profile's candidates and fingerprint, the ticket's credentials. */
    private static String answer(String offer, HostProfile profile, StatelessAdmissionCodec.Credentials ticket) {
        String mid = field(offer, "mid");
        StringBuilder answer = new StringBuilder("v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n")
                .append("a=group:BUNDLE ").append(mid).append("\r\n")
                .append("m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\n")
                .append("a=mid:").append(mid).append("\r\na=setup:active\r\n")
                .append("a=ice-ufrag:").append(ticket.localUfrag()).append("\r\n")
                .append("a=ice-pwd:").append(ticket.icePwd()).append("\r\n")
                .append("a=fingerprint:").append(profile.dtlsFingerprint().value()).append("\r\n")
                .append("a=sctp-port:").append(profile.sctpPort()).append("\r\n")
                .append("a=max-message-size:").append(profile.maxMessageSize()).append("\r\n");
        int foundation = 1;
        for (HostCandidate candidate : profile.candidates()) {
            answer.append("a=candidate:").append(foundation++).append(" 1 udp 2130706431 ")
                    .append(candidate.endpoint().getAddress().getHostAddress()).append(' ')
                    .append(candidate.endpoint().getPort()).append(" typ host\r\n");
        }
        return answer.append("a=end-of-candidates\r\n").toString();
    }

    private static String field(String sdp, String name) {
        return sdp.lines().filter(line -> line.startsWith("a=" + name + ":")).findFirst()
                .orElseThrow(() -> new AssertionError("No a=" + name + " in " + sdp)).substring(name.length() + 3)
                .strip();
    }

    /** The client's events. */
    private static final class Client implements PeerListener {
        final Map<String, CompletableFuture<DataLink>> opened = new ConcurrentHashMap<>();
        final BlockingQueue<byte[]> messages = new LinkedBlockingQueue<>();

        CompletableFuture<DataLink> open(String label) {
            return opened.computeIfAbsent(label, ignored -> new CompletableFuture<>());
        }

        @Override
        public void onLocalCandidate(String candidate, String mid) {
        }

        @Override
        public void onGatheringComplete() {
        }

        @Override
        public boolean onRemoteChannel(DataLink link) {
            return false;
        }

        @Override
        public void onChannelOpen(DataLink link) {
            open(link.parameters().label()).complete(link);
        }

        @Override
        public void onMessage(DataLink link, ByteBuffer message) {
            byte[] bytes = new byte[message.remaining()];
            message.get(bytes);
            messages.add(bytes);
        }

        @Override
        public void onTextMessage(DataLink link, ByteBuffer message) {
        }

        @Override
        public void onQueueLow(DataLink link) {
        }

        @Override
        public void onChannelClosed(DataLink link, CloseCause cause) {
        }

        @Override
        public void onPathSelected(TransportPath path) {
        }

        @Override
        public void onStateChange(PeerState state) {
        }

        @Override
        public void onIceStateChange(IceState state) {
        }

        @Override
        public void onClosed(CloseCause cause) {
        }
    }
}

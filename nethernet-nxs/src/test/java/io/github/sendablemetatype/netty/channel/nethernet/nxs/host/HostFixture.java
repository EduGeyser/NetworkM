package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetChildChannel;
import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeEngine;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakeIceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.FakePeer;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerConfig;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.AdmissionKey;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionCodec;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionValidator;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.util.nethernet.DtlsCertificate;
import io.github.sendablemetatype.netty.util.nethernet.IdentityPublicKey;
import io.github.sendablemetatype.netty.util.nethernet.OfferCheck;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A NetherNet endpoint on the fake engine with NXS as one of its sources, and the provider's side of a stateless
 * ticket: the test mints tokens the way a provider does and holds the checks a client would send.
 */
final class HostFixture implements AutoCloseable {
    static final String KEY_ID = "K001";
    static final String SECRET = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    static final InetSocketAddress CLIENT = new InetSocketAddress("198.51.100.9", 50000);
    static final String CLIENT_UFRAG = "clnt";
    static final String CLIENT_PASSWORD = "clientpassword0123456789";
    static final String CLIENT_FINGERPRINT_HEX = "ab".repeat(32);
    /** What the fake engine's peers offer, so the host's gathering on the shared port completes. */
    static final String PROBE_OFFER = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
            + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=ice-ufrag:prob\r\n"
            + "a=ice-pwd:probepassword0123456789\r\na=fingerprint:sha-256 " + "CD:".repeat(31) + "CD\r\n"
            + "a=setup:actpass\r\na=mid:0\r\na=sctp-port:5000\r\na=max-message-size:262144\r\n";

    final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    final FakeEngine engine = new FakeEngine();
    final LinkedBlockingQueue<NetherNetChildChannel> children = new LinkedBlockingQueue<>();
    final ServerBootstrap bootstrap;
    final DtlsCertificate certificate;
    final KeyPair clientKey;
    NetherNetServerChannel server;
    HostTransport transport;
    FakeIceEndpoint endpoint;

    HostFixture() throws Exception {
        bootstrap = new ServerBootstrap()
                .group(group)
                .channelFactory(() -> new NetherNetServerChannel(engine))
                .option(NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test"))
                .option(NetherChannelOption.NETHER_SERVER_OFFER_CHECK, new OfferCheck(List.of()))
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel child) {
                        children.add((NetherNetChildChannel) child);
                    }
                });
        engine.offerWith(PROBE_OFFER);
        engine.gatherWith(PROBE_OFFER + "a=candidate:1 1 udp 2122260223 127.0.0.1 40000 typ host\r\n"
                + "a=end-of-candidates\r\n");
        X509Bundle bundle = new CertificateBuilder().subject("CN=NetherNet Host")
                .algorithm(CertificateBuilder.Algorithm.ecp256).setIsCertificateAuthority(true).buildSelfSigned();
        certificate = DtlsCertificate.of(bundle.getCertificatePEM(), bundle.getPrivateKeyPEM());
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp384r1"));
        clientKey = generator.generateKeyPair();
    }

    /** The host certificate's fingerprint, as the profile publishes it. */
    DtlsFingerprint fingerprint() {
        return DtlsFingerprint.of(certificate.sha256());
    }

    /** Binds the endpoint and starts NXS on it with these settings. */
    HostTransport start(HostTransport.Settings settings) throws Exception {
        server = (NetherNetServerChannel) bootstrap.bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        endpoint = engine.endpoints().getFirst();
        transport = new HostTransport(settings, certificate, server.eventLoop());
        server.addSource(transport.source());
        transport.started().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return transport;
    }

    static HostTransport.Settings fixed() {
        return new HostTransport.Settings(HostTransport.Publication.FIXED, List.of(), true, false, false,
                TicketLedger.Limits.defaults());
    }

    void installKey() throws Exception {
        transport.installAdmissionKeys(List.of(new AdmissionKey(KEY_ID, SECRET))).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
    }

    String audience() {
        return StatelessAdmissionValidator.audience(transport.incarnation());
    }

    /** A ticket for the fixture's client and key, as the provider issues it, expiring this many seconds ahead. */
    StatelessAdmissionCodec.Credentials mint(long seconds) throws Exception {
        long expiresAt = (System.currentTimeMillis() / 1000 + seconds) * 1000;
        byte[] nonce = new byte[12];
        new SecureRandom().nextBytes(nonce);
        return StatelessAdmissionCodec.issue(KEY_ID, SECRET, audience(), CLIENT_UFRAG,
                new StatelessAdmissionCodec.Claims(expiresAt, CLIENT_FINGERPRINT_HEX, 5000, 262144, binding(), "42",
                        CLIENT_PASSWORD, new byte[0]), nonce);
    }

    /** The token's login binding: the first 16 bytes of an HMAC over the client's canonical key. */
    private String binding() throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] digest = mac.doFinal(("nxs-identity-binding-v1\0" + audience() + "\0"
                + Base64.getEncoder().encodeToString(IdentityPublicKey.canonical(clientKey.getPublic())))
                .getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(Arrays.copyOf(digest, 16));
    }

    /** Holds a check from this address for the ufrag, as the shared port passes it on. */
    Check hold(String localUfrag, InetSocketAddress from) {
        Check check = new Check(localUfrag, from);
        assertTrue(endpoint.hold(check), "the endpoint has no held check handler");
        return check;
    }

    /** Waits until the condition holds. */
    static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting: " + what);
            }
            Thread.sleep(10);
        }
    }

    /** Lets everything queued on the event loop so far run. */
    void sync() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            group.next().submit(() -> { }).sync();
        }
    }

    NetherNetChildChannel child() throws InterruptedException {
        NetherNetChildChannel child = children.poll(5, TimeUnit.SECONDS);
        assertNotNull(child, "no child was exposed");
        return child;
    }

    /** The peers created for joins, leaving out the one the host gathers with. */
    List<FakePeer> answerers() {
        return engine.peers().stream().filter(peer -> peer.config().role() == PeerConfig.Role.ANSWERER).toList();
    }

    /** The newest peer created for a join, waiting for one if there is none yet. */
    FakePeer lastPeer() throws InterruptedException {
        await(() -> !answerers().isEmpty(), "a peer");
        return answerers().getLast();
    }

    @Override
    public void close() {
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    /** A held check the test hands to the endpoint's handler. */
    static final class Check implements IceEndpoint.HeldCheck {
        final String localUfrag;
        final InetSocketAddress remote;
        final CompletableFuture<Void> rejected = new CompletableFuture<>();

        Check(String localUfrag, InetSocketAddress remote) {
            this.localUfrag = localUfrag;
            this.remote = remote;
        }

        @Override
        public InetSocketAddress localAddress() {
            return new InetSocketAddress("127.0.0.1", 40000);
        }

        @Override
        public InetSocketAddress remoteAddress() {
            return remote;
        }

        @Override
        public String localUfrag() {
            return localUfrag;
        }

        @Override
        public String remoteUfrag() {
            return CLIENT_UFRAG;
        }

        @Override
        public void reject() {
            rejected.complete(null);
        }
    }
}

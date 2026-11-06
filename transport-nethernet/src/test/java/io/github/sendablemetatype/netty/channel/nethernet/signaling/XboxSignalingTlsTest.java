package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.pkitesting.X509Bundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLHandshakeException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.cert.CertificateException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Xbox signaling's TLS against a local WebSocket service. The upgrade request carries the Xbox
 * token, so a certificate the client trusts but that names another host must fail the connection
 * before the request is sent.
 */
@Timeout(20)
class XboxSignalingTlsTest {
    private static final String SERVICE_HOST = "signal.test";
    private static final String TOKEN = "MCToken test";

    private final EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final CompletableFuture<String> upgradeToken = new CompletableFuture<>();
    private Channel server;
    private LocalService signaling;

    @AfterEach
    void tearDown() {
        if (signaling != null) {
            signaling.close();
        }
        if (server != null) {
            server.close().syncUninterruptibly();
        }
        serverGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @Test
    void aCertificateForAnotherHostFailsTheConnection() throws Exception {
        X509Bundle authority = TestCertificates.authority();
        startService(TestCertificates.serverContext(TestCertificates.server(authority, "other.test")));
        signaling = new LocalService(authority);

        CompletableFuture<?> attempt = signaling.openSocket(TOKEN);

        Throwable handshake = assertThrows(ExecutionException.class,
                () -> signaling.handshake.get().handshakeFuture().get(5, TimeUnit.SECONDS)).getCause();
        assertInstanceOf(SSLHandshakeException.class, handshake);
        assertInstanceOf(CertificateException.class, rootCause(handshake),
                "the trusted chain is refused for its name: " + handshake);
        assertThrows(ExecutionException.class, () -> attempt.get(5, TimeUnit.SECONDS));
        assertFalse(upgradeToken.isDone(), "the token never reaches a host the certificate does not name");
    }

    @Test
    void aCertificateForTheServiceHostConnects() throws Exception {
        X509Bundle authority = TestCertificates.authority();
        startService(TestCertificates.serverContext(TestCertificates.server(authority, SERVICE_HOST)));
        signaling = new LocalService(authority);

        signaling.openSocket(TOKEN);

        assertEquals(TOKEN, upgradeToken.get(5, TimeUnit.SECONDS));
    }

    private void startService(SslContext context) throws InterruptedException {
        server = new ServerBootstrap()
                .group(serverGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(context.newHandler(ch.alloc()), new HttpServerCodec(),
                                new HttpObjectAggregator(8192), new SimpleChannelInboundHandler<FullHttpRequest>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
                                        upgradeToken.complete(request.headers().get(HttpHeaderNames.AUTHORIZATION));
                                    }
                                });
                    }
                })
                .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /** Xbox signaling pointed at the local service under its public name, trusting the test CA. */
    private final class LocalService extends NetherNetXboxRpcSignaling {
        private final X509Bundle authority;
        private final CompletableFuture<SslHandler> handshake = new CompletableFuture<>();

        private LocalService(X509Bundle authority) {
            super("local", refresh -> CompletableFuture.completedFuture(TOKEN),
                    new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()), null);
            this.authority = authority;
        }

        @Override
        ChannelFuture connectSocket(String token) {
            int port = ((InetSocketAddress) server.localAddress()).getPort();
            URI service = URI.create("wss://" + SERVICE_HOST + ":" + port + "/ws/v1.0/messaging/connect");
            try {
                ChannelFuture connect = connectSocket(token, service, server.localAddress(),
                        authority.toTrustManagerFactory());
                // The pipeline is set up on registration, so it holds the TLS handler once connected.
                connect.addListener(done -> handshake.complete(connect.channel().pipeline().get(SslHandler.class)));
                return connect;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

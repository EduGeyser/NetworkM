package io.github.sendablemetatype.netty.util.nethernet;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/** Serves a key set over plain HTTP on loopback and counts the fetches. */
final class JwksServer implements AutoCloseable {
    private final HttpServer server;
    final AtomicInteger fetches = new AtomicInteger();
    volatile boolean failing;

    JwksServer(String keySet, int maxAgeSeconds) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/keys", exchange -> {
            fetches.incrementAndGet();
            if (failing) {
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
                return;
            }
            byte[] body = keySet.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Cache-Control", "max-age=" + maxAgeSeconds);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    URI uri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/keys");
    }

    TrustedIssuer minecraftStandIn() {
        return TrustedIssuer.jwksAt(uri(), TrustedIssuer.MINECRAFT_ISSUER, TrustedIssuer.MINECRAFT_AUDIENCE);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * One HTTP signaling connection on an embedded channel with frozen time, so deadlines can be
 * stepped through without waiting.
 */
final class EmbeddedHttp implements AutoCloseable {
    static final InetSocketAddress CLIENT = new InetSocketAddress("192.0.2.10", 50123);
    static final InetSocketAddress LOCAL = new InetSocketAddress("127.0.0.1", 19132);

    final EmbeddedChannel channel;

    EmbeddedHttp(NetherNetHttpSignaling signaling) {
        this(signaling, CLIENT);
    }

    EmbeddedHttp(NetherNetHttpSignaling signaling, InetSocketAddress remote) {
        channel = new EmbeddedChannel() {
            @Override
            protected SocketAddress remoteAddress0() {
                return remote;
            }

            @Override
            protected SocketAddress localAddress0() {
                return LOCAL;
            }
        };
        channel.freezeTime();
        signaling.initConnection(channel);
    }

    /** Feeds raw bytes as the client would send them. */
    ByteBuf send(String text) {
        ByteBuf input = Unpooled.copiedBuffer(text, StandardCharsets.UTF_8);
        channel.writeInbound(input);
        channel.runPendingTasks();
        return input;
    }

    void advance(long millis) {
        channel.advanceTimeBy(millis, TimeUnit.MILLISECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
    }

    boolean isOpen() {
        channel.runPendingTasks();
        return channel.isOpen();
    }

    /** Everything written so far, split into responses. */
    List<Response> responses() {
        channel.runPendingTasks();
        StringBuilder written = new StringBuilder();
        for (ByteBuf part; (part = channel.readOutbound()) != null; ) {
            try {
                written.append(part.toString(StandardCharsets.UTF_8));
            } finally {
                part.release();
            }
        }
        return Response.parseAll(written.toString());
    }

    /** The one response written so far. */
    Response response() {
        List<Response> responses = responses();
        if (responses.size() != 1) {
            throw new AssertionError("expected one response, got " + responses);
        }
        return responses.getFirst();
    }

    @Override
    public void close() {
        channel.finishAndReleaseAll();
    }

    static String get(String uri, String... headers) {
        return "GET " + uri + " HTTP/1.1\r\n" + headers(headers) + "\r\n";
    }

    static String post(String networkId, String body, String... headers) {
        return "POST /v1/join/" + networkId + " HTTP/1.1\r\nContent-Length: "
                + body.getBytes(StandardCharsets.UTF_8).length + "\r\n" + headers(headers) + "\r\n" + body;
    }

    /** The given header lines, with a default Host unless one of them is a Host. */
    private static String headers(String... headers) {
        String given = String.join("", headers);
        String lower = given.toLowerCase(Locale.ROOT);
        boolean host = lower.startsWith("host:") || lower.contains("\r\nhost:");
        return (host ? "" : "Host: 127.0.0.1:19132\r\n") + given;
    }

    /** A parsed response, header names lowercase. */
    record Response(int status, Map<String, String> headers, String body) {
        static List<Response> parseAll(String text) {
            List<Response> responses = new ArrayList<>();
            int at = 0;
            while (at < text.length()) {
                int end = text.indexOf("\r\n\r\n", at);
                if (end < 0) {
                    throw new AssertionError("truncated response: " + text.substring(at));
                }
                String[] lines = text.substring(at, end).split("\r\n");
                int status = Integer.parseInt(lines[0].split(" ")[1]);
                Map<String, String> headers = new LinkedHashMap<>();
                for (int i = 1; i < lines.length; i++) {
                    int colon = lines[i].indexOf(':');
                    headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT),
                            lines[i].substring(colon + 1).trim());
                }
                int length = Integer.parseInt(headers.getOrDefault("content-length", "0"));
                at = end + 4;
                responses.add(new Response(status, headers, text.substring(at, at + length)));
                at += length;
            }
            return responses;
        }

        boolean closes() {
            return "close".equalsIgnoreCase(headers.get("connection"));
        }
    }
}

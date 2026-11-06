package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Logs one debug line per HTTP signaling request: who sent it (the client a trusted proxy spoke
 * for, and the proxy), the method, the path, the status of its response and how long it took. The
 * path is logged without its query, and nothing of a body or a header, so no offer, token or
 * client-chosen query reaches the log. Costs nothing while debug logging is off for this class.
 * <p>
 * Sits between the HTTP codec and the aggregator, so it sees every request start and every final
 * response, also those the aggregator writes itself ({@code 413}, {@code 417}). Responses come in
 * request order, so each is matched to the oldest request still waiting for one.
 */
final class HttpAccessLog extends ChannelDuplexHandler {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(HttpAccessLog.class);
    /** Paths are the client's to choose; this keeps a line short. */
    private static final int MAX_PATH_LENGTH = 128;

    private final NetherNetHttpSignaling signaling;
    private final HttpConnection connection;
    private final Queue<Entry> waiting = new ArrayDeque<>(2);

    private record Entry(String line, long startNanos) {
    }

    HttpAccessLog(NetherNetHttpSignaling signaling, HttpConnection connection) {
        this.signaling = signaling;
        this.connection = connection;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest request && log.isDebugEnabled()) {
            SourcedAddress source = signaling.sourceOf(connection, (InetSocketAddress) ctx.channel().remoteAddress(),
                    request.headers().getAll(HttpRequestHandler.FORWARDED_FOR));
            String requester = source.via() == null ? String.valueOf(source.address())
                    : source.address() + " via " + source.via();
            waiting.add(new Entry(requester + " " + request.method() + " " + path(request.uri()),
                    ctx.executor().ticker().nanoTime()));
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        // Interim responses such as 100 Continue leave the request waiting
        if (msg instanceof HttpResponse response && response.status().code() >= 200) {
            Entry entry = waiting.poll();
            if (entry != null) {
                long millis = (ctx.executor().ticker().nanoTime() - entry.startNanos()) / 1_000_000;
                log.debug("{} -> {} ({} ms)", entry.line(), response.status().code(), millis);
            }
        }
        ctx.write(msg, promise);
    }

    /** The request target without its query, printable and bounded. */
    static String path(String uri) {
        int query = uri.indexOf('?');
        String path = query < 0 ? uri : uri.substring(0, query);
        StringBuilder out = new StringBuilder(Math.min(path.length(), MAX_PATH_LENGTH + 3));
        for (int i = 0; i < path.length() && out.length() < MAX_PATH_LENGTH; i++) {
            char c = path.charAt(i);
            out.append(c < 0x20 || c > 0x7e ? '?' : c);
        }
        if (path.length() > MAX_PATH_LENGTH) {
            out.append("...");
        }
        return out.toString();
    }
}

/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet.nxs.control;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.OwnershipGuard;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * One bounded text WebSocket connection on Netty. Authentication, endpoint trust, application acknowledgments,
 * reconnecting and session authority belong to the caller. A completed send means the frame was written, not that
 * the peer's application accepted it.
 *
 * <p>Every bound applies to UTF-8 bytes: one message, the fragments it arrives in, the sends waiting including the
 * one being written, and their total. Connecting, receiving a message until the application accepted it, writing
 * one send and closing each have a deadline. One complete message is delivered at a time; the socket reads on only
 * after the receiver's stage completes. Binary messages, oversized messages and an unnegotiated subprotocol end the
 * connection with a close frame instead of an abort.
 *
 * <p>All state belongs to the owner executor, which also runs the timers and the receiver. Netty events are passed
 * to it in order. The event loop group and the owner are borrowed and never shut down here.
 */
public final class ControlSocket {
    public record Limits(int maxMessageBytes, int maxReceiveParts, int maxPendingSends, long maxPendingBytes,
                         Duration connectTimeout, Duration receiveTimeout, Duration sendTimeout,
                         Duration closeTimeout) {
        public Limits {
            if (maxMessageBytes < 1 || maxReceiveParts < 1 || maxPendingSends < 1 || maxPendingBytes < 1) {
                throw new IllegalArgumentException("WebSocket limits must be positive");
            }
            for (Duration timeout : List.of(connectTimeout, receiveTimeout, sendTimeout, closeTimeout)) {
                if (timeout.isNegative() || timeout.isZero()) {
                    throw new IllegalArgumentException("WebSocket timeouts must be positive");
                }
                timeout.toNanos(); // Rejects durations the scheduler cannot represent
            }
        }
    }

    /** How the peer closed the connection. */
    public record Close(int statusCode, String reason) {
    }

    /** RFC 6455 close codes this carrier sends. */
    public static final int NORMAL = 1000, UNSUPPORTED_DATA = 1003, POLICY_VIOLATION = 1008, TOO_BIG = 1009;

    private final Limits limits;
    private final String subprotocol;
    private final ScheduledExecutorService owner;
    private final Function<String, ? extends CompletionStage<?>> receiver;
    private final CompletableFuture<Void> opened = new CompletableFuture<>();
    private final CompletableFuture<Close> closed = new CompletableFuture<>();
    private final ArrayDeque<Send> sends = new ArrayDeque<>();
    private final ArrayDeque<Received> ready = new ArrayDeque<>();
    private Channel channel;
    private Send activeSend;
    private long pendingBytes;
    private CompositeByteBuf incoming;
    private int receiveParts;
    private ScheduledFuture<?> connectTimer, receiveTimer, sendTimer, closeTimer;
    private boolean connected, closeRequested, closeSent, receiving, terminal;

    private ControlSocket(Limits limits, String subprotocol, ScheduledExecutorService owner,
                          Function<String, ? extends CompletionStage<?>> receiver) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.subprotocol = Objects.requireNonNull(subprotocol, "subprotocol");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.receiver = Objects.requireNonNull(receiver, "receiver");
    }

    /**
     * Starts the upgrade. Production endpoints are {@code wss} with normal certificate and host name checks;
     * {@code ws} serves loopback development only and is never a fallback. No redirect is followed: anything but
     * {@code 101} fails the upgrade.
     *
     * @param tls         The client TLS context for {@code wss}, with host name verification; ignored for {@code ws}
     * @param subprotocol The subprotocol the server must select exactly; empty to request none
     * @param receiver    Takes each complete message on the owner and returns promptly; the next message is read
     *                    once its stage completes, and a failed stage ends the connection
     */
    public static ControlSocket connect(EventLoopGroup group, URI endpoint, String subprotocol,
                                        Map<String, String> headers, SslContext tls, Limits limits,
                                        ScheduledExecutorService owner,
                                        Function<String, ? extends CompletionStage<?>> receiver) {
        Objects.requireNonNull(endpoint, "endpoint");
        String scheme = endpoint.getScheme();
        boolean secure = "wss".equalsIgnoreCase(scheme);
        if (!(secure || "ws".equalsIgnoreCase(scheme)) || endpoint.getHost() == null
                || endpoint.getUserInfo() != null || endpoint.getFragment() != null) {
            throw new IllegalArgumentException("Expected a WebSocket endpoint without user-info or fragment");
        }
        if (secure && tls == null) {
            throw new IllegalArgumentException("A wss endpoint needs a TLS context");
        }
        ControlSocket socket = new ControlSocket(limits, subprotocol, owner, receiver);
        HttpHeaders upgrade = new DefaultHttpHeaders();
        Map.copyOf(headers).forEach(upgrade::add);
        String host = endpoint.getHost().startsWith("[")
                ? endpoint.getHost().substring(1, endpoint.getHost().length() - 1)
                : endpoint.getHost();
        int port = endpoint.getPort() > 0 ? endpoint.getPort() : secure ? 443 : 80;
        WebSocketClientProtocolConfig config = WebSocketClientProtocolConfig.newBuilder()
                .webSocketUri(endpoint)
                .subprotocol(subprotocol.isEmpty() ? null : subprotocol)
                .version(WebSocketVersion.V13)
                .customHeaders(upgrade)
                .maxFramePayloadLength(limits.maxMessageBytes())
                .handleCloseFrames(false)
                .dropPongFrames(true)
                .generateOriginHeader(false)
                .handshakeTimeoutMillis(limits.connectTimeout().toMillis())
                .build();
        owner.execute(() -> socket.connectTimer = socket.schedule(
                () -> socket.fail(new TimeoutException("WebSocket upgrade timed out")), limits.connectTimeout()));
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.min(Integer.MAX_VALUE,
                        limits.connectTimeout().toMillis()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        ChannelPipeline pipeline = channel.pipeline();
                        if (secure) {
                            pipeline.addLast(tls.newHandler(channel.alloc(), host, port));
                        }
                        pipeline.addLast(new HttpClientCodec(), new HttpObjectAggregator(8192),
                                new WebSocketClientProtocolHandler(config), socket.new Handler());
                    }
                });
        try {
            bootstrap.connect(host, port).addListener((ChannelFutureListener) future -> {
                Channel channel = future.channel();
                owner.execute(() -> {
                    socket.channel = channel;
                    if (socket.terminal) {
                        channel.close();
                    } else if (!future.isSuccess()) {
                        socket.fail(future.cause());
                    }
                });
            });
        } catch (RuntimeException failure) {
            owner.execute(() -> socket.fail(failure));
        }
        return socket;
    }

    /** Completes once the upgrade succeeded and the subprotocol matched. */
    public CompletionStage<Void> opened() {
        return opened.minimalCompletionStage();
    }

    /** Completes on the peer's close frame; failures, aborts, violations and timeouts complete it exceptionally. */
    public CompletionStage<Close> closed() {
        return closed.minimalCompletionStage();
    }

    public CompletionStage<Void> sendText(String text) {
        return sendText(text, OwnershipGuard.ALWAYS);
    }

    /**
     * Queues one complete message, failing at once when the queue is full. The guard is checked when the message
     * reaches the socket; if it no longer holds, only this message is dropped and its stage fails with the guard's
     * exception, so one late reply cannot take the connection with it.
     */
    public CompletionStage<Void> sendText(String text, OwnershipGuard guard) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(guard, "guard");
        CompletableFuture<Void> result = new CompletableFuture<>();
        long bytes;
        try {
            bytes = utf8Bytes(text);
        } catch (IllegalArgumentException malformed) {
            return CompletableFuture.failedFuture(malformed);
        }
        owner.execute(() -> {
            if (terminal || closeRequested || !connected) {
                result.completeExceptionally(new IOException("WebSocket is not open"));
            } else if (bytes > limits.maxMessageBytes()) {
                result.completeExceptionally(new IOException("WebSocket message exceeds byte limit"));
            } else if (sends.size() + (activeSend == null ? 0 : 1) >= limits.maxPendingSends()
                    || bytes > limits.maxPendingBytes() - pendingBytes) {
                result.completeExceptionally(new IOException("WebSocket send capacity exhausted"));
            } else {
                sends.add(new Send(text, bytes, guard, result));
                pendingBytes += bytes;
                pump();
            }
        });
        return result.minimalCompletionStage();
    }

    /** Sends what is queued, then a normal close, all within the close deadline. */
    public CompletionStage<Close> closeGracefully() {
        owner.execute(() -> {
            if (terminal || closeRequested) {
                return;
            }
            closeRequested = true;
            closeTimer = schedule(() -> fail(new TimeoutException("WebSocket close timed out")), limits.closeTimeout());
            pump();
        });
        return closed();
    }

    /**
     * Ends the connection over a protocol violation with close code 1008, as the NXS control protocol requires,
     * rather than dropping it silently.
     */
    public void closeForViolation(String reason) {
        owner.execute(() -> violate(POLICY_VIOLATION, reason));
    }

    public void abort() {
        owner.execute(() -> fail(new IOException("WebSocket aborted")));
    }

    private void pump() {
        if (terminal || channel == null || !connected || closeSent) {
            return;
        }
        while (activeSend == null) {
            Send send = sends.poll();
            if (send == null) {
                if (closeRequested) {
                    closeSent = true;
                    channel.writeAndFlush(new CloseWebSocketFrame(NORMAL, ""));
                }
                return;
            }
            try {
                send.guard.requireCurrent();
            } catch (RuntimeException stale) {
                pendingBytes -= send.bytes;
                send.result.completeExceptionally(stale);
                continue;
            }
            activeSend = send;
            sendTimer = schedule(() -> {
                if (activeSend == send) {
                    fail(new TimeoutException("WebSocket send timed out"));
                }
            }, limits.sendTimeout());
            Channel writing = channel;
            writing.writeAndFlush(new TextWebSocketFrame(send.text)).addListener(future -> owner.execute(() -> {
                if (terminal || activeSend != send) {
                    return;
                }
                if (!future.isSuccess()) {
                    fail(future.cause());
                    return;
                }
                cancel(sendTimer);
                activeSend = null;
                pendingBytes -= send.bytes;
                send.result.complete(null);
                pump();
            }));
        }
    }

    private void violate(int code, String reason) {
        if (terminal) {
            return;
        }
        Channel current = channel;
        IOException failure = new IOException("WebSocket closed with " + code + ": " + reason);
        if (current != null && connected && !closeSent) {
            closeSent = true;
            current.writeAndFlush(new CloseWebSocketFrame(code, reason)).addListener(ChannelFutureListener.CLOSE);
            terminate(null, failure, false);
        } else {
            terminate(null, failure, true);
        }
    }

    private void fail(Throwable failure) {
        terminate(null, failure, true);
    }

    /** @param closeChannel Whether to close the channel now, rather than after a close frame went out */
    private void terminate(Close close, Throwable failure, boolean closeChannel) {
        if (terminal) {
            return;
        }
        terminal = true;
        cancel(connectTimer);
        cancel(receiveTimer);
        cancel(sendTimer);
        cancel(closeTimer);
        if (incoming != null) {
            incoming.release();
            incoming = null;
        }
        ready.forEach(received -> cancel(received.timer()));
        ready.clear();
        List<Send> pending = new ArrayList<>(sends);
        sends.clear();
        if (activeSend != null) {
            pending.add(activeSend);
            activeSend = null;
        }
        pendingBytes = 0;
        if (closeChannel && channel != null) {
            channel.close();
        }
        Throwable sendFailure = failure == null ? new IOException("WebSocket peer closed") : failure;
        pending.forEach(send -> send.result.completeExceptionally(sendFailure));
        opened.completeExceptionally(sendFailure);
        if (failure == null) {
            closed.complete(close);
        } else {
            closed.completeExceptionally(failure);
        }
    }

    private void opened(ChannelHandlerContext context, String selected) {
        if (terminal) {
            context.close();
            return;
        }
        if (!subprotocol.equals(selected == null ? "" : selected)) {
            violate(POLICY_VIOLATION, "subprotocol not negotiated");
            return;
        }
        cancel(connectTimer);
        connected = true;
        opened.complete(null);
        pump(); // A close may have been requested during the upgrade
    }

    private void fragment(ChannelHandlerContext context, WebSocketFrame frame) {
        try {
            if (terminal) {
                return;
            }
            boolean first = frame instanceof TextWebSocketFrame;
            if (first == (incoming != null)) {
                violate(POLICY_VIOLATION, "unexpected continuation");
                return;
            }
            if (first) {
                incoming = context.alloc().compositeBuffer(limits.maxReceiveParts() + 1);
                receiveParts = 0;
                // Also bounds the time the application takes to accept the complete message
                receiveTimer = schedule(() -> fail(new TimeoutException("WebSocket receive timed out")),
                        limits.receiveTimeout());
            }
            receiveParts++;
            if (receiveParts > limits.maxReceiveParts()
                    || frame.content().readableBytes() > limits.maxMessageBytes() - incoming.readableBytes()) {
                violate(TOO_BIG, "message exceeds limit");
                return;
            }
            incoming.addComponent(true, frame.content().retain());
            if (!frame.isFinalFragment()) {
                return;
            }
            String message;
            try {
                message = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(incoming.nioBuffer())
                        .toString();
            } catch (CharacterCodingException malformed) {
                violate(UNSUPPORTED_DATA, "invalid UTF-8");
                return;
            } finally {
                incoming.release();
                incoming = null;
            }
            deliver(context, message);
        } finally {
            ReferenceCountUtil.release(frame);
        }
    }

    /**
     * Hands one message to the receiver at a time. Messages decoded from the same read wait their turn, and nothing
     * more is read until the receiver accepted them all.
     */
    private void deliver(ChannelHandlerContext context, String message) {
        context.channel().config().setAutoRead(false);
        ready.add(new Received(message, receiveTimer));
        if (!receiving) {
            next(context);
        }
    }

    private void next(ChannelHandlerContext context) {
        Received received = ready.poll();
        if (received == null) {
            receiving = false;
            context.channel().config().setAutoRead(true);
            return;
        }
        receiving = true;
        CompletionStage<?> stage;
        try {
            stage = Objects.requireNonNull(receiver.apply(received.message()), "receiver stage");
        } catch (RuntimeException failure) {
            fail(failure);
            return;
        }
        stage.whenComplete((ignored, failure) -> owner.execute(() -> {
            if (terminal) {
                return;
            }
            if (failure != null) {
                fail(failure);
                return;
            }
            cancel(received.timer());
            next(context);
        }));
    }

    private ScheduledFuture<?> schedule(Runnable task, Duration delay) {
        return owner.schedule(task, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static void cancel(ScheduledFuture<?> timer) {
        if (timer != null) {
            timer.cancel(false);
        }
    }

    /** UTF-8 length of a string, refusing unpaired surrogates, which have no UTF-8 form. */
    static long utf8Bytes(CharSequence value) {
        long bytes = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                i++;
                if (i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) {
                    throw new IllegalArgumentException("Text contains an unpaired surrogate");
                }
                bytes += 4;
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException("Text contains an unpaired surrogate");
            } else if (c < 0x80) {
                bytes += 1;
            } else if (c < 0x800) {
                bytes += 2;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }

    private record Send(String text, long bytes, OwnershipGuard guard, CompletableFuture<Void> result) {
    }

    /** A complete message waiting for the receiver, with the deadline it must be accepted by. */
    private record Received(String message, ScheduledFuture<?> timer) {
    }

    /** Hands every event to the owner, in order. */
    private final class Handler extends ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object event) {
            if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                String selected = context.pipeline().get(WebSocketClientProtocolHandler.class).handshaker()
                        .actualSubprotocol();
                owner.execute(() -> opened(context, selected));
            } else if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_TIMEOUT) {
                owner.execute(() -> fail(new TimeoutException("WebSocket upgrade timed out")));
            }
            context.fireUserEventTriggered(event);
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) {
            switch (message) {
                case TextWebSocketFrame text -> owner.execute(() -> fragment(context, text));
                case ContinuationWebSocketFrame continuation -> owner.execute(() -> fragment(context, continuation));
                case CloseWebSocketFrame close -> {
                    int code = close.statusCode();
                    String reason = close.reasonText();
                    close.release();
                    owner.execute(() -> {
                        if (!closeSent && context.channel().isActive()) {
                            closeSent = true;
                            context.writeAndFlush(new CloseWebSocketFrame(code < 0 ? NORMAL : code, ""))
                                    .addListener(ChannelFutureListener.CLOSE);
                        } else {
                            context.close();
                        }
                        terminate(new Close(code, reason), null, false);
                    });
                }
                case BinaryWebSocketFrame binary -> {
                    binary.release();
                    owner.execute(() -> violate(UNSUPPORTED_DATA, "binary messages are unsupported"));
                }
                default -> ReferenceCountUtil.release(message);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) {
            owner.execute(() -> fail(new IOException("WebSocket connection closed")));
            context.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            owner.execute(() -> fail(cause));
            context.close();
        }
    }
}

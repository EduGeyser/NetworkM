package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.codec.NetherNetServerDataCodec;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertisement;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanAdvertiser;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanDiscoveryRequest;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.lan.LanServerData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * What LAN discovery answers requests with: a fixed record, encoded once, or a {@link LanAdvertiser}
 * asked per request through {@link ProviderCalls}, with a deadline of its own and the same bound as
 * HTTP's status provider. When the advertiser does not deliver, the last server data it delivered in time for
 * everyone goes out instead, or no response while there is none; data it chose for one requester
 * ({@link LanAdvertisement#advertiseToRequester}) is never repeated to another. Each advertiser has a fallback of its own, so
 * a call to one that was replaced never reaches its successor's. Only v7 server data is advertised
 * (ruling F-3).
 */
final class LanAdvertisements {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(LanAdvertisements.class);
    // Hex expansion, 24 plaintext header bytes, a 32-byte MAC and AES padding must fit a 65,507-byte UDP payload.
    private static final int MAX_SERVER_DATA_BYTES = 32_723;

    private sealed interface Mode {
    }

    /** @param payload the encoded response payload */
    private record Fixed(byte[] payload) implements Mode {
    }

    /**
     * @param last the last server data this advertiser delivered in time: the fallback, and an
     *             encoding cache
     */
    private record Asked(LanAdvertiser advertiser, ProviderCalls calls, AtomicReference<Encoded> last)
            implements Mode {

        /** Asks the advertiser; may block. */
        Reply reply(LanDiscoveryRequest request) {
            return switch (advertiser.advertise(request)) {
                case LanAdvertisement.Advertise(var data) -> new Produced(encoded(data));
                // Sent, not Produced, so it never becomes the fallback another requester gets
                case LanAdvertisement.AdvertiseToRequester(var data) -> new Sent(Optional.of(encode(data)));
                case LanAdvertisement.Decline _ -> new Sent(Optional.empty());
                case null -> throw new NullPointerException("The LAN advertiser answered null");
            };
        }

        /** Encodes what the advertiser produced, unless it equals the last record. */
        private Encoded encoded(LanServerData.V7 data) {
            Encoded previous = last.get();
            return previous != null && previous.data().equals(data) ? previous : new Encoded(data, encode(data));
        }

        Reply fallback() {
            Encoded previous = last.get();
            return new Sent(previous == null ? Optional.empty() : Optional.of(previous.payload()));
        }
    }

    /** Server data the advertiser produced, with its encoded payload. */
    private record Encoded(LanServerData.V7 data, byte[] payload) {
    }

    /** What one request gets. */
    private sealed interface Reply {
        Optional<byte[]> payload();
    }

    /** Server data the advertiser delivered within the deadline. */
    private record Produced(Encoded encoded) implements Reply {
        @Override
        public Optional<byte[]> payload() {
            return Optional.of(encoded.payload());
        }
    }

    /** A decline, or the fallback. */
    private record Sent(Optional<byte[]> payload) implements Reply {
    }

    private final Executor executor;
    /** Null while nothing is advertised: requests get no response. */
    private volatile Mode mode;

    /** @param executor runs the advertiser's calls; not an I/O loop */
    LanAdvertisements(Executor executor) {
        this.executor = executor;
    }

    /**
     * Answers every request with this server data from now on.
     *
     * @throws IllegalArgumentException if it does not fit one datagram; what was advertised stays
     */
    void fix(LanServerData.V7 data) {
        mode = new Fixed(encode(Objects.requireNonNull(data, "data")));
    }

    /**
     * Asks this advertiser per request from now on.
     *
     * @param timeout how long one call may take
     * @throws IllegalArgumentException if the timeout is not positive; what was advertised stays
     */
    void ask(LanAdvertiser advertiser, Duration timeout) {
        Objects.requireNonNull(advertiser, "advertiser");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
        mode = new Asked(advertiser, new ProviderCalls("LAN advertiser", log, timeout, executor),
                new AtomicReference<>());
    }

    /**
     * What one request is answered with.
     *
     * @param request the request, already decrypted and validated
     * @param loop    the discovery loop, which runs the deadline
     * @return the response payload, or empty for no response; completes on the advertiser's
     *         thread or the loop
     */
    CompletableFuture<Optional<byte[]>> answer(LanDiscoveryRequest request, EventExecutor loop) {
        return switch (mode) {
            case null -> CompletableFuture.completedFuture(Optional.empty());
            case Fixed fixed -> CompletableFuture.completedFuture(Optional.of(fixed.payload()));
            case Asked asked -> asked.calls().call(() -> asked.reply(request), asked::fallback, loop)
                    .thenApply(reply -> {
                        // A call that missed its deadline lost to the fallback, so it never gets here
                        if (reply instanceof Produced produced) {
                            asked.last().set(produced.encoded());
                        }
                        return reply.payload();
                    });
        };
    }

    /** The response payload: the server data's length as hex text, then the hex text. */
    private static byte[] encode(LanServerData.V7 data) {
        ByteBuf buf = Unpooled.buffer(128, MAX_SERVER_DATA_BYTES);
        byte[] hex;
        try {
            NetherNetServerDataCodec.encode(buf, data);
            hex = ByteBufUtil.hexDump(buf).getBytes(StandardCharsets.US_ASCII);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("LAN advertisement exceeds the UDP payload limit", e);
        } finally {
            buf.release();
        }
        ByteBuf response = Unpooled.buffer(4 + hex.length);
        try {
            response.writeIntLE(hex.length);
            response.writeBytes(hex);
            byte[] payload = new byte[response.readableBytes()];
            response.readBytes(payload);
            return payload;
        } finally {
            response.release();
        }
    }
}

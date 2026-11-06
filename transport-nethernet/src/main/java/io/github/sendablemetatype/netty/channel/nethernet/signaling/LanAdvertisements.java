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

/**
 * What LAN discovery answers requests with: a fixed record, encoded once, or a {@link LanAdvertiser}
 * asked per request through {@link ProviderCalls}, with the same deadline and bound as HTTP's status
 * provider. When the advertiser does not deliver, the last server data it produced goes out
 * instead, or no response while there is none. Only v7 server data is advertised (ruling F-3).
 */
final class LanAdvertisements {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(LanAdvertisements.class);
    /** As HTTP's status provider has by default. */
    static final Duration ADVERTISER_TIMEOUT = Duration.ofSeconds(1);
    // Hex expansion, 24 plaintext header bytes, a 32-byte MAC and AES padding must fit a 65,507-byte UDP payload.
    private static final int MAX_SERVER_DATA_BYTES = 32_723;

    private sealed interface Mode {
    }

    /** @param payload the encoded response payload */
    private record Fixed(byte[] payload) implements Mode {
    }

    private record Asked(LanAdvertiser advertiser, ProviderCalls calls) implements Mode {
    }

    /** Server data the advertiser produced, with its encoded payload. */
    private record Encoded(LanServerData.V7 data, byte[] payload) {
    }

    private final Duration timeout;
    private final Executor executor;
    /** Null while nothing is advertised: requests get no response. */
    private volatile Mode mode;
    /** The last server data the current advertiser produced: the fallback, and an encoding cache. */
    private volatile Encoded last;

    /**
     * @param timeout  how long one advertiser call may take
     * @param executor runs the advertiser's calls; not an I/O loop
     */
    LanAdvertisements(Duration timeout, Executor executor) {
        this.timeout = timeout;
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

    /** Asks this advertiser per request from now on. */
    void ask(LanAdvertiser advertiser) {
        Objects.requireNonNull(advertiser, "advertiser");
        last = null;
        mode = new Asked(advertiser, new ProviderCalls("LAN advertiser", log, timeout, executor));
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
            case Asked asked -> asked.calls().call(() -> switch (asked.advertiser().advertise(request)) {
                case LanAdvertisement.Advertise(var data) -> Optional.of(produced(data));
                case LanAdvertisement.Decline _ -> Optional.<byte[]>empty();
                case null -> throw new NullPointerException("The LAN advertiser answered null");
            }, this::fallback, loop);
        };
    }

    /** Encodes what the advertiser produced, unless it equals the last record, and keeps it. */
    private byte[] produced(LanServerData.V7 data) {
        Encoded previous = last;
        if (previous != null && previous.data().equals(data)) {
            return previous.payload();
        }
        byte[] payload = encode(data);
        last = new Encoded(data, payload);
        return payload;
    }

    private Optional<byte[]> fallback() {
        Encoded previous = last;
        return previous == null ? Optional.empty() : Optional.of(previous.payload());
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

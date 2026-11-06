/* Copyright 2026 CloudburstMC. Licensed under the Apache License, Version 2.0. */
package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import java.nio.ByteBuffer;
import java.util.Objects;

import static io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.invalid;

/**
 * The diagnostic transport profile: the two NetherNet data channels with their exact labels and reliability, and
 * one 56-byte frame received on the reliable one. The endpoint reports what its engine sees; anything else fails
 * the attempt. Thread safe; the engine may call from its own threads.
 */
public final class DiagnosticChannels {
    static final String RELIABLE = "ReliableDataChannel", UNRELIABLE = "UnreliableDataChannel";

    /**
     * What the engine reports for an opened data channel.
     *
     * @param maxRetransmits        The retransmission limit, null when unlimited
     * @param maxPacketLifeTimeMillis The lifetime limit, null when unlimited
     */
    public record Channel(String label, String protocol, boolean ordered, Integer maxRetransmits,
                          Integer maxPacketLifeTimeMillis) {
        public Channel {
            Objects.requireNonNull(label, "label");
            Objects.requireNonNull(protocol, "protocol");
        }

        /** The reliable NetherNet channel, as the prober creates it. */
        public static Channel reliable() {
            return new Channel(RELIABLE, "", true, null, null);
        }

        /** The unreliable NetherNet channel, as the prober creates it. */
        public static Channel unreliable() {
            return new Channel(UNRELIABLE, "", false, 0, null);
        }
    }

    private final boolean[] open = new boolean[2];
    private boolean connected, failed, protocolFailed;
    private byte[] incoming;
    private int receivedFrames, receivedBytes;

    /**
     * A data channel opened. Only the two NetherNet channels, once each, with their reliability are allowed.
     *
     * @throws IllegalArgumentException If the channel is anything else, which fails the attempt
     */
    public synchronized void opened(Channel channel) {
        int index = switch (channel.label()) {
            case RELIABLE -> 0;
            case UNRELIABLE -> 1;
            default -> -1;
        };
        boolean reliable = channel.ordered() && channel.maxRetransmits() == null;
        boolean unreliable = !channel.ordered() && Integer.valueOf(0).equals(channel.maxRetransmits());
        if (index < 0
                || open[index]
                || !channel.protocol().isEmpty()
                || channel.maxPacketLifeTimeMillis() != null
                || (index == 0 ? !reliable : !unreliable)) {
            protocolFailed = true;
            throw invalid();
        }
        open[index] = true;
    }

    /** The peer connection reached its connected state. */
    public synchronized void connected() {
        connected = true;
    }

    /** The peer connection or a data channel failed or closed. */
    public synchronized void failed() {
        failed = true;
    }

    /** Whether the peer is connected and both channels are open, so the exchange may start. */
    public synchronized boolean ready() {
        return connected && open[0] && open[1] && !failed && !protocolFailed;
    }

    /** Whether a channel failed below the diagnostic protocol. */
    public synchronized boolean failedTransport() {
        return failed;
    }

    /** Whether the peer broke the diagnostic protocol. */
    public synchronized boolean protocolFailed() {
        return protocolFailed;
    }

    /** The frames received so far. */
    public synchronized int receivedFrames() {
        return receivedFrames;
    }

    /** The bytes received so far. */
    public synchronized int receivedBytes() {
        return receivedBytes;
    }

    /** A text message, which the profile never allows. */
    public synchronized void receivedText() {
        protocolFailed = true;
    }

    /**
     * A binary message. The engine's buffer is borrowed, so the one permitted frame is copied before returning.
     *
     * @param reliable Whether it came on the reliable channel
     */
    public synchronized void received(boolean reliable, ByteBuffer bytes) {
        if (protocolFailed) {
            return;
        }
        if (!reliable || bytes.remaining() != DiagnosticExchange.FRAME_BYTES || receivedFrames != 0) {
            protocolFailed = true;
            return;
        }
        receivedFrames = 1;
        receivedBytes = DiagnosticExchange.FRAME_BYTES;
        incoming = new byte[DiagnosticExchange.FRAME_BYTES];
        bytes.get(incoming);
    }

    /** @return The received frame, once, or null */
    public synchronized byte[] poll() {
        byte[] frame = incoming;
        incoming = null;
        return frame;
    }

    /** Drops a received frame that was not polled. */
    public synchronized void clear() {
        incoming = null;
    }
}

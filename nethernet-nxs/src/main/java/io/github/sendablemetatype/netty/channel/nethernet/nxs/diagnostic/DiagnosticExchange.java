/* Copyright 2026 CloudburstMC. Licensed under the Apache License, Version 2.0. */
package io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import static io.github.sendablemetatype.netty.channel.nethernet.nxs.diagnostic.DiagnosticAdmissionCodec.*;

/** One reliable PING/PONG on the admitted, pinned DTLS connection. */
public final class DiagnosticExchange {
    public static final int FRAME_BYTES = 56, PING = 2, PONG = 3;

    /** Writes a frame the exchange hands over and no longer touches. */
    @FunctionalInterface
    public interface Sender {
        void send(int channel, byte[] ownedFrame);
    }

    private final String attempt;
    private final boolean host;
    private final Sender sender;
    private final byte[] nonce = new byte[32];
    private int sentFrames, sentBytes, receivedFrames, receivedBytes;
    private boolean started, received, failed;

    /** Callers must validate admission, pinned DTLS and the selected endpoint before starting. */
    public DiagnosticExchange(String attemptIdHex, boolean host, Sender sender) {
        unhex(attemptIdHex, 16);
        this.attempt = attemptIdHex;
        this.host = host;
        this.sender = java.util.Objects.requireNonNull(sender);
        if (!host) {
            new SecureRandom().nextBytes(nonce);
        }
    }

    /** Starts the exchange; the prober sends its PING now. Runs once. */
    public synchronized void start() {
        if (started || failed) {
            throw invalid();
        }
        started = true;
        if (!host) {
            send(PING, nonce);
        }
    }

    /**
     * Takes the peer's frame: the host answers a valid PING with its PONG, the prober checks the echo.
     *
     * @throws IllegalArgumentException If the frame is not the one the exchange expects
     */
    public synchronized void receive(int channel, byte[] input) {
        if (!started
                || failed
                || received
                || input == null
                || input.length != FRAME_BYTES
                || channel != 0) {
            fail();
        }
        receivedFrames++;
        receivedBytes += input.length;
        byte[] frame = input.clone();
        if (!Arrays.equals(Arrays.copyOf(frame, 6), new byte[] {0, 78, 88, 68, 80, 1})
                || frame[7] != 0
                || !Arrays.equals(Arrays.copyOfRange(frame, 8, 24), unhex(attempt, 16))
                || frame[6] != (host ? PING : PONG)) {
            fail();
        }
        byte[] value = Arrays.copyOfRange(frame, 24, 56);
        if (!host && !MessageDigest.isEqual(nonce, value)) {
            fail();
        }
        received = true;
        if (host) {
            send(PONG, value);
        }
    }

    /** Only the prober can verify that its original random PING was echoed. */
    public synchronized boolean complete() {
        return !host && !failed && received;
    }

    /** The frames sent so far. */
    public synchronized int sentFrames() {
        return sentFrames;
    }

    /** The bytes sent so far. */
    public synchronized int sentBytes() {
        return sentBytes;
    }

    /** The frames received so far. */
    public synchronized int receivedFrames() {
        return receivedFrames;
    }

    /** The bytes received so far. */
    public synchronized int receivedBytes() {
        return receivedBytes;
    }

    private void send(int kind, byte[] value) {
        sentFrames++;
        sentBytes += FRAME_BYTES;
        try {
            sender.send(0, encode(attempt, kind, 0, value));
        } catch (RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    /** The 56-byte frame for a PING or PONG of an attempt, on channel 0. */
    public static byte[] encode(String attemptIdHex, int kind, int channel, byte[] value) {
        if (value.length != 32 || (kind != PING && kind != PONG) || channel != 0) {
            throw invalid();
        }
        return ByteBuffer.allocate(FRAME_BYTES)
                .put(new byte[] {0, 78, 88, 68, 80, 1, (byte) kind, 0})
                .put(unhex(attemptIdHex, 16))
                .put(value)
                .array();
    }

    private void fail() {
        failed = true;
        throw invalid();
    }
}

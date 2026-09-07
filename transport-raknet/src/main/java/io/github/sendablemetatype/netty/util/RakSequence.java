package io.github.sendablemetatype.netty.util;

/** Arithmetic for RakNet's unsigned 24-bit wire counters. */
public final class RakSequence {
    public static final int MASK = 0xffffff;

    private RakSequence() {
    }

    /** Signed distance within half the sequence space, including across a wrap. */
    public static int difference(int index, int reference) {
        return (index - reference) << 8 >> 8;
    }
}

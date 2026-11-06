package io.github.sendablemetatype.netty.channel.nethernet.nxs;

/** The client's wall and monotonic clocks, so tests can run its schedule without waiting. */
interface ProviderClock {
    ProviderClock SYSTEM = new ProviderClock() {
        @Override
        public long millis() {
            return System.currentTimeMillis();
        }

        @Override
        public long nanos() {
            return System.nanoTime();
        }
    };

    /** Epoch milliseconds, as signatures and deadlines on the wire use. */
    long millis();

    /** A monotonic reading for local timers. */
    long nanos();
}

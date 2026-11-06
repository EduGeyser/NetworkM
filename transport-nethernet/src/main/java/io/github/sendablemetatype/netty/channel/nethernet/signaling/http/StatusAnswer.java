package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;

import java.util.Objects;

/** How a {@link StatusProvider} answers one status request. */
public sealed interface StatusAnswer {

    /**
     * Serves NetherNet with this status document.
     *
     * @param status the document
     * @return the answer
     */
    static StatusAnswer serve(NetherNetServerStatus status) {
        return new Serve(status);
    }

    /**
     * Declines NetherNet for this request with a {@code 404}. The client reads any status other
     * than 2xx as no NetherNet here, and one that still supports RakNet joins over RakNet instead.
     *
     * @return the answer
     */
    static StatusAnswer decline() {
        return Decline.INSTANCE;
    }

    /**
     * Serves NetherNet with this status document.
     *
     * @param status the document
     */
    record Serve(NetherNetServerStatus status) implements StatusAnswer {
        public Serve {
            Objects.requireNonNull(status, "status");
        }
    }

    /** Declines NetherNet for this request. */
    enum Decline implements StatusAnswer {
        INSTANCE
    }
}

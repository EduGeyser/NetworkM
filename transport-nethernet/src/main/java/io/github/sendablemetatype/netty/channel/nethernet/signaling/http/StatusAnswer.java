package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerStatus;

import java.util.Objects;

/** How a {@link StatusProvider} answers one status request. */
public sealed interface StatusAnswer {

    /**
     * Serves NetherNet with this status document. The document may also answer other requests
     * that name the same {@code Host}, as the fallback when the provider fails, is late or has no
     * room. Use {@link #serveToRequester} for a document shaped for this requester.
     *
     * @param status the document
     * @return the answer
     */
    static StatusAnswer serve(NetherNetServerStatus status) {
        return new Serve(status, true);
    }

    /**
     * Serves NetherNet with this status document to this request alone, for a document that may
     * depend on the requester, such as a MOTD chosen by its address. The fallback never repeats
     * it to anyone, so a request whose provider call fails gets the last shared document for its
     * {@code Host} instead, or an empty {@code 200}.
     *
     * @param status the document
     * @return the answer
     */
    static StatusAnswer serveToRequester(NetherNetServerStatus status) {
        return new Serve(status, false);
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
     * @param shared whether the document may answer other requests for the same {@code Host}
     */
    record Serve(NetherNetServerStatus status, boolean shared) implements StatusAnswer {
        public Serve {
            Objects.requireNonNull(status, "status");
        }
    }

    /** Declines NetherNet for this request. */
    enum Decline implements StatusAnswer {
        INSTANCE
    }
}

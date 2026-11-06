package io.github.sendablemetatype.netty.channel.nethernet.signaling.lan;

import java.util.Objects;

/** How a {@link LanAdvertiser} answers one discovery request. */
public sealed interface LanAdvertisement {

    /**
     * Answers the request with this server data.
     *
     * @param data the advertisement
     * @return the answer
     */
    static LanAdvertisement advertise(LanServerData.V7 data) {
        return new Advertise(data);
    }

    /**
     * Sends no response: the requester does not list this server.
     *
     * @return the answer
     */
    static LanAdvertisement decline() {
        return Decline.INSTANCE;
    }

    /**
     * Answers the request with this server data.
     *
     * @param data the advertisement
     */
    record Advertise(LanServerData.V7 data) implements LanAdvertisement {
        public Advertise {
            Objects.requireNonNull(data, "data");
        }
    }

    /** Sends no response. */
    enum Decline implements LanAdvertisement {
        INSTANCE
    }
}

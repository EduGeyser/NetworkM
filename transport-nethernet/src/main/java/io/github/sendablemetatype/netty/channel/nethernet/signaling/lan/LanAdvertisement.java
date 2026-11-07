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
     * Answers this request alone with this server data, chosen for the requester (its address or
     * network ID). It is never repeated to another requester: when a later call misses its
     * deadline, the fallback is the last data answered with {@link #advertise}, or no response.
     *
     * @param data the advertisement
     * @return the answer
     */
    static LanAdvertisement advertiseToRequester(LanServerData.V7 data) {
        return new AdvertiseToRequester(data);
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

    /**
     * Answers this request alone with this server data; see {@link #advertiseToRequester}.
     *
     * @param data the advertisement
     */
    record AdvertiseToRequester(LanServerData.V7 data) implements LanAdvertisement {
        public AdvertiseToRequester {
            Objects.requireNonNull(data, "data");
        }
    }

    /** Sends no response. */
    enum Decline implements LanAdvertisement {
        INSTANCE
    }
}

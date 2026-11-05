package io.github.sendablemetatype.netty.channel.nethernet.address;

import java.util.Objects;
import java.util.Optional;

/**
 * Decides the remote address one NetherNet channel reports, from where signaling met the peer
 * and the selected ICE pairs.
 * <p>
 * Before the channel goes active it reports the signaled address. From activation on it follows
 * the newest selected pair: a frozen address goes stale once a client switches networks. A
 * relayed pair reports the TURN relay, the transport peer actually connected, never what the peer
 * claims behind it. With the proxied-address preference, an address a trusted signaling proxy
 * declared stands instead of any pair.
 * <p>
 * Event loop only: feed it selected pairs on the channel's loop, so that every change is ordered
 * with the pipeline. Not thread-safe.
 */
public final class RemoteAddressTracker {
    private final SourcedAddress signaled;
    private final boolean preferProxied;
    private SourcedAddress media;
    private SourcedAddress reported;
    private boolean active;

    /**
     * @param signaled      Where signaling met the peer, {@link SourcedAddress#unknown()} when it did not say
     * @param preferProxied Whether an address a trusted signaling proxy declared wins over the ICE pair
     */
    public RemoteAddressTracker(SourcedAddress signaled, boolean preferProxied) {
        this.signaled = Objects.requireNonNull(signaled, "signaled");
        this.preferProxied = preferProxied;
        this.reported = signaled;
    }

    /**
     * @return Where signaling met the peer, kept for the channel's life. Per-address limits counted
     *         the join under this address and release it under the same one.
     */
    public SourcedAddress signaled() {
        return this.signaled;
    }

    /**
     * @return The newest selected pair's remote side, or empty before ICE selected one
     */
    public Optional<SourcedAddress> media() {
        return Optional.ofNullable(this.media);
    }

    /**
     * @return The address the channel reports now
     */
    public SourcedAddress reported() {
        return this.reported;
    }

    /**
     * Takes the newest selected pair as the reported address, right before the channel fires
     * {@code channelActive}. The switch from the signaled address fires no event: consumers read
     * the address when the channel goes active.
     *
     * @return The address the channel reports from now on
     */
    public SourcedAddress activate() {
        this.active = true;
        this.reported = reported(this.signaled, this.media, this.preferProxied);
        return this.reported;
    }

    /**
     * Notes a newly selected pair. Before activation it only becomes the candidate for
     * {@link #activate}; after, it moves the reported address.
     *
     * @param pair The pair's remote side, from {@link SourcedAddress#selectedPair}
     * @return The change to fire as a user event, when the reported address moved
     */
    public Optional<RemoteAddressChangedEvent> selectedPair(SourcedAddress pair) {
        this.media = Objects.requireNonNull(pair, "pair");
        if (!this.active) {
            return Optional.empty();
        }
        SourcedAddress previous = this.reported;
        this.reported = reported(this.signaled, pair, this.preferProxied);
        // A new relay for the same client changes the source but not the address consumers key by
        return previous.address().equals(this.reported.address()) ? Optional.empty()
                : Optional.of(new RemoteAddressChangedEvent(previous, this.reported));
    }

    static SourcedAddress reported(SourcedAddress signaled, SourcedAddress media, boolean preferProxied) {
        if (media == null || (preferProxied && signaled.proxied())) {
            return signaled;
        }
        return media;
    }
}

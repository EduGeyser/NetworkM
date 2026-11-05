package io.github.sendablemetatype.netty.channel.nethernet.address;

import java.util.Objects;

/**
 * Fired down a NetherNet channel's pipeline as a user event when its remote address moves after
 * the channel went active, as when ICE renominates after a client switches networks. Each source
 * says where the address came from. {@code remoteAddress()} already returns the new address when
 * the event fires.
 *
 * @param previous The address reported before
 * @param current  The address reported now
 */
public record RemoteAddressChangedEvent(SourcedAddress previous, SourcedAddress current) {
    public RemoteAddressChangedEvent {
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(current, "current");
    }
}

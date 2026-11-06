package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;

import java.util.Objects;

/**
 * Admits the two data channels a NetherNet client opens, at most one of each.
 * A channel is admitted only with the expected label and delivery parameters:
 * the reliable one ordered with unlimited retransmissions, the unreliable one
 * unordered with no retransmissions. Duplicates, unknown channels and channels
 * arriving after the connection closed are refused, and the engine closes
 * them. The subprotocol is not checked, for compatibility with older clients.
 */
final class DataChannelSlots {
    private DataLink reliable;
    private DataLink unreliable;
    private boolean closed;

    /**
     * @param link a channel the remote peer opened
     * @return whether the channel is kept; also true for a link already admitted
     */
    synchronized boolean admit(DataLink link) {
        Objects.requireNonNull(link, "link");
        if (link == reliable || link == unreliable) {
            return true;
        }
        if (closed) {
            return false;
        }
        ChannelParameters parameters = link.parameters();
        if (reliable == null && isReliable(parameters)) {
            reliable = link;
            return true;
        }
        if (unreliable == null && isUnreliable(parameters)) {
            unreliable = link;
            return true;
        }
        return false;
    }

    synchronized DataLink reliable() {
        return reliable;
    }

    synchronized DataLink unreliable() {
        return unreliable;
    }

    /** Refuses every channel from now on. */
    synchronized void stopAccepting() {
        closed = true;
    }

    static boolean isReliable(ChannelParameters parameters) {
        return NetherNetConstants.RELIABLE_CHANNEL_LABEL.equals(parameters.label())
                && !parameters.negotiated() && parameters.ordered() && parameters.reliable();
    }

    static boolean isUnreliable(ChannelParameters parameters) {
        return NetherNetConstants.UNRELIABLE_CHANNEL_LABEL.equals(parameters.label())
                && !parameters.negotiated() && !parameters.ordered() && !parameters.reliable()
                && parameters.maxRetransmits().orElse(0) == 0
                && parameters.maxPacketLifeTimeMs().orElse(0) == 0;
    }
}

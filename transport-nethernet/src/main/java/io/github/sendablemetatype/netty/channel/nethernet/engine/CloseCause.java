package io.github.sendablemetatype.netty.channel.nethernet.engine;

/**
 * Why a peer or one of its data links ended. The transport maps these to
 * {@link io.github.sendablemetatype.netty.channel.TransportDisconnectReason}.
 */
public enum CloseCause {
    /** Creating the peer or applying a description failed. */
    NEGOTIATION_FAILED,
    /** The remote peer closed the data link in an orderly way. */
    REMOTE_CLOSED,
    /** The remote peer aborted the SCTP association. */
    REMOTE_ABORTED,
    /** ICE connectivity failed or was lost. */
    ICE_FAILED,
    /** The DTLS or SCTP transport failed while ICE was still connected. */
    TRANSPORT_FAILED,
    /** The engine ended the peer without saying why. */
    UNKNOWN
}

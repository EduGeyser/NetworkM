package io.github.sendablemetatype.netty.channel.nethernet.signaling;

/**
 * Why the endpoint did not let a client join, with how each kind of source tells the client: an
 * HTTP status for HTTP signaling, and a NetherNet error code in a {@code CONNECTERROR} signal for
 * Xbox RPC, LAN and NXS. Never a success status: a 2xx would leave the client parsing an answer
 * that was never written.
 *
 * <p>The statuses follow the initial table of ruling E-7. The error codes are positions in the
 * vanilla client's signaling error enumeration, as go-nethernet lists it ({@code signal.go},
 * {@code ErrorCode*}); vanilla reads the payload of {@code CONNECTERROR} as a signed 32-bit
 * decimal number. Which message each status or code makes the client show is still to be
 * captured (real-client check 6).
 */
public enum JoinRefusal {
    /** The offer carried no identity assertion. */
    IDENTITY_MISSING(401, JoinRefusal.INCOMING_CONNECTION_IGNORED),
    /** The assertion was malformed, its proof failed, or it was not attested where that is required. */
    IDENTITY_INVALID(401, JoinRefusal.INCOMING_CONNECTION_IGNORED),
    /** The host turned the client away. */
    HOST_REFUSED(403, JoinRefusal.INCOMING_CONNECTION_IGNORED),
    /** No room: capacity, pending joins or the per-address throttle. */
    CAPACITY(503, JoinRefusal.INCOMING_CONNECTION_IGNORED),
    /** The identity could not be checked because its issuer's keys were unreachable. */
    TRUST_SOURCE_UNREACHABLE(500, JoinRefusal.GENERIC_FAILURE),
    /** No answer was ready in time. */
    NO_ANSWER_IN_TIME(504, JoinRefusal.NEGOTIATION_TIMEOUT),
    /**
     * The offer is unusable: empty, an SDP the identity check could not read, an unusable
     * {@code a=max-message-size}, or a description the engine refused. The client's input is at
     * fault, not the server.
     */
    BAD_OFFER(400, JoinRefusal.FAILED_TO_SET_REMOTE_DESCRIPTION),
    /** Something failed on the server while answering. */
    INTERNAL(500, JoinRefusal.FAILED_TO_CREATE_ANSWER);

    private static final int NEGOTIATION_TIMEOUT = 2;
    private static final int FAILED_TO_CREATE_ANSWER = 11;
    private static final int FAILED_TO_SET_REMOTE_DESCRIPTION = 13;
    private static final int INCOMING_CONNECTION_IGNORED = 16;
    private static final int GENERIC_FAILURE = 35;

    private final int httpStatus;
    private final int errorCode;

    JoinRefusal(int httpStatus, int errorCode) {
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
    }

    /** The HTTP status HTTP signaling answers with, 4xx or 5xx. */
    public int httpStatus() {
        return httpStatus;
    }

    /** The NetherNet error code sent in a {@code CONNECTERROR} signal. */
    public int errorCode() {
        return errorCode;
    }
}

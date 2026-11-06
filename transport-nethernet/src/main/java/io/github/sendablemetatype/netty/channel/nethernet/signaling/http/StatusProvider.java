package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

/**
 * Answers {@code GET /v1/join} status requests: the status document, the NetherNet equivalent of
 * RakNet's unconnected pong, or a decline.
 *
 * <p>Called for every request, off the I/O threads, with at most 64 calls running at once. An
 * answer that takes longer than the status deadline, a call that throws, and a request beyond
 * those 64 are all answered with the last document served for the same {@code Host}, or with an
 * empty {@code 200} when there is none, never with an error: a failed status request would cost
 * the client NetherNet. That fallback goes to whoever names the Host, so answer a document shaped
 * for one requester with {@link StatusAnswer#serveToRequester}, which it never repeats. The endpoint is open to anyone who can reach the port, so keep the work
 * per call small, for example by serving a cached document.
 */
@FunctionalInterface
public interface StatusProvider {

    /**
     * @param request the request
     * @return the document to serve, or a decline
     */
    StatusAnswer status(StatusRequest request);
}

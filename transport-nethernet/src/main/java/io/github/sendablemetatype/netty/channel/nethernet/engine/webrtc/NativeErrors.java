package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

/** How webrtc-java reports that a call failed. */
final class NativeErrors {

    private NativeErrors() {
    }

    /**
     * Whether a throwable is webrtc-java reporting a failed call, as opposed to
     * the JVM failing. Its native code throws runtime exceptions for what it
     * checks itself (a disposed object, a refused argument, an error libwebrtc
     * returned) and turns a C++ exception into a plain {@link Error}: an
     * unparsable description or candidate, a factory whose threads did not
     * start, a native reference left behind. Subclasses of {@code Error}, such
     * as {@link OutOfMemoryError} or {@link LinkageError}, are not a call's
     * failure.
     *
     * @param thrown what the call threw
     * @return true if the call failed and the JVM is fine
     */
    static boolean isCallFailure(Throwable thrown) {
        return thrown instanceof RuntimeException || thrown.getClass() == Error.class;
    }
}

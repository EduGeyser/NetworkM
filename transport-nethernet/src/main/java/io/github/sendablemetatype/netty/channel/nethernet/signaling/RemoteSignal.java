package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.util.Objects;

/** A signal a joining client sends after its offer. */
public sealed interface RemoteSignal {

    /**
     * A trickled candidate.
     *
     * @param candidate the candidate line as the client sent it
     */
    record Candidate(String candidate) implements RemoteSignal {

        public Candidate {
            Objects.requireNonNull(candidate, "candidate");
        }
    }

    /**
     * The client gave up on the join.
     *
     * @param code the NetherNet error code it sent
     */
    record Error(int code) implements RemoteSignal {
    }
}

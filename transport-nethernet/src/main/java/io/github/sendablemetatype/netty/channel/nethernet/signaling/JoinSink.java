package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;

import java.util.Optional;

/**
 * The endpoint side of a {@link JoinSource}. Each source gets a sink of its own, which carries the
 * admission policy the endpoint applies to that source's joins.
 */
public interface JoinSink {

    /**
     * Hands a join to the endpoint, which admits or refuses it and replies through
     * {@link Join#reply()}. Any thread; returns at once and never throws for a refused join.
     *
     * @param join the join
     */
    void join(Join join);

    /**
     * Whether a join from this address would pass the endpoint's capacity and throttle checks
     * now. A source that hears from a client before its offer (an HTTP status request) uses it to
     * decline early. A true answer is no promise: the join is checked again when it arrives.
     *
     * @param source where the join would come from
     * @return false if the join would certainly be refused
     */
    boolean admits(SourcedAddress source);

    /**
     * The endpoint's shared ICE port, for a source that admits joins from a connectivity check alone (NXS stateless
     * admission) or keeps STUN bindings from the port.
     *
     * @return the port, empty when each session takes ports of its own
     */
    default Optional<SharedIcePort> sharedIcePort() {
        return Optional.empty();
    }
}

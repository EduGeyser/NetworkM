package io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The state of one STUN binding kept alive from the gameplay port, as the endpoint's monitor reports it. The mapping
 * and its age are empty before the first answer; a later failed transaction keeps the last mapping.
 *
 * @param server              The STUN server, numeric
 * @param local               The local address the binding is kept from, or null when the engine does not say
 * @param mapped              The address the server saw in its latest answer, or null before the first one
 * @param successfulResponses The answers so far
 * @param failedTransactions  The transactions that got an error or no answer in time
 * @param mappingRevision     How often the mapped address changed, the first answer included
 * @param lastSuccessAge      How long ago the latest answer came
 */
public record StunReading(InetSocketAddress server, InetSocketAddress local, InetSocketAddress mapped, State state,
                          long successfulResponses, long failedTransactions, long mappingRevision,
                          Optional<Duration> lastSuccessAge) {
    /** The outcome of the binding's latest completed transaction. */
    public enum State {
        PENDING,
        SUCCEEDED,
        FAILED
    }

    public StunReading {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(lastSuccessAge, "lastSuccessAge");
        if (!concrete(server) || (mapped != null && !concrete(mapped)) || successfulResponses < 0
                || failedTransactions < 0 || mappingRevision < 0
                || lastSuccessAge.filter(Duration::isNegative).isPresent()) {
            throw new IllegalArgumentException("Invalid STUN reading");
        }
    }

    /** Whether the server has answered with a mapping. */
    public boolean answered() {
        return mapped != null && lastSuccessAge.isPresent() && successfulResponses > 0 && mappingRevision > 0;
    }

    private static boolean concrete(InetSocketAddress endpoint) {
        return endpoint != null && !endpoint.isUnresolved() && endpoint.getPort() >= 1;
    }
}

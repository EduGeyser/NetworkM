package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketId;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * What the endpoint observed about one admitted ticket. Never carries SDP, credentials or player identity.
 *
 * @param occurredAt Epoch milliseconds on this host's clock
 * @param reason     A short lowercase code, required for {@link OutcomeStage#FAILED}, otherwise optional
 * @param remote     The selected pair's remote endpoint at transport establishment, or null when unknown; reported
 *                   only to providers that advertise connectivity observation
 */
public record TransportOutcome(TicketId ticketId, OutcomeStage stage, long occurredAt, String reason,
                               InetSocketAddress remote) {
    /** The lowercase code an outcome reason is, as the wire reference describes it. */
    public static final Pattern REASON = Pattern.compile("[a-z0-9][a-z0-9_.-]{0,127}");

    public TransportOutcome {
        Objects.requireNonNull(ticketId, "ticketId");
        Objects.requireNonNull(stage, "stage");
        if (stage.game()) {
            throw new IllegalArgumentException("The game reports its own outcomes");
        }
        if (occurredAt < 0 || occurredAt > 9007199254740991L
                || stage == OutcomeStage.FAILED && reason == null
                || reason != null && !REASON.matcher(reason).matches()) {
            throw new IllegalArgumentException("Invalid transport outcome");
        }
        if (remote != null && (remote.isUnresolved() || remote.getPort() < 1)) {
            throw new IllegalArgumentException("The remote endpoint must be numeric with a port");
        }
    }
}

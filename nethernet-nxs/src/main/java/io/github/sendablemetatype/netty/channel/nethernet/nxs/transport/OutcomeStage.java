package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

/** The observation an outcome report carries about one admitted ticket. */
public enum OutcomeStage {
    /** An authenticated connectivity check arrived; optional. */
    ICE_SEEN("ticket.ice_seen"),
    /** Both data channels became usable. */
    DATA_CHANNELS_OPEN("ticket.data_channels_open"),
    /** An authenticated attempt failed before the transport became usable; carries a reason. */
    FAILED("ticket.failed"),
    /** The game admitted the player. */
    GAME_JOINED("ticket.game_joined"),
    /** The game rejected the player. */
    GAME_REJECTED("ticket.game_rejected");

    private final String wire;

    OutcomeStage(String wire) {
        this.wire = wire;
    }

    /** The name on the wire. */
    public String wire() {
        return wire;
    }

    /** Whether the game, not the transport, reports this stage. */
    public boolean game() {
        return this == GAME_JOINED || this == GAME_REJECTED;
    }
}

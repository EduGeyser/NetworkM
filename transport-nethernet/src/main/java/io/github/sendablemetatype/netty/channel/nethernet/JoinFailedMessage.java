/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.channel.nethernet;

/**
 * The warning for a join that never carried traffic, which is what a host sees when a player
 * cannot connect. It says how far the join got, so a host can tell a missing path from a filter
 * that drops part of the traffic. The player's address goes to a debug line instead, as hosts may
 * keep addresses out of their logs.
 */
final class JoinFailedMessage {

    /** How far a join got before it failed. */
    enum JoinProgress {
        /** ICE found no path. */
        NONE,
        /** This side's ICE checks came back, but the peer connection never came up. */
        ICE_CONNECTED,
        /** The peer connection came up, but the channel never went active. */
        PEER_CONNECTED
    }

    private JoinFailedMessage() {
    }

    /**
     * @param player   the player's name, null when the join carried none
     * @param progress how far the join got
     * @param icePort  the UDP port ICE uses, 0 when each connection takes its own
     * @param relayed  whether players only reach that port through PROXY protocol relays
     * @return the warning
     */
    static String of(String player, JoinProgress progress, int icePort, boolean relayed) {
        // A name from the offer's token is self-asserted unless attested, so it must not be able to
        // forge log lines
        String name = player == null ? "" : player.replaceAll("\\p{Cntrl}", "").strip();
        String who = name.isEmpty() ? "A player" : name.length() > 32 ? name.substring(0, 32) : name;
        return switch (progress) {
            case PEER_CONNECTED -> who + " reached this server over NetherNet, but the connection did not finish "
                    + "setting up.";
            // This side's checks came back, yet a client goes on only once its own checks do
            case ICE_CONNECTED -> who + " could not connect over NetherNet: their device answered this server, but "
                    + "the connection stalled after that. A firewall or a hosting provider's DDoS filter may be "
                    + "dropping other packets their device sends " + (relayed ? "through the PROXY protocol relays."
                    : icePort > 0 ? "to UDP port " + icePort + "." : "to this server over UDP.");
            case NONE -> relayed
                    ? who + " could not connect over NetherNet: nothing arrived through the PROXY protocol relays "
                    + "this server advertises. Each relay has to forward to UDP port " + icePort + " and be listed."
                    : who + " could not connect over NetherNet: no network path to this server was found. Their "
                    + "device needs to reach " + (icePort > 0 ? "UDP port " + icePort + " on one of the addresses "
                    + "this server" : "this server over UDP on one of the addresses it") + " advertises.";
        };
    }
}

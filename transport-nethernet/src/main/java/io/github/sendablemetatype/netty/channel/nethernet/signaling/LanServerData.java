package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.util.Objects;

/**
 * The server data a LAN discovery response carries, one record per binary layout. A server
 * advertises {@link V7} only; {@link V6} exists so a scanner can still read older servers.
 *
 * <p>This is the LAN record. The HTTP status document ({@code NetherNetServerStatus}) has its own
 * model; the two describe the same server but are not required to match.
 */
public sealed interface LanServerData permits LanServerData.V6, LanServerData.V7 {

    /** @return the layout version, the first byte on the wire */
    int dataVersion();

    /**
     * The layout BDS 1.26.50 and later send (BDS 1.26.51 and the 1.26.60.21 preview, see
     * {@code NetherNetServerDataCodec}). Components are in wire order.
     *
     * @param serverName     the server's display name
     * @param protocol       the Bedrock protocol version the server speaks
     * @param version        the Bedrock version string, such as {@code 1.26.60}
     * @param levelName      the world's display name
     * @param playerCount    the current number of players
     * @param maxPlayerCount the maximum number of players
     * @param gameType       {@code GameType}: 0 Survival, 1 Creative, 2 Adventure
     * @param editorWorld    whether the world is an Editor project
     * @param hardcore       whether the world is hardcore
     * @param onlineAuth     whether any Microsoft-authenticated player may join
     * @param selfSignedAuth whether self-signed identities may join
     * @param nonce          the endpoint's nonce, which clients echo in their Login data. BDS draws one
     *                       per process and writes it as unpadded lowercase hex; use one value per
     *                       endpoint, for example {@code NetherNetServerStatus.randomNonce()}
     * @param connectionType {@code Social::ConnectionType}; BDS sends 4 ({@code LANWebRTCSignaling})
     */
    record V7(String serverName, int protocol, String version, String levelName, int playerCount,
              int maxPlayerCount, int gameType, boolean editorWorld, boolean hardcore, boolean onlineAuth,
              boolean selfSignedAuth, String nonce, int connectionType) implements LanServerData {
        public V7 {
            Objects.requireNonNull(serverName, "serverName");
            Objects.requireNonNull(version, "version");
            Objects.requireNonNull(levelName, "levelName");
            Objects.requireNonNull(nonce, "nonce");
        }

        @Override
        public int dataVersion() {
            return 7;
        }
    }

    /**
     * The layout BDS 1.26.40 to 1.26.45 send. Read only: servers no longer advertise it.
     * Components are in wire order.
     *
     * @param transportLayer 2 for NetherNet; v7 dropped this field
     * @see V7 for the other components
     */
    record V6(String serverName, String levelName, int gameType, int playerCount, int maxPlayerCount,
              boolean editorWorld, boolean hardcore, boolean onlineAuth, boolean selfSignedAuth, String nonce,
              int transportLayer, int connectionType) implements LanServerData {
        public V6 {
            Objects.requireNonNull(serverName, "serverName");
            Objects.requireNonNull(levelName, "levelName");
            Objects.requireNonNull(nonce, "nonce");
        }

        @Override
        public int dataVersion() {
            return 6;
        }
    }
}

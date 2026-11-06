package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetConstants;
import io.github.sendablemetatype.netty.channel.nethernet.engine.CandidatePolicy;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import io.github.sendablemetatype.netty.channel.nethernet.engine.IceServer;
import io.github.sendablemetatype.netty.util.nethernet.ServerIdentity;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NetherChannelConfigOptionsTest {
    private static final Function<EmbeddedChannel, ChannelConfig> CONNECTION = DefaultNetherChannelConfig::new;
    private static final Function<EmbeddedChannel, ChannelConfig> CLIENT = DefaultNetherClientChannelConfig::new;
    private static final Function<EmbeddedChannel, ChannelConfig> SERVER = DefaultNetherServerChannelConfig::new;

    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    static Stream<Arguments> optionsAConfigDoesNotTake() {
        return Stream.of(
                arguments("client", CLIENT, NetherChannelOption.NETHER_SERVER_IDENTITY, ServerIdentity.ephemeral("test")),
                arguments("client", CLIENT, NetherChannelOption.NETHER_SERVER_MAX_SESSIONS, 10),
                // A child's peer is the server's, so its gathering options would have no effect.
                arguments("connection", CONNECTION, NetherChannelOption.NETHER_ICE_SERVERS, List.of()),
                arguments("connection", CONNECTION, NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 100),
                // The server channel carries no messages itself; its children take these.
                arguments("server", SERVER, NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, 1024),
                arguments("server", SERVER, NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() { }),
                arguments("server", SERVER, NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS, 2),
                arguments("client", CLIENT, ChannelOption.SO_KEEPALIVE, true),
                arguments("server", SERVER, ChannelOption.SO_BACKLOG, 128));
    }

    @ParameterizedTest(name = "{0} refuses {2}")
    @MethodSource("optionsAConfigDoesNotTake")
    <T> void anOptionTheConfigDoesNotTakeIsRefusedByName(String kind, Function<EmbeddedChannel, ChannelConfig> config,
                                                         ChannelOption<T> option, T value) {
        ChannelConfig target = config.apply(channel);
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> target.setOption(option, value));
        String name = option.name().substring(option.name().lastIndexOf('#') + 1);
        assertEquals("EmbeddedChannel does not take the channel option " + name, refused.getMessage());
        assertNull(target.getOption(option));
        assertFalse(target.getOptions().containsKey(option));
    }

    static Stream<Arguments> valuesOutOfRange() {
        return Stream.of(
                arguments(CONNECTION, NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, 0,
                        "NETHER_MAX_REASSEMBLED_BYTES must be at least 1, not 0"),
                arguments(CONNECTION, NetherChannelOption.NETHER_SEND_PAUSE_BYTES, -1,
                        "NETHER_SEND_PAUSE_BYTES must be from 1 to 16515072, not -1"),
                arguments(CLIENT, NetherChannelOption.NETHER_SEND_PAUSE_BYTES,
                        DefaultNetherChannelConfig.MAX_SEND_PAUSE_BYTES + 1,
                        "NETHER_SEND_PAUSE_BYTES must be from 1 to 16515072, not 16515073"),
                arguments(CLIENT, NetherChannelOption.NETHER_MAX_QUEUED_BYTES, 0,
                        "NETHER_MAX_QUEUED_BYTES must be at least 1, not 0"),
                arguments(CLIENT, NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 0,
                        "NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS must be at least 1, not 0"),
                arguments(SERVER, NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS, 0,
                        "NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS must be at least 1, not 0"),
                arguments(SERVER, NetherChannelOption.NETHER_SERVER_MAX_PENDING_JOINS, 0,
                        "NETHER_SERVER_MAX_PENDING_JOINS must be at least 1, not 0"),
                arguments(SERVER, NetherChannelOption.NETHER_SERVER_STUN_MAX_AGE, Duration.ZERO,
                        "NETHER_SERVER_STUN_MAX_AGE must be longer than zero, not PT0S"),
                arguments(SERVER, NetherChannelOption.NETHER_SERVER_STUN_SERVERS,
                        List.of(InetSocketAddress.createUnresolved("stun.example.test", 0)),
                        "NETHER_SERVER_STUN_SERVERS needs a port for every server, not stun.example.test/<unresolved>:0"),
                arguments(SERVER, NetherChannelOption.NETHER_SERVER_ADVERTISED_ADDRESSES,
                        IntStream.range(0, 33).mapToObj(port -> new InetSocketAddress("192.0.2.1", port)).toList(),
                        "NETHER_SERVER_ADVERTISED_ADDRESSES takes at most 32 entries, not 33"));
    }

    @ParameterizedTest(name = "{3}")
    @MethodSource("valuesOutOfRange")
    <T> void aValueOutOfRangeIsRefusedWithTheRangeAndNotStored(Function<EmbeddedChannel, ChannelConfig> config,
                                                               ChannelOption<T> option, T value, String message) {
        ChannelConfig target = config.apply(channel);
        T before = target.getOption(option);
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> target.setOption(option, value));
        assertEquals(message, refused.getMessage());
        assertEquals(before, target.getOption(option));
    }

    @Test
    void thePauseLevelGoesUpToWhereOneMoreSegmentStillFitsTheEnginesQueue() {
        DefaultNetherChannelConfig config = new DefaultNetherChannelConfig(channel);

        assertTrue(config.setOption(NetherChannelOption.NETHER_SEND_PAUSE_BYTES,
                DefaultNetherChannelConfig.MAX_SEND_PAUSE_BYTES));
        assertEquals(DataLink.SEND_QUEUE_CAPACITY,
                config.getSendPauseBytes() + NetherNetConstants.MAX_OUTBOUND_MESSAGE_SIZE);
    }

    @Test
    void onlyOptionsWithoutADefaultValueTakeNull() {
        DefaultNetherClientChannelConfig client = new DefaultNetherClientChannelConfig(channel);
        client.setOption(NetherChannelOption.NETHER_METRICS, new NetherChannelMetrics() { });
        assertTrue(client.setOption(NetherChannelOption.NETHER_METRICS, null));
        assertNull(client.getMetrics());
        assertTrue(client.setOption(NetherChannelOption.NETHER_CLIENT_IDENTITY, null));

        NullPointerException missing = assertThrows(NullPointerException.class,
                () -> client.setOption(NetherChannelOption.NETHER_CANDIDATE_POLICY, null));
        assertEquals("NETHER_CANDIDATE_POLICY must not be null", missing.getMessage());
        assertThrows(NullPointerException.class, () -> client.setOption(NetherChannelOption.NETHER_ICE_SERVERS,
                Collections.singletonList((IceServer) null)));
        assertEquals(CandidatePolicy.DEFAULT, client.getCandidatePolicy());
    }

    /** Every option a config lists keeps its default when set to it, so no default is out of its own range. */
    @ParameterizedTest
    @MethodSource("configs")
    void everyListedOptionTakesItsOwnValue(Function<EmbeddedChannel, ChannelConfig> config) {
        ChannelConfig target = config.apply(channel);
        for (Map.Entry<ChannelOption<?>, Object> entry : target.getOptions().entrySet()) {
            setUnchecked(target, entry.getKey(), entry.getValue());
        }
    }

    static Stream<Function<EmbeddedChannel, ChannelConfig>> configs() {
        return Stream.of(CONNECTION, CLIENT, SERVER);
    }

    @Test
    void eachConfigListsTheNetherNetOptionsThatActOnItsChannel() {
        assertEquals(List.of(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES, NetherChannelOption.NETHER_SEND_PAUSE_BYTES,
                        NetherChannelOption.NETHER_MAX_QUEUED_BYTES, NetherChannelOption.NETHER_METRICS),
                netherNetOptions(new DefaultNetherChannelConfig(channel)));
        List<ChannelOption<?>> client = netherNetOptions(new DefaultNetherClientChannelConfig(channel));
        assertTrue(client.containsAll(List.of(NetherChannelOption.NETHER_MAX_REASSEMBLED_BYTES,
                NetherChannelOption.NETHER_ICE_SERVERS, NetherChannelOption.NETHER_CLIENT_IDENTITY)), client.toString());
        assertTrue(client.stream().noneMatch(option -> option.name().contains("NETHER_SERVER_")), client.toString());
        List<ChannelOption<?>> server = netherNetOptions(new DefaultNetherServerChannelConfig(channel));
        assertTrue(server.containsAll(List.of(NetherChannelOption.NETHER_ICE_SERVERS,
                NetherChannelOption.NETHER_SERVER_IDENTITY)), server.toString());
        assertTrue(server.stream().noneMatch(option -> option.name().contains("NETHER_CLIENT_")
                || option == NetherChannelOption.NETHER_METRICS), server.toString());
    }

    @Test
    void nettysOwnOptionsStillApply() {
        DefaultNetherChannelConfig connection = new DefaultNetherChannelConfig(channel);
        assertTrue(connection.setOption(ChannelOption.AUTO_READ, false));
        assertFalse(connection.isAutoRead());
        DefaultNetherServerChannelConfig server = new DefaultNetherServerChannelConfig(channel);
        assertTrue(server.setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1234));
        assertEquals(1234, server.getConnectTimeoutMillis());
    }

    @Test
    void listsAreStoredAsImmutableCopiesAndStunServersOnce() {
        DefaultNetherServerChannelConfig server = new DefaultNetherServerChannelConfig(channel);
        InetSocketAddress stun = InetSocketAddress.createUnresolved("stun.example.test", 3478);
        List<InetSocketAddress> servers = new ArrayList<>(List.of(stun, stun));
        server.setOption(NetherChannelOption.NETHER_SERVER_STUN_SERVERS, servers);
        servers.clear();
        assertEquals(List.of(stun), server.getStunServers());
        assertThrows(UnsupportedOperationException.class, () -> server.getStunServers().add(stun));
    }

    private static List<ChannelOption<?>> netherNetOptions(ChannelConfig config) {
        return config.getOptions().keySet().stream()
                .filter(option -> option.name().startsWith(NetherChannelOption.class.getName() + "#"))
                .sorted((a, b) -> Integer.compare(a.id(), b.id()))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static <T> void setUnchecked(ChannelConfig config, ChannelOption<T> option, Object value) {
        assertTrue(config.setOption(option, (T) value), option.name());
    }
}

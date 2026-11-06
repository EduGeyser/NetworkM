package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.provider.ProviderRuntimeConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Putting NXS on a bound endpoint. */
@Timeout(15)
class NxsHostTest {
    private final HostFixture fixture;
    @TempDir
    Path directory;

    NxsHostTest() throws Exception {
        fixture = new HostFixture();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    private ProviderRuntimeConfiguration runtime(int udpPort) throws Exception {
        return ProviderRuntimeConfiguration.resolve(
                ProviderRuntimeConfiguration.Settings.builder("https://provider.example").build(), directory, udpPort,
                10);
    }

    @Test
    void addsNxsAsASourceThatLeavesWithItsTransport() throws Exception {
        NetherNetServerChannel server = (NetherNetServerChannel) fixture.bootstrap
                .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        int port = ((InetSocketAddress) server.localAddress()).getPort();

        NxsHost.Host host = NxsHost.open(server, runtime(port), fixture.identity).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        assertEquals(List.of(host.transport().source()), server.sources());

        host.transport().close().toCompletableFuture().get(5, TimeUnit.SECONDS);
        HostFixture.await(() -> server.sources().isEmpty(), "the source to leave the endpoint");
        server.close().sync();
    }

    @Test
    void refusesAnEndpointOnAnotherPortThanTheOneNxsPublishes() throws Exception {
        NetherNetServerChannel server = (NetherNetServerChannel) fixture.bootstrap
                .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        int port = ((InetSocketAddress) server.localAddress()).getPort();

        ExecutionException refused = assertThrows(ExecutionException.class, () -> NxsHost.open(server,
                runtime(port == 65535 ? port - 1 : port + 1), fixture.identity).toCompletableFuture().get());
        assertInstanceOf(IllegalArgumentException.class, refused.getCause());
        server.close().sync();
    }
}

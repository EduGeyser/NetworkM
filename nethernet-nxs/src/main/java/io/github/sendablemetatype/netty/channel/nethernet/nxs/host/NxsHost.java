package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.provider.ProviderRuntimeConfiguration;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Puts NXS on a bound NetherNet endpoint, as upstream's {@code NativeProviderHostFactory} does for its own server
 * channel. The consumer wires the rest:
 * <pre>{@code
 * NetherNetServerChannel endpoint = ...; // bound, with the shared ICE port on the NXS UDP port
 * HostIdentity identity = HostIdentity.load(certificateFile, privateKeyFile);
 * NxsHost.Host host = NxsHost.open(endpoint, runtime, identity).toCompletableFuture().join();
 * ProviderClient client = new ProviderClient(runtime.clientConfiguration(), store, host.transport(),
 *         status, health, diagnostics);
 * client.start();
 * // Every NXS child carries a login binding: decide it with TransportIdentityBinding.verifyLogin
 * // before the ticket's deadline, or the child is closed.
 * }</pre>
 * Closing the client closes the transport, which removes itself from the endpoint; players whose login was decided
 * stay connected.
 */
public final class NxsHost {

    /**
     * NXS on one endpoint.
     *
     * @param transport The provider client's transport
     * @param warnings  Settings that were left out rather than refused, for the host to log
     */
    public record Host(HostTransport transport, List<String> warnings) {
        public Host {
            Objects.requireNonNull(transport, "transport");
            warnings = List.copyOf(warnings);
        }
    }

    private NxsHost() {
    }

    /**
     * Adds NXS as a join source of the endpoint. Publication follows the runtime settings as upstream's factory
     * maps them: with STUN discovery or assisted joins the endpoint publishes configured or public local endpoints,
     * kept current with STUN mappings; behind PROXY protocol relays it publishes only the configured endpoints, which
     * must name the relays the endpoint's {@code NETHER_SERVER_TRUSTED_PROXIES} lists; otherwise its bound addresses
     * plus the configured endpoints.
     *
     * @param endpoint A bound endpoint whose shared ICE port is the runtime's UDP port
     * @param identity The DTLS identity every NXS peer presents
     * @return Completes once the endpoint started NXS, or exceptionally if it cannot
     */
    public static CompletionStage<Host> open(NetherNetServerChannel endpoint, ProviderRuntimeConfiguration runtime,
                                             HostIdentity identity) {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(identity, "identity");
        if (!endpoint.isActive() || !endpoint.isRegistered()) {
            return CompletableFuture.failedFuture(new IllegalStateException("The NetherNet endpoint is not bound"));
        }
        int icePort = ((InetSocketAddress) endpoint.localAddress()).getPort();
        if (icePort != runtime.udpPort()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("NXS publishes udp/"
                    + runtime.udpPort() + ", but the endpoint's shared ICE port is udp/" + icePort));
        }
        HostTransport.Publication publication = runtime.stunDiscovery() || runtime.assistedJoins()
                ? HostTransport.Publication.MAINTAINED
                : runtime.relayed() ? HostTransport.Publication.RELAYED : HostTransport.Publication.FIXED;
        HostTransport transport = new HostTransport(new HostTransport.Settings(publication,
                runtime.advertisedEndpoints(), runtime.localDevelopment(), runtime.assistedJoins(),
                runtime.stunDiscovery(), TicketLedger.Limits.defaults()), identity, endpoint.eventLoop());
        try {
            endpoint.addSource(transport.source());
        } catch (IllegalStateException closed) {
            transport.close();
            return CompletableFuture.failedFuture(closed);
        }
        transport.whenClosed().thenRun(() -> endpoint.removeSource(transport.source()));
        return transport.started().thenApply(ignored -> new Host(transport, runtime.warnings()));
    }
}

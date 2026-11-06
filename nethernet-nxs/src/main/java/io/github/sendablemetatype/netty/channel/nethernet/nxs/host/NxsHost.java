package io.github.sendablemetatype.netty.channel.nethernet.nxs.host;

import io.github.sendablemetatype.netty.channel.nethernet.NetherNetServerChannel;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.TicketLedger;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.provider.ProviderRuntimeConfiguration;
import io.github.sendablemetatype.netty.util.nethernet.DtlsCertificate;

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
 * // Created on the first start next to the server identity key and kept; DtlsCertificate.load takes a supplied one
 * DtlsCertificate certificate = DtlsCertificate.loadOrCreate(identityKeyFile.resolveSibling("dtls-certificate.pem"));
 * NxsHost.Host host = NxsHost.open(endpoint, runtime, certificate).toCompletableFuture().join();
 * ProviderClient client = new ProviderClient(runtime.clientConfiguration(), store, host.transport(),
 *         status, health, diagnostics);
 * client.start();
 * // Every NXS child carries a login binding: decide it with TransportIdentityBinding.verifyLogin
 * // before the ticket's deadline, or the child is closed.
 * }</pre>
 * Closing the client closes the transport, which removes itself from the endpoint; players whose login was accepted
 * stay connected, and every other NXS child is closed.
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
     * @param certificate The DTLS certificate every NXS peer presents, whose fingerprint the profile publishes. Keep
     *                    one per endpoint for good: clients that hold the published profile expect it.
     * @return Completes once the endpoint started NXS, or exceptionally if it cannot
     */
    public static CompletionStage<Host> open(NetherNetServerChannel endpoint, ProviderRuntimeConfiguration runtime,
                                             DtlsCertificate certificate) {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(certificate, "certificate");
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
                runtime.stunDiscovery(), TicketLedger.Limits.defaults()), certificate, endpoint.eventLoop());
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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.netty.channel.EventLoopGroup;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Map;

/**
 * The provider's WebSocket, when configured and advertised: creates the carrier from discovery, signs its upgrade,
 * and reconnects it on its own while assisted joins are on, since the provider can push joins only over a live
 * socket. Used on the lifecycle executor.
 */
final class ControlChannel {
    private final ClientRuntime runtime;
    private final RequestSigner signer;
    private final ProviderClient.Environment environment;
    private Assistance assistance;
    private EventLoopGroup group;
    private ProviderWebSocket socket;
    private URI endpoint;

    ControlChannel(ClientRuntime runtime, RequestSigner signer, ProviderClient.Environment environment) {
        this.runtime = runtime;
        this.signer = signer;
        this.environment = environment;
    }

    void assistance(Assistance assistance) {
        this.assistance = assistance;
    }

    /** The carrier, or null when operations go over HTTPS only. */
    ProviderWebSocket socket() {
        return socket;
    }

    /** Whether the provider's discovery offers a WebSocket this client uses. */
    boolean available() {
        return endpoint != null;
    }

    /**
     * Replaces the carrier, between operations, so the next upgrade carries the current authorization and the
     * assisted routing hint. None unless the configuration allows it and discovery advertises one.
     */
    void configure() throws IOException {
        close();
        endpoint = null;
        if (runtime.config.controlTransport() != ProviderClient.ControlTransport.AUTO
                || runtime.discovery == null || !runtime.discovery.advertises(Wire.WEBSOCKET)) {
            return;
        }
        try {
            URI control = URI.create(runtime.origin + "/v1/nxs/control");
            endpoint = URI.create((runtime.origin.startsWith("https:") ? "wss" : "ws") + "://"
                    + control.getRawAuthority() + control.getRawPath());
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid provider WebSocket capability", invalid);
        }
        if (group == null) {
            group = environment.eventLoops().get();
        }
        socket = new ProviderWebSocket(endpoint, group, environment.tls(), runtime.executor, runtime.clock,
                runtime::clockOffsetMillis, runtime.config.assistedJoins() ? assistance : null, this::reconnect);
    }

    /** Signs the upgrade: {@code GET} on the control path, carrying the routing hint while assisting. */
    Map<String, String> upgradeHeaders() {
        try {
            return signer.upgradeHeaders(endpoint, assistance != null && assistance.mode());
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException("Cannot sign the WebSocket upgrade", failure);
        }
    }

    /** Opens the socket now while assisted joins are on; operations alone would leave it closed until they run. */
    void reconnect() {
        ProviderState state = runtime.keeper.state();
        if (socket == null || assistance == null || !assistance.mode() || !runtime.started || runtime.closing
                || state.registration() == null || state.pendingOperation() != null) {
            return;
        }
        socket.connectNow(state.registration().instanceId() + ":" + state.generation(), this::upgradeHeaders);
    }

    void close() {
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

    /** Releases the event loop this client created. */
    void shutdown() {
        close();
        if (group != null) {
            group.shutdownGracefully();
            group = null;
        }
    }
}

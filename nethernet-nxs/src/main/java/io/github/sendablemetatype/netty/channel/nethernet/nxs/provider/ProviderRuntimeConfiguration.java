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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.provider;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderClient;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.ProviderOrigin;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.connectivity.ProviderEndpoint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.CandidateMethod;
import io.netty.util.NetUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validated NXS settings, with the listener and capacity inherited from whatever is hosting. Hosts name their
 * options after the {@link Settings} methods.
 */
public final class ProviderRuntimeConfiguration {
    private final URI origin;
    private final Path stateDirectory;
    private final String authorizationToken;
    private final String label;
    private final int udpPort;
    private final List<InetSocketAddress> advertisedEndpoints;
    private final List<String> warnings;
    private final int capacity;
    private final Settings settings;

    private ProviderRuntimeConfiguration(URI origin, Path stateDirectory, String authorizationToken, String label,
                                         int udpPort, List<InetSocketAddress> advertisedEndpoints,
                                         List<String> warnings, int capacity, Settings settings) {
        this.origin = origin;
        this.stateDirectory = stateDirectory;
        this.authorizationToken = authorizationToken;
        this.label = label;
        this.udpPort = udpPort;
        this.advertisedEndpoints = advertisedEndpoints;
        this.warnings = warnings;
        this.capacity = capacity;
        this.settings = settings;
    }

    /**
     * @param settings   What the host has configured for the provider
     * @param directory  The host's data directory, which the state directory and a token file are resolved against
     * @param udpPort    The fixed gameplay UDP port the provider hands out, which cannot be ephemeral
     * @param maxPlayers The routing capacity to register with
     * @throws IOException If a setting is invalid
     */
    public static ProviderRuntimeConfiguration resolve(Settings settings, Path directory, int udpPort, int maxPlayers)
            throws IOException {
        URI origin;
        try {
            origin = URI.create(ProviderOrigin.canonicalize(URI.create(settings.endpoint())));
        } catch (RuntimeException invalid) {
            throw new IOException("nxs.endpoint must be an HTTPS origin (HTTP is allowed only on loopback)");
        }
        String token = token(settings.token(), directory);
        String label = label(settings.label());
        if (udpPort < 1 || udpPort > 65535) {
            throw new IOException("NXS needs a fixed UDP port between 1 and 65535");
        }
        List<InetSocketAddress> endpoints = settings.advertiseAddresses().stream()
                .map(endpoint -> endpoint.getPort() == 0 && !endpoint.isUnresolved()
                        ? new InetSocketAddress(endpoint.getAddress(), udpPort)
                        : endpoint)
                .distinct()
                .toList();
        if (endpoints.size() > 32) {
            throw new IOException("nxs.advertise-addresses allows at most 32 endpoints");
        }
        for (InetSocketAddress endpoint : endpoints) {
            // Loopback is sorted out below, where it depends on where the provider runs
            if (!ProviderEndpoint.advertisable(endpoint, true)) {
                throw new IOException("nxs.advertise-addresses must name numeric addresses a peer can use");
            }
        }
        // The list may be shared with built-in signaling, where a client on this machine can use loopback. Players
        // get these from the provider, so loopback stays only where the endpoint policy takes it.
        List<String> warnings = List.of();
        if (!localDevelopment(origin) || stunDiscovery(settings) || assistedJoins(settings)) {
            List<InetSocketAddress> loopback = endpoints.stream()
                    .filter(endpoint -> endpoint.getAddress().isLoopbackAddress())
                    .toList();
            if (!loopback.isEmpty()) {
                endpoints = endpoints.stream().filter(endpoint -> !loopback.contains(endpoint)).toList();
                String skipped = loopback.stream().map(NetUtil::toSocketAddressString).collect(Collectors.joining(", "));
                warnings = List.of("Not announcing " + skipped
                        + " through the signaling service: players on other machines cannot reach a loopback address.");
            }
        }
        int capacity = Math.max(1, maxPlayers);
        if (capacity > 1000000) {
            throw new IOException("Invalid inherited routing capacity");
        }
        var runtime = new ProviderRuntimeConfiguration(origin, directory.resolve("provider-state"), token, label,
                udpPort, endpoints, warnings, capacity, settings);
        try {
            runtime.clientConfiguration();
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid nxs settings: " + invalid.getMessage());
        }
        return runtime;
    }

    /** What a host's configuration supplies, so this does not depend on how the host stores it. */
    public static final class Settings {
        private final String endpoint;
        private final String token;
        private final List<InetSocketAddress> advertiseAddresses;
        private final String label;
        private final boolean connectionChecks;
        private final boolean stunDiscovery;
        private final boolean assistedJoins;
        private final boolean deregisterOnShutdown;
        private final boolean gameOutcomes;
        private final ProviderClient.Location location;
        private final ProviderClient.BedrockProtocols bedrockProtocols;
        private final boolean relayed;

        private Settings(Builder builder) {
            endpoint = builder.endpoint;
            token = builder.token;
            advertiseAddresses = builder.advertiseAddresses;
            label = builder.label;
            connectionChecks = builder.connectionChecks;
            stunDiscovery = builder.stunDiscovery;
            assistedJoins = builder.assistedJoins;
            deregisterOnShutdown = builder.deregisterOnShutdown;
            gameOutcomes = builder.gameOutcomes;
            location = builder.location;
            bedrockProtocols = builder.bedrockProtocols;
            relayed = builder.relayed;
        }

        /** @param endpoint The provider origin */
        public static Builder builder(String endpoint) {
            return new Builder(endpoint);
        }

        /** The provider's origin as configured. */
        public String endpoint() {
            return endpoint;
        }

        /** The authorization token, or null for anonymous registration. */
        public String token() {
            return token;
        }

        /** The endpoints to advertise; port 0 stands for the gameplay port. */
        public List<InetSocketAddress> advertiseAddresses() {
            return advertiseAddresses;
        }

        /** The configured label, or null when none is configured. */
        public String label() {
            return label;
        }

        /** Whether the provider may check connectivity with diagnostic joins. */
        public boolean connectionChecks() {
            return connectionChecks;
        }

        /** Whether the endpoint discovers its public mapping with STUN. */
        public boolean stunDiscovery() {
            return stunDiscovery;
        }

        /** Whether the provider may push assisted joins. */
        public boolean assistedJoins() {
            return assistedJoins;
        }

        /** Whether stopping removes the registration. */
        public boolean deregisterOnShutdown() {
            return deregisterOnShutdown;
        }

        /** Whether the host reports game outcomes for admitted tickets. */
        public boolean gameOutcomes() {
            return gameOutcomes;
        }

        /** The declared location, or null for none. */
        public ProviderClient.Location location() {
            return location;
        }

        /** The declared protocols, or null for none. */
        public ProviderClient.BedrockProtocols bedrockProtocols() {
            return bedrockProtocols;
        }

        /** Whether players reach the host only through PROXY protocol relays. */
        public boolean relayed() {
            return relayed;
        }

        @Override
        public String toString() {
            return "Settings[endpoint=" + endpoint + "]";
        }

        /** Builds settings from what an operator configured. */
        public static final class Builder {
            private final String endpoint;
            private String token;
            private List<InetSocketAddress> advertiseAddresses = List.of();
            private String label;
            private boolean connectionChecks;
            private boolean stunDiscovery;
            private boolean assistedJoins;
            private boolean deregisterOnShutdown;
            private boolean gameOutcomes;
            private ProviderClient.Location location;
            private ProviderClient.BedrockProtocols bedrockProtocols;
            private boolean relayed;

            private Builder(String endpoint) {
                this.endpoint = endpoint;
            }

            /** @param token A bearer token, {@code file:} and the path of a file holding one, or empty to register anonymously */
            public Builder token(String token) {
                this.token = token;
                return this;
            }

            /**
             * @param advertiseAddresses Reachable endpoints, resolved by the host, at most 32; port 0 means the
             *                           gameplay port. Empty to derive them from the listener. Loopback ones are
             *                           left out with a warning unless a provider on this machine takes them.
             */
            public Builder advertiseAddresses(List<InetSocketAddress> advertiseAddresses) {
                this.advertiseAddresses = List.copyOf(advertiseAddresses);
                return this;
            }

            /**
             * @param label The instance's initial name at the provider, or empty to send none. A host name is never
             *              sent on its own: it may say more about an internal network than an operator means to.
             */
            public Builder label(String label) {
                this.label = label;
                return this;
            }

            /** @param connectionChecks Whether the provider may check reachability from its regions */
            public Builder connectionChecks(boolean connectionChecks) {
                this.connectionChecks = connectionChecks;
                return this;
            }

            /**
             * @param stunDiscovery Whether to find and maintain public addresses through the provider's STUN servers
             *                      when none are advertised
             */
            public Builder stunDiscovery(boolean stunDiscovery) {
                this.stunDiscovery = stunDiscovery;
                return this;
            }

            /**
             * @param assistedJoins Whether the provider may help players connect over its WebSocket, which this then
             *                      uses for every operation
             */
            public Builder assistedJoins(boolean assistedJoins) {
                this.assistedJoins = assistedJoins;
                return this;
            }

            /** @param deregisterOnShutdown For hosts whose state directory does not persist */
            public Builder deregisterOnShutdown(boolean deregisterOnShutdown) {
                this.deregisterOnShutdown = deregisterOnShutdown;
                return this;
            }

            /** @see ProviderClient.Configuration.Builder#gameOutcomes */
            public Builder gameOutcomes(boolean gameOutcomes) {
                this.gameOutcomes = gameOutcomes;
                return this;
            }

            /** @see ProviderClient.Configuration.Builder#location */
            public Builder location(ProviderClient.Location location) {
                this.location = location;
                return this;
            }

            /** @see ProviderClient.Configuration.Builder#bedrockProtocols */
            public Builder bedrockProtocols(ProviderClient.BedrockProtocols bedrockProtocols) {
                this.bedrockProtocols = bedrockProtocols;
                return this;
            }

            /**
             * @param relayed Whether players reach the gameplay port through PROXY protocol relays the endpoint is
             *                configured with. Then only the advertised addresses, which name the relays, are
             *                offered, and assisted joins and STUN discovery are off, since they contact clients
             *                directly and publish this host's own address.
             */
            public Builder relayed(boolean relayed) {
                this.relayed = relayed;
                return this;
            }

            /** The settings as built so far. */
            public Settings build() {
                return new Settings(this);
            }
        }
    }

    /** The provider's canonical origin. */
    public URI origin() {
        return origin;
    }

    /** Where the client keeps its state file. */
    public Path stateDirectory() {
        return stateDirectory;
    }

    /** The configured label, or null when none is configured. */
    public String label() {
        return label;
    }

    /** The gameplay UDP port handed out to players. */
    public int udpPort() {
        return udpPort;
    }

    /** The routing capacity registered with the provider. */
    public int capacity() {
        return capacity;
    }

    /** The configured endpoints, empty when the endpoint derives them. */
    public List<InetSocketAddress> advertisedEndpoints() {
        return advertisedEndpoints;
    }

    /** Settings that were left out rather than refused, for the host to log. */
    public List<String> warnings() {
        return warnings;
    }

    /** Whether the provider runs on this machine, which may also be handed loopback endpoints. */
    public boolean localDevelopment() {
        return localDevelopment(origin);
    }

    private static boolean localDevelopment(URI origin) {
        return Set.of("127.0.0.1", "localhost", "[::1]").contains(origin.getHost());
    }

    /** Whether the endpoint discovers and maintains its public address through STUN. */
    public boolean stunDiscovery() {
        return stunDiscovery(settings);
    }

    /** Whether the endpoint serves assisted joins. */
    public boolean assistedJoins() {
        return assistedJoins(settings);
    }

    /** Whether players reach the host only through PROXY protocol relays. */
    public boolean relayed() {
        return settings.relayed();
    }

    private static boolean stunDiscovery(Settings settings) {
        // Discovery publishes this host's own public address, around the relays
        return settings.stunDiscovery() && !settings.relayed();
    }

    private static boolean assistedJoins(Settings settings) {
        // Assistance contacts clients directly, around the relays
        return settings.assistedJoins() && !settings.relayed();
    }

    /** The client configuration these settings call for. */
    public ProviderClient.Configuration clientConfiguration() {
        return ProviderClient.Configuration.builder(origin)
                .label(label)
                .authorizationToken(authorizationToken)
                // Only assistance needs the provider's WebSocket
                .controlTransport(assistedJoins() ? ProviderClient.ControlTransport.AUTO
                        : ProviderClient.ControlTransport.HTTP)
                .diagnosticAdmission(settings.connectionChecks())
                .connectivityMethod(advertisedEndpoints.isEmpty() ? CandidateMethod.DISCOVERED : CandidateMethod.DEFINED)
                .assistedJoins(assistedJoins())
                .deregisterOnShutdown(settings.deregisterOnShutdown())
                .gameOutcomes(settings.gameOutcomes())
                .location(settings.location())
                .bedrockProtocols(settings.bedrockProtocols())
                .build();
    }

    private static String label(String configured) throws IOException {
        if (configured == null || configured.isBlank()) {
            return null;
        }
        String label = configured.strip();
        if (label.codePointCount(0, label.length()) > 128) {
            throw new IOException("The NXS label allows at most 128 characters");
        }
        return label;
    }

    private static String token(String value, Path directory) throws IOException {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            value = Objects.requireNonNull(SecretValue.resolve(value, directory)).trim();
        } catch (IOException unreadable) {
            throw new IOException("nxs.token file must be readable and contain at most 16384 bytes");
        }
        if (value.isBlank() || value.length() > 16384 || value.chars().anyMatch(c -> c <= 32 || c == 127)) {
            throw new IOException("nxs.token must contain one non-empty bearer token");
        }
        return value;
    }

    @Override
    public String toString() {
        return "ProviderRuntimeConfiguration[origin=" + origin + "]";
    }
}

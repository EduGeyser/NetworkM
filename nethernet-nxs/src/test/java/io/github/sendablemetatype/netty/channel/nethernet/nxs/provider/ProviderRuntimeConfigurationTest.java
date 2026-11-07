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
import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.CandidateMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/** Resolving what a host configured into what the provider client is given. */
class ProviderRuntimeConfigurationTest {
    private static final String PROVIDER = "https://signal.example.net";

    private static InetSocketAddress endpoint(String ip, int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName(ip), port);
        } catch (Exception invalid) {
            throw new AssertionError(invalid);
        }
    }

    private static ProviderRuntimeConfiguration.Settings settings(String endpoint, String token,
                                                                  List<InetSocketAddress> advertise, String label) {
        return ProviderRuntimeConfiguration.Settings.builder(endpoint)
                .token(token)
                .advertiseAddresses(advertise)
                .label(label)
                .build();
    }

    private static ProviderRuntimeConfiguration runtime(Path dir, ProviderRuntimeConfiguration.Settings settings)
            throws IOException {
        return ProviderRuntimeConfiguration.resolve(settings, dir, 20000, 40);
    }

    private static ProviderRuntimeConfiguration runtime(Path dir) throws IOException {
        return runtime(dir, settings(PROVIDER, "", List.of(), "Host"));
    }

    @Test
    void onlyAssistedJoinsUseTheWebSocket(@TempDir Path dir) throws Exception {
        var assisted = runtime(dir, ProviderRuntimeConfiguration.Settings.builder(PROVIDER)
                .stunDiscovery(true).assistedJoins(true).build()).clientConfiguration();
        assertTrue(assisted.assistedJoins());
        assertEquals(ProviderClient.ControlTransport.AUTO, assisted.controlTransport());
        var plain = runtime(dir).clientConfiguration();
        assertFalse(plain.assistedJoins());
        assertEquals(ProviderClient.ControlTransport.HTTP, plain.controlTransport());
    }

    @Test
    void passesTheBedrockProtocolsToTheClient(@TempDir Path dir) throws Exception {
        var protocols = new ProviderClient.BedrockProtocols(List.of(818, 2225), "26.60");
        assertEquals(protocols, runtime(dir, ProviderRuntimeConfiguration.Settings.builder(PROVIDER)
                .bedrockProtocols(protocols).build()).clientConfiguration().bedrockProtocols());
        assertNull(runtime(dir).clientConfiguration().bedrockProtocols());
    }

    @Test
    void relaysTurnAssistanceAndDiscoveryOff(@TempDir Path dir) throws Exception {
        // Assistance contacts clients directly and discovery publishes this host, both around the relays
        var runtime = runtime(dir, ProviderRuntimeConfiguration.Settings.builder(PROVIDER)
                .assistedJoins(true)
                .stunDiscovery(true)
                .advertiseAddresses(List.of(endpoint("8.8.8.8", 19165)))
                .relayed(true)
                .build());
        assertTrue(runtime.relayed());
        assertFalse(runtime.assistedJoins());
        assertFalse(runtime.clientConfiguration().assistedJoins());
        assertFalse(runtime.stunDiscovery());
    }

    @Test
    void inheritsTheListenerAndCapacityFromTheHost(@TempDir Path dir) throws Exception {
        ProviderRuntimeConfiguration result = runtime(dir);
        assertEquals(PROVIDER, result.origin().toString());
        assertEquals(20000, result.udpPort());
        assertEquals(40, result.capacity());
        assertEquals("Host", result.label());
        assertEquals(dir.resolve("provider-state"), result.stateDirectory());
        assertNull(result.clientConfiguration().authorizationToken());
    }

    @Test
    void connectionChecksAndMethodComeFromOrdinaryLocalSettings(@TempDir Path dir) throws Exception {
        assertFalse(runtime(dir).clientConfiguration().diagnosticAdmission());
        for (var endpoints : List.of(List.<InetSocketAddress>of(), List.of(endpoint("1.1.1.1", 19132)))) {
            var configured = runtime(dir, ProviderRuntimeConfiguration.Settings.builder(PROVIDER)
                    .advertiseAddresses(endpoints).connectionChecks(true).build()).clientConfiguration();
            assertTrue(configured.diagnosticAdmission());
            assertEquals(endpoints.isEmpty() ? CandidateMethod.DISCOVERED : CandidateMethod.DEFINED,
                    configured.connectivityMethod());
        }
    }

    @Test
    void readsTheSettingsWithoutLeakingTheToken(@TempDir Path dir) throws Exception {
        ProviderRuntimeConfiguration result = runtime(dir, settings(PROVIDER, "configured-secret",
                List.of(endpoint("1.1.1.1", 29133), endpoint("2606:4700:4700::1111", 39133),
                        endpoint("1.1.1.1", 29133)), "lobby-0"));
        assertEquals("configured-secret", result.clientConfiguration().authorizationToken());
        assertEquals("lobby-0", result.clientConfiguration().label());
        assertEquals(2, result.advertisedEndpoints().size(), "the duplicate endpoint collapses");
        assertFalse(result.toString().contains("configured-secret"));
        assertFalse(result.clientConfiguration().toString().contains("configured-secret"));
    }

    @Test
    void sendsNoLabelUnlessOneIsConfigured(@TempDir Path dir) throws Exception {
        assertEquals("lobby-0", runtime(dir, settings(PROVIDER, "", List.of(), " lobby-0 ")).label());
        // A host name can say more about an internal network than an operator means to share
        for (String unset : new String[]{null, " "}) {
            assertNull(runtime(dir, settings(PROVIDER, "", List.of(), unset)).label());
            assertNull(runtime(dir, settings(PROVIDER, "", List.of(), unset)).clientConfiguration().label());
        }
        assertThrows(IOException.class, () -> runtime(dir, settings(PROVIDER, "", List.of(), "x".repeat(129))));
    }

    @Test
    void readsTokenFilesAndNeverLeaksTheToken(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("token"), "file-secret\n");
        for (String reference : List.of("file:token", "file:" + dir.resolve("token"), "./token",
                dir.resolve("token").toString())) {
            assertEquals("file-secret", runtime(dir, settings(PROVIDER, reference, List.of(), null))
                    .clientConfiguration().authorizationToken(), reference);
        }
        // Any other value is the token itself, even with a file of that name beside the configuration
        assertEquals("token", runtime(dir, settings(PROVIDER, "token", List.of(), null))
                .clientConfiguration().authorizationToken());
        // A value that starts with '/' names a file, as upstream reads it; with nothing there it is never shown
        IOException path = assertThrows(IOException.class, () -> runtime(dir, settings(PROVIDER,
                "/token-that-starts-with-a-slash", List.of(), null)));
        assertTrue(path.getMessage().startsWith("nxs.token looks like a file path"), path.getMessage());
        assertFalse(path.toString().contains("token-that-starts"), path.toString());
        for (String value : List.of("file:missing", "file:", "bad secret")) {
            IOException failure = assertThrows(IOException.class,
                    () -> runtime(dir, settings(PROVIDER, value, List.of(), null)));
            assertFalse(failure.toString().contains("bad secret"));
        }
        // A file holding only a line terminator carries no token
        Files.writeString(dir.resolve("token"), "\n");
        assertThrows(IOException.class, () -> runtime(dir, settings(PROVIDER, "file:token", List.of(), null)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://signal.example.net", "signal.example.net", "https://signal.example.net/path",
            "https://user@signal.example.net", "https://xn--bcher-kva.example", "https://[2001:DB8:0:0::1]",
            "https://010.0.0.1"})
    void refusesAnInsecureOrMalformedProvider(String endpoint, @TempDir Path dir) {
        assertThrows(IOException.class, () -> runtime(dir, settings(endpoint, "", List.of(), null)));
    }

    @Test
    void allowsPlainHttpOnLoopbackForLocalDevelopment(@TempDir Path dir) throws Exception {
        assertEquals("http://127.0.0.1:8080",
                runtime(dir, settings("http://127.0.0.1:8080", "", List.of(), null)).origin().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"::", "224.0.0.1", "fe80::1", "203.0.113.1", "2001:db8::1", "255.255.255.255",
            "198.18.0.1"})
    void refusesAnAdvertisedEndpointAPeerCouldNotUse(String address, @TempDir Path dir) {
        assertThrows(IOException.class,
                () -> runtime(dir, settings(PROVIDER, "", List.of(endpoint(address, 19133)), null)));
    }

    @Test
    void takesTheUdpPortWhenAnEntryNamesNone(@TempDir Path dir) throws Exception {
        var endpoints = runtime(dir, settings("http://127.0.0.1:8080", "",
                List.of(endpoint("1.1.1.1", 0), endpoint("127.0.0.1", 29133)), null)).advertisedEndpoints();
        assertEquals(List.of(endpoint("1.1.1.1", 20000), endpoint("127.0.0.1", 29133)), endpoints);
    }

    @Test
    void leavesOutLoopbackThatPlayersCouldNotReach(@TempDir Path dir) throws Exception {
        var mixed = runtime(dir, settings(PROVIDER, "",
                List.of(endpoint("127.0.0.1", 0), endpoint("1.1.1.1", 0), endpoint("::1", 29133)), null));
        assertEquals(List.of(endpoint("1.1.1.1", 20000)), mixed.advertisedEndpoints());
        assertEquals(1, mixed.warnings().size());
        assertTrue(mixed.warnings().getFirst().contains("127.0.0.1:20000, [::1]:29133"), mixed.warnings().toString());
        assertEquals(CandidateMethod.DEFINED, mixed.clientConfiguration().connectivityMethod());

        // With nothing left the endpoint derives its own, as with an empty list
        var none = runtime(dir, settings(PROVIDER, "", List.of(endpoint("127.0.0.1", 0)), null));
        assertTrue(none.advertisedEndpoints().isEmpty());
        assertEquals(CandidateMethod.DISCOVERED, none.clientConfiguration().connectivityMethod());

        // A provider on this machine takes loopback, unless STUN or assistance pick the maintained policy
        var local = runtime(dir, settings("http://127.0.0.1:8080", "", List.of(endpoint("127.0.0.1", 0)), null));
        assertEquals(1, local.advertisedEndpoints().size());
        assertTrue(local.warnings().isEmpty());
        var localStun = runtime(dir, ProviderRuntimeConfiguration.Settings.builder("http://127.0.0.1:8080")
                .advertiseAddresses(List.of(endpoint("127.0.0.1", 0)))
                .stunDiscovery(true)
                .build());
        assertTrue(localStun.advertisedEndpoints().isEmpty());
        assertEquals(1, localStun.warnings().size());
    }

    @Test
    void needsAFixedUdpPort(@TempDir Path dir) throws Exception {
        var settings = settings(PROVIDER, "", List.of(), null);
        // 0 would be ephemeral, and the provider hands the port out
        assertThrows(IOException.class, () -> ProviderRuntimeConfiguration.resolve(settings, dir, 0, 20));
        assertEquals(65535, ProviderRuntimeConfiguration.resolve(settings, dir, 65535, 20).udpPort());
    }

    @Test
    void refusesMoreThan32AdvertisedEndpoints(@TempDir Path dir) {
        var endpoints = IntStream.rangeClosed(1, 33).mapToObj(i -> endpoint("8.8.8.8", 20000 + i)).toList();
        assertThrows(IOException.class, () -> runtime(dir, settings(PROVIDER, "", endpoints, null)));
    }
}

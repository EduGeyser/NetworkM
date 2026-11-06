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

package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What a client says about itself in the query of a status request. Retail 1.26.62 sends it on
 * the server list poll ({@code ?version=1.26.62&protocol=2225&platform=8&id=...}) and nothing on
 * the status request right before a join (captured 2026-11-05). The client reports it unchecked,
 * so it is a hint, nothing to trust.
 *
 * @param version  the game version, such as {@code 1.26.62}
 * @param protocol the network protocol version
 * @param platform the build platform, such as {@code 8} for Windows
 */
public record ClientInfo(String version, int protocol, int platform) {

    /** Room for {@code 1.100.100.100} and then some. */
    public static final int MAX_VERSION_LENGTH = 16;

    public ClientInfo {
        Objects.requireNonNull(version, "version");
    }

    /**
     * Reads it from a decoded query, all or nothing: empty when any of the three is missing or
     * malformed. A version longer than {@link #MAX_VERSION_LENGTH} or other than digits and dots
     * is malformed.
     *
     * @param query the query's parameters
     * @return what the client said, or empty
     */
    public static Optional<ClientInfo> fromQuery(Map<String, List<String>> query) {
        String version = parameter(query, "version");
        String protocol = parameter(query, "protocol");
        String platform = parameter(query, "platform");
        if (version == null || protocol == null || platform == null || !isVersion(version)) {
            return Optional.empty();
        }
        try {
            return Optional.of(new ClientInfo(version, Integer.parseInt(protocol), Integer.parseInt(platform)));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static String parameter(Map<String, List<String>> query, String name) {
        List<String> values = query.get(name);
        return values == null || values.isEmpty() ? null : values.getFirst();
    }

    private static boolean isVersion(String version) {
        if (version.isEmpty() || version.length() > MAX_VERSION_LENGTH) {
            return false;
        }
        for (int i = 0; i < version.length(); i++) {
            char c = version.charAt(i);
            if (c != '.' && (c < '0' || c > '9')) {
                return false;
            }
        }
        return true;
    }
}

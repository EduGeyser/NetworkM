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

package io.github.sendablemetatype.netty.util.address;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Reads {@code host}, {@code host:port} and {@code [IPv6]:port} as operators write them in
 * configuration, without looking anything up.
 */
public final class HostPort {
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9.-]{1,255}");
    private static final Pattern BRACKETED = Pattern.compile("[0-9a-fA-F:.]{1,255}");
    private static final Pattern PORT = Pattern.compile("[0-9]{1,5}");

    private HostPort() {
    }

    /**
     * @param value       The text to read
     * @param defaultPort The port when the text names none
     * @return The host and port, unresolved, or empty for anything else, such as userinfo, a path,
     *         a port outside 1 to 65535 or an unbracketed IPv6 address
     */
    public static Optional<InetSocketAddress> parse(String value, int defaultPort) {
        String host = value;
        String port = null;
        boolean bracketed = value.startsWith("[");
        if (bracketed) {
            int close = value.indexOf(']');
            if (close < 0 || (close + 1 < value.length() && value.charAt(close + 1) != ':')) {
                return Optional.empty();
            }
            host = value.substring(1, close);
            port = close + 1 < value.length() ? value.substring(close + 2) : null;
        } else {
            int colon = value.indexOf(':');
            if (colon >= 0) {
                host = value.substring(0, colon);
                port = value.substring(colon + 1);
            }
        }
        if (!(bracketed ? BRACKETED : NAME).matcher(host).matches()) {
            return Optional.empty();
        }
        if (port == null) {
            return Optional.of(InetSocketAddress.createUnresolved(host, defaultPort));
        }
        if (!PORT.matcher(port).matches()) {
            return Optional.empty();
        }
        int number = Integer.parseInt(port);
        return number < 1 || number > 65535 ? Optional.empty()
                : Optional.of(InetSocketAddress.createUnresolved(host, number));
    }
}

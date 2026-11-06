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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * One regional connection check the provider reported in a heartbeat reply. It is feedback, never a command: it
 * can only withhold failed public endpoints from later player offers.
 *
 * @param region    The provider's region name, 1 to 32 of {@code [A-Za-z0-9_-]}
 * @param method    How the checked candidate was found; {@link CandidateMethod#PER_JOIN} for an assisted check
 * @param target    The exact endpoint checked, or null when an assisted check failed without one
 * @param checkedAt Epoch milliseconds on the provider's clock
 * @param expiresAt Epoch milliseconds, at most five minutes after {@code checkedAt}
 */
public record ConnectivityCheck(String region, AddressFamily family, CandidateMethod method, InetSocketAddress target,
                                ConnectivityOutcome outcome, long checkedAt, long expiresAt) {
    public ConnectivityCheck {
        Objects.requireNonNull(family, "family");
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(outcome, "outcome");
        if (region == null || !region.matches("[A-Za-z0-9_-]{1,32}")) {
            throw new IllegalArgumentException("Invalid connectivity region");
        }
        if (target != null && (target.isUnresolved() || target.getPort() < 1
                || AddressFamily.of(target.getAddress()) != family)) {
            throw new IllegalArgumentException("Invalid connectivity target");
        }
        if (checkedAt < 0 || expiresAt > 9007199254740991L || expiresAt <= checkedAt
                || expiresAt - checkedAt > 300000) {
            throw new IllegalArgumentException("Invalid connectivity check");
        }
    }
}

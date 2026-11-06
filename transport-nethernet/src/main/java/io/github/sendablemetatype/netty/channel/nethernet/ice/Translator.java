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

package io.github.sendablemetatype.netty.channel.nethernet.ice;

import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath.CandidateType;
import io.netty.util.NetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Announces endpoints this host did not gather as server reflexive candidates, each based on a
 * UDP host candidate of the same family, on that host's port when the endpoint has none.
 * <p>
 * That works because libwebrtc accepts an authenticated check from any source on any of its ports
 * and never looks at the address it was sent to ({@code p2p/base/port.cc}), so a NAT that
 * forwards the announced endpoint to the host's port carries the connection. Each endpoint and
 * port is announced once per answer, whatever the number of interfaces sharing the socket.
 * Holds the per-answer state, so one instance serves one answer or one trickled session.
 */
final class Translator {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(Translator.class);

    /**
     * RFC 8445 section 5.1.2.1 with the server reflexive type preference, below any host. Each
     * further translation takes the next lower local preference, which must be unique per type.
     */
    private static final long PRIORITY = (100L << 24) | (65535L << 8) | 255;
    private static final int FOUNDATION = 80000000;

    private final boolean hideBase;
    private final Set<String> emitted = new HashSet<>();
    private int count;

    /**
     * @param hideBase Whether the related address is announced as the unspecified address and
     *                 port 0, so that players do not learn this host's own addresses
     */
    Translator(boolean hideBase) {
        this.hideBase = hideBase;
    }

    /** Notes a UDP endpoint the answer already names, so that it is not announced again. */
    void present(CandidateLine candidate) {
        if (candidate.isUdp()) {
            this.emitted.add(candidate.endpointKey());
        }
    }

    /**
     * @param hosts     The UDP host candidates to base translations on, preferred first
     * @param endpoints The endpoints to announce
     * @return One server reflexive candidate per endpoint and port not yet announced or present
     */
    List<CandidateLine> translate(List<CandidateLine> hosts, List<InetSocketAddress> endpoints) {
        List<CandidateLine> candidates = new ArrayList<>();
        for (InetSocketAddress endpoint : endpoints) {
            InetAddress target = endpoint.getAddress();
            String address = NetUtil.toAddressString(target);
            for (CandidateLine host : hosts) {
                InetAddress base = host.ipAddress().orElse(null);
                if (host.type() != CandidateType.HOST || !host.isUdp() || base == null
                        || base.getAddress().length != target.getAddress().length) {
                    continue;
                }
                int port = endpoint.getPort() == 0 ? host.port() : endpoint.getPort();
                if (!this.emitted.add(address + " " + port)) {
                    continue;
                }
                log.debug("Announcing {} {} as a translation of {} {}", address, port, host.address(), host.port());
                long priority = PRIORITY - ((long) this.count << 8);
                String related = this.hideBase ? (base.getAddress().length == 4 ? "0.0.0.0" : "::") + " rport 0"
                        : host.address() + " rport " + host.port();
                String line = "candidate:" + (FOUNDATION + this.count) + " 1 " + host.transport() + " " + priority
                        + " " + address + " " + port + " typ srflx raddr " + related;
                this.count++;
                candidates.add(CandidateLine.parse(line).orElseThrow());
            }
        }
        return candidates;
    }

    /** Whether a candidate is the endpoint: same address, and same UDP port unless the endpoint has port 0. */
    static boolean names(InetSocketAddress endpoint, CandidateLine candidate) {
        return candidate.ipAddress().map(address -> address.equals(endpoint.getAddress())).orElse(false)
                && (endpoint.getPort() == 0 || (candidate.isUdp() && candidate.port() == endpoint.getPort()));
    }

    static boolean namedByAny(List<InetSocketAddress> endpoints, CandidateLine candidate) {
        for (InetSocketAddress endpoint : endpoints) {
            if (names(endpoint, candidate)) {
                return true;
            }
        }
        return false;
    }
}

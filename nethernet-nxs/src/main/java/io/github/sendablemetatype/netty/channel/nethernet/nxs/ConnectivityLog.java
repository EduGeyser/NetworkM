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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.*;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Operator-facing reachability observations; never changes serving or admission. Used on the lifecycle executor. */
final class ConnectivityLog {
    private record Key(String region, AddressFamily family, CandidateMethod method) {
    }

    private record Observation(long revision, ConnectivityCheck check) {
    }

    private final Consumer<ProviderDiagnostic> sink;
    private final boolean assisted;
    private final boolean checks;
    private final LongSupplier millis;
    private final Map<Key, Observation> reported = new HashMap<>();
    private final Map<AddressFamily, String> publications = new EnumMap<>(AddressFamily.class);
    private final Map<AddressFamily, Boolean> available = new EnumMap<>(AddressFamily.class);
    private long revision;
    private boolean announced;

    ConnectivityLog(Consumer<ProviderDiagnostic> sink, boolean assisted, boolean checks, LongSupplier millis) {
        this.sink = sink;
        this.assisted = assisted;
        this.checks = checks;
        this.millis = millis;
    }

    /** Logs which families players can reach, when that changes. */
    void published(HostProfileSnapshot snapshot) {
        if (!announced) {
            announced = true;
            debug("Assisted joins " + (assisted ? "on" : "off") + ", connection checks " + (checks ? "on" : "off") + ".");
            if (!checks) {
                warn("Connection checks are turned off, so server reachability cannot be checked.");
            }
        }
        List<HostCandidate> candidates = snapshot.profile().candidates();
        List<HostCandidate> probes = snapshot.probeCandidates();
        for (AddressFamily family : AddressFamily.values()) {
            List<String> offered = endpoints(candidates, family);
            boolean assist = assisted && snapshot.assistedFamilies().contains(family);
            String decision = "offered=" + offered + ", test targets=" + endpoints(probes, family)
                    + ", assisted=" + assist;
            if (decision.equals(publications.put(family, decision))) {
                continue;
            }
            boolean usable = !offered.isEmpty() || assist;
            Boolean wasUsable = available.put(family, usable);
            if (!Objects.equals(wasUsable, usable)) {
                if (usable) {
                    info(name(family) + " is available for player connections.");
                } else if (Boolean.TRUE.equals(wasUsable) || !endpoints(probes, family).isEmpty()) {
                    warn(name(family) + " has no available player address."
                            + (checks ? " Connection checks will continue." : ""));
                }
            }
            debug(name(family) + " addresses: " + decision + ".");
        }
    }

    /** Logs new check results once; an unchanged result repeats only at debug. */
    void checks(HostProfileSnapshot snapshot, List<ConnectivityCheck> results) {
        if (snapshot.candidateRevision() < revision) {
            return;
        }
        revision = snapshot.candidateRevision();
        published(snapshot);
        List<HostCandidate> candidates = snapshot.profile().candidates();
        long now = millis.getAsLong();
        for (ConnectivityCheck check : results.stream()
                .sorted(Comparator.comparingLong(ConnectivityCheck::checkedAt)).toList()) {
            if (check.checkedAt() > now || check.expiresAt() <= now) {
                continue;
            }
            Key key = new Key(check.region(), check.family(), check.method());
            Observation observation = reported.get(key);
            ConnectivityCheck previous = observation == null ? null : observation.check();
            if (observation != null && observation.revision() == revision && previous.checkedAt() >= check.checkedAt()) {
                continue;
            }
            reported.put(key, new Observation(revision, check));
            String message = name(check.family()) + " " + method(check.method()) + " connection check from "
                    + check.region() + " " + result(check.outcome()) + ".";
            // An assisted check reaches a different temporary peer each time; that alone is not a new result
            boolean unchanged = previous != null && previous.outcome() == check.outcome()
                    && (check.method() == CandidateMethod.PER_JOIN || Objects.equals(previous.target(), check.target()));
            if (unchanged) {
                debug(message);
            } else if (check.outcome() == ConnectivityOutcome.ESTABLISHED) {
                info(message);
            } else if (check.outcome() == ConnectivityOutcome.NOT_ESTABLISHED) {
                boolean assist = assisted && snapshot.assistedFamilies().contains(check.family());
                boolean offered = check.target() != null
                        && endpoints(candidates, check.family()).contains(endpoint(check.target()));
                String impact = assist ? " Some players may still be able to connect."
                        : check.target() == null ? " Player addresses are unchanged."
                        : offered ? " Other regions can still reach this address."
                        : " This address is temporarily unavailable to players.";
                warn(message + impact + " Check " + port(check, candidates) + " in your firewall.");
            } else {
                warn(message + " Player addresses are unchanged. Checks will retry automatically.");
            }
            debug("Check details: region=" + check.region() + ", family=" + name(check.family()) + ", method="
                    + check.method().wire() + ", target=" + (check.target() == null ? "unknown" : endpoint(check.target()))
                    + ", result=" + check.outcome() + ", checkedAt=" + Instant.ofEpochMilli(check.checkedAt()) + ".");
        }
    }

    private static String name(AddressFamily family) {
        return "IPv" + family.number();
    }

    private static String result(ConnectivityOutcome outcome) {
        return switch (outcome) {
            case ESTABLISHED -> "passed";
            case NOT_ESTABLISHED -> "failed";
            case UNKNOWN -> "was inconclusive";
            case UNAVAILABLE -> "could not run";
        };
    }

    private static String method(CandidateMethod method) {
        return switch (method) {
            case PER_JOIN -> "assisted";
            case WARM_STUN -> "automatically discovered";
            case DEFINED, DISCOVERED -> "direct";
        };
    }

    private static String port(ConnectivityCheck check, List<HostCandidate> candidates) {
        if (check.target() != null) {
            return "UDP port " + check.target().getPort();
        }
        return candidates.isEmpty() ? "the server's UDP port" : "UDP port " + candidates.getFirst().endpoint().getPort();
    }

    private static List<String> endpoints(List<HostCandidate> candidates, AddressFamily family) {
        return candidates.stream().filter(c -> c.family() == family).map(c -> endpoint(c.endpoint())).sorted().toList();
    }

    /** One spelling for candidates and check targets, so they compare. */
    private static String endpoint(InetSocketAddress target) {
        String address = target.getAddress().getHostAddress();
        return (target.getAddress() instanceof Inet6Address ? "[" + address + "]" : address) + ":" + target.getPort();
    }

    private void debug(String message) {
        sink.accept(new ProviderDiagnostic(ProviderDiagnostic.Level.DEBUG, message));
    }

    private void info(String message) {
        sink.accept(new ProviderDiagnostic(ProviderDiagnostic.Level.INFO, message));
    }

    private void warn(String message) {
        sink.accept(new ProviderDiagnostic(ProviderDiagnostic.Level.WARN, message));
    }
}

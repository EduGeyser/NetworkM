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
import io.github.sendablemetatype.netty.util.address.EndpointAddress;
import io.netty.util.NetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Decides which of the remote side's candidates reach the engine for one connection attempt,
 * and remembers which of them are the peer's TURN relays.
 * <p>
 * A server applies only IP-literal UDP candidates, at most {@link #MAX_CANDIDATES} from the
 * remote description and as many again from trickling. A name would make the server look it up
 * for whoever sent the offer, and the caps bound how many addresses a peer can make the server
 * check. NetherNet gathers no TCP, so TCP candidates only use up the cap. Behind PROXY protocol
 * relays the server applies none: players reach it only through their relay, and it must never
 * contact them directly. Real clients sent only IPv4 UDP host candidates (2026-11-05 captures).
 * <p>
 * Server and client alike drop candidates at a multicast, limited broadcast or unspecified
 * address: no peer can answer a check from there, and a check sent there reaches whatever
 * listens on the sender's network rather than the peer. Loopback and link-local stay, for peers
 * on the same host or link.
 * <p>
 * A client also takes UDP candidates that name a host (see {@link #client}), with the same caps
 * and TCP dropped: the server decides which addresses the client checks, so each client attempt
 * runs the answer and every trickled candidate through an intake of its own. The engine never
 * sees a name. The intake looks each one up off the calling thread, at most
 * {@value #MAX_LOOKUPS} per attempt, runs the address it resolves to through the same address
 * rules, and hands the candidate on rewritten to that IP literal, later and on its own. A name
 * that does not resolve is dropped. Numeric spellings that are no IP literal here, such as
 * {@code 3758096635} or {@code 0xe00000fb}, count as names, so the rules see whatever the resolver
 * makes of them: the JDK reads the first as 224.0.0.251 itself and refuses the second, which
 * glibc would also read as 224.0.0.251. Either way the candidate is dropped.
 * <p>
 * Trickled candidates carry no m-section, so they are tagged with the remote description's first
 * m-section and its {@code a=mid}: libwebrtc refuses a candidate whose mid matches no section. A
 * server hands {@link #remoteDescription} the offer before any trickled candidate; a client's
 * intake starts from its own offer. Safe to call from any thread.
 */
public final class RemoteCandidateIntake {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(RemoteCandidateIntake.class);

    /** Taken from the description, and again from trickling: far more than a peer gathers. */
    public static final int MAX_CANDIDATES = 32;
    /**
     * How many ports are guessed at for one peer. A peer gathers one port per interface, so a
     * handful covers any real client, and the peer must not decide how many packets leave here.
     */
    static final int MAX_INFERRED_CANDIDATES = 8;
    /**
     * RFC 8445 section 5.1.2.1 with the server reflexive type preference and local preference 0,
     * below anything the peer itself signals.
     */
    private static final long INFERRED_PRIORITY = (long) CandidateType.SERVER_REFLEXIVE.typePreference() << 24 | 255;
    private static final byte[] LIMITED_BROADCAST = {(byte) 255, (byte) 255, (byte) 255, (byte) 255};
    /** How many names a client attempt looks up at most, over its answer and trickling together. */
    static final int MAX_LOOKUPS = MAX_CANDIDATES;
    /**
     * A name as SDP allows one in a candidate (RFC 8839 section 5.1, an FQDN), which also covers
     * mDNS names and numeric spellings. Anything else, such as an empty token, is not looked up.
     */
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.-]{1,253}");

    private enum Mode {
        /** IP literals only, capped. */
        SERVER,
        /** Nothing applied, behind PROXY protocol relays. */
        RELAYED,
        /** IP literals, and names once looked up, capped. */
        CLIENT
    }

    /**
     * Looks up the name a remote candidate gives instead of an IP literal. A client's intake asks
     * it for every name it takes.
     */
    @FunctionalInterface
    public interface Resolver {
        /** The system resolver ({@link InetAddress#getByName}), on a virtual thread per name. */
        Resolver SYSTEM = Resolver::lookUp;

        /**
         * @param name The name, never an IP literal
         * @return Completes with the address to check, or exceptionally when the name does not
         *         resolve; on any thread
         */
        CompletionStage<InetAddress> resolve(String name);

        private static CompletionStage<InetAddress> lookUp(String name) {
            CompletableFuture<InetAddress> address = new CompletableFuture<>();
            Thread.ofVirtual().name("nethernet-candidate-lookup").start(() -> {
                try {
                    address.complete(InetAddress.getByName(name));
                } catch (UnknownHostException | RuntimeException e) {
                    address.completeExceptionally(e);
                }
            });
            return address;
        }
    }

    /** A name a client takes: its candidate, and the m-section it was tagged with. */
    private record Lookup(CandidateLine candidate, String mid, int mLineIndex) {
    }

    /** What filtering gave: the result for the engine, and the names to look up once unlocked. */
    private record Filtered<T>(T result, List<Lookup> lookups) {
    }

    private final Mode mode;
    private final Resolver resolver;
    private final Consumer<RemoteCandidate> lookedUp;
    private String mid = "";
    private int mLineIndex;
    private int trickled;
    private int lookups;
    private final Set<InetSocketAddress> relays = new LinkedHashSet<>();
    /** The UDP ports the peer's host candidates use, for {@link #inferredCandidates}. */
    private final Set<Integer> hostPorts = new LinkedHashSet<>();
    /** Whether the peer signaled anything but host candidates, so it needs no guessing. */
    private boolean reachableWithoutGuessing;

    private RemoteCandidateIntake(Mode mode, Resolver resolver, Consumer<RemoteCandidate> lookedUp) {
        this.mode = mode;
        this.resolver = resolver;
        this.lookedUp = lookedUp;
    }

    /**
     * @return The intake for a server, which applies IP-literal UDP candidates only
     */
    public static RemoteCandidateIntake server() {
        return new RemoteCandidateIntake(Mode.SERVER, null, null);
    }

    /**
     * @return The intake for a server behind PROXY protocol relays, which applies none, so it only
     *         answers the checks a peer sends through its relay
     */
    public static RemoteCandidateIntake relayed() {
        return new RemoteCandidateIntake(Mode.RELAYED, null, null);
    }

    /**
     * The intake for one client attempt, which also takes names: a server may name its host by a
     * dynamic or split-horizon DNS name, or by a cluster name inside its own network. Each name is
     * looked up, and the candidate at the address it resolves to goes to {@code lookedUp} if the
     * address rules take that address. Lookups are never withdrawn, so the receiver drops what
     * comes for an attempt that ended.
     * <p>
     * A server may trickle candidates ahead of its answer. An answer mirrors the offer's
     * m-sections (RFC 9429 section 5.3.1), so until the answer is here, trickled candidates take
     * the offer's first m-section.
     *
     * @param offer    The attempt's own offer
     * @param resolver Looks names up, usually {@link Resolver#SYSTEM}
     * @param lookedUp Receives each candidate a name stood for, rewritten to its IP literal, on the
     *                 resolver's thread; must not block
     * @return The intake for the attempt
     */
    public static RemoteCandidateIntake client(String offer, Resolver resolver, Consumer<RemoteCandidate> lookedUp) {
        RemoteCandidateIntake intake = new RemoteCandidateIntake(Mode.CLIENT, Objects.requireNonNull(resolver, "resolver"),
                Objects.requireNonNull(lookedUp, "lookedUp"));
        intake.noteFirstSection(SdpText.lines(offer));
        return intake;
    }

    /**
     * The remote description to hand the engine: blank lines removed, and only the IP-literal
     * candidates this intake takes, in their order. Also notes the description's m-section for
     * trickled candidates. A client's intake starts looking up the names it takes; their
     * candidates follow on their own, after this returns.
     *
     * @param sdp The remote description as received
     * @return The description for the engine
     */
    public String remoteDescription(String sdp) {
        Filtered<String> filtered;
        synchronized (this) {
            filtered = filterDescription(sdp);
        }
        filtered.lookups().forEach(this::lookUp);
        return filtered.result();
    }

    private Filtered<String> filterDescription(String sdp) {
        List<String> lines = SdpText.lines(sdp);
        noteFirstSection(lines);
        List<String> out = new ArrayList<>(lines.size());
        List<Lookup> named = new ArrayList<>();
        int kept = 0;
        for (String line : lines) {
            if (line.startsWith("a=candidate:")) {
                Optional<CandidateLine> candidate = CandidateLine.parse(line);
                if (candidate.isEmpty()) {
                    log.debug("Dropping an unreadable remote candidate: {}", line);
                    continue;
                }
                if (unusable(candidate.get())) {
                    continue;
                }
                if (isName(candidate.get())) {
                    if (kept < MAX_CANDIDATES && lookupFor(candidate.get(), named)) {
                        kept++;
                    }
                    continue;
                }
                note(candidate.get());
                if (!takes(candidate.get()) || kept >= MAX_CANDIDATES) {
                    continue;
                }
                kept++;
                out.add(candidate.get().withCanonicalAddress().attribute());
                continue;
            }
            out.add(line);
        }
        return new Filtered<>(SdpText.join(out), named);
    }

    /**
     * Notes the first m-section and its {@code a=mid} for trickled candidates. A section without
     * a mid keeps the one noted before.
     */
    private void noteFirstSection(List<String> lines) {
        int section = -1;
        for (String line : lines) {
            if (line.startsWith("m=")) {
                if (++section > 0) {
                    return;
                }
                this.mLineIndex = section;
            } else if (section == 0 && line.startsWith("a=mid:")) {
                this.mid = line.substring("a=mid:".length()).strip();
            }
        }
    }

    /**
     * Decides on a candidate the remote side trickled. A name a client's intake takes is looked
     * up, and its candidate follows on its own, after this returns.
     *
     * @param candidate The candidate as signaled, {@code candidate:...} or {@code a=candidate:...}
     * @return The candidate for the engine, or empty when it is not taken now
     */
    public Optional<RemoteCandidate> trickled(String candidate) {
        Filtered<Optional<RemoteCandidate>> filtered;
        synchronized (this) {
            filtered = filterTrickled(candidate);
        }
        filtered.lookups().forEach(this::lookUp);
        return filtered.result();
    }

    private Filtered<Optional<RemoteCandidate>> filterTrickled(String candidate) {
        Optional<CandidateLine> line = CandidateLine.parse(candidate);
        if (line.isEmpty()) {
            log.debug("Ignoring an unreadable remote candidate: {}", candidate);
            return new Filtered<>(Optional.empty(), List.of());
        }
        if (unusable(line.get())) {
            return new Filtered<>(Optional.empty(), List.of());
        }
        if (isName(line.get())) {
            List<Lookup> named = new ArrayList<>(1);
            if (this.trickled < MAX_CANDIDATES && lookupFor(line.get(), named)) {
                this.trickled++;
            }
            return new Filtered<>(Optional.empty(), named);
        }
        note(line.get());
        if (!takes(line.get()) || this.trickled >= MAX_CANDIDATES) {
            log.debug("Ignoring remote candidate {}", candidate);
            return new Filtered<>(Optional.empty(), List.of());
        }
        this.trickled++;
        return new Filtered<>(Optional.of(new RemoteCandidate(line.get().withCanonicalAddress().sdp(), this.mid,
                this.mLineIndex)), List.of());
    }

    /** Whether a client's intake looks this candidate's address up: a UDP candidate naming a host. */
    private boolean isName(CandidateLine candidate) {
        return this.mode == Mode.CLIENT && candidate.isUdp() && candidate.ipAddress().isEmpty();
    }

    /**
     * Adds the name's lookup, tagged with the m-section noted now, if the address is a name and
     * the attempt has lookups left. Holds the lock.
     *
     * @return Whether the name is looked up
     */
    private boolean lookupFor(CandidateLine candidate, List<Lookup> named) {
        if (!NAME.matcher(candidate.address()).matches()) {
            log.debug("Dropping a remote candidate on port {} whose address is neither an IP literal nor a name",
                    candidate.port());
            return false;
        }
        if (this.lookups >= MAX_LOOKUPS) {
            log.debug("Dropping a named remote candidate on port {}: this attempt looked up {} names already",
                    candidate.port(), MAX_LOOKUPS);
            return false;
        }
        this.lookups++;
        named.add(new Lookup(candidate, this.mid, this.mLineIndex));
        return true;
    }

    /**
     * Looks a name up, never holding the lock, and hands its candidate on at the address it
     * resolved to, if the address rules take that address.
     */
    private void lookUp(Lookup lookup) {
        CompletionStage<InetAddress> resolving;
        try {
            resolving = this.resolver.resolve(lookup.candidate().address());
        } catch (RuntimeException e) {
            resolving = CompletableFuture.failedFuture(e);
        }
        resolving.whenComplete((address, failure) -> {
            if (failure != null || address == null) {
                log.debug("Dropping a named remote candidate on port {}: the name does not resolve ({})",
                        lookup.candidate().port(), failure == null ? "no address" : failure.getClass().getSimpleName());
                return;
            }
            CandidateLine resolved = lookup.candidate().withAddress(address);
            if (unusable(resolved)) {
                return;
            }
            synchronized (this) {
                note(resolved);
            }
            this.lookedUp.accept(new RemoteCandidate(resolved.sdp(), lookup.mid(), lookup.mLineIndex()));
        });
    }

    /**
     * Whether the remote side named this endpoint as one of its TURN relays, in its description or
     * by trickling, whether or not the candidate was applied. A selected pair through a PROXY
     * protocol relay is only ever peer reflexive, so this is how its TURN relay shows.
     *
     * @param endpoint The remote side of a selected pair
     * @return Whether it is one of the peer's relays
     */
    public synchronized boolean isPeerRelay(InetSocketAddress endpoint) {
        return this.relays.contains(endpoint);
    }

    /**
     * Candidates at the address the peer signaled from, one per UDP port its host candidates use,
     * at most {@value #MAX_INFERRED_CANDIDATES}, for a server to add to the engine once it has
     * gathered.
     * <p>
     * A peer that signals only host candidates, as retail HTTP clients do, offers nothing a host on
     * another network can reach, and its own checks die on the first NAT they meet. Its public
     * address is known anyway, because it just signaled from it, and consumer NATs usually keep
     * the port a socket already uses. Checking there costs a few packets, and if it maps that way
     * the check opens the path in both directions.
     * <p>
     * Nothing is inferred by a client, behind PROXY protocol relays, when this host gathered a
     * public host candidate of its own, when the peer signaled a reflexive or relayed candidate,
     * or when the signaled address is not public (a placeholder for an unknown address never is).
     *
     * @param localDescription This side's description once gathering is complete
     * @param signaledFrom     Where the peer signaled from
     * @return The candidates to add, empty when there is nothing to infer
     */
    public synchronized List<RemoteCandidate> inferredCandidates(String localDescription,
                                                                 InetSocketAddress signaledFrom) {
        InetAddress from = signaledFrom.getAddress();
        if (this.mode != Mode.SERVER || this.reachableWithoutGuessing || from == null
                || EndpointAddress.scope(from) != EndpointAddress.Scope.PUBLIC
                || hasPublicHostCandidate(localDescription)) {
            return List.of();
        }
        String address = NetUtil.toAddressString(from);
        String unspecified = from instanceof Inet4Address ? "0.0.0.0" : "::";
        List<RemoteCandidate> candidates = new ArrayList<>(this.hostPorts.size());
        int foundation = 90000000;
        for (int port : this.hostPorts) {
            candidates.add(new RemoteCandidate("candidate:" + foundation++ + " 1 udp " + INFERRED_PRIORITY + " "
                    + address + " " + port + " typ srflx raddr " + unspecified + " rport 0",
                    this.mid, this.mLineIndex));
        }
        return candidates;
    }

    /**
     * Whether a description holds a host candidate at a public address. A host that gathered one
     * is reachable as the protocol expects and needs no guessing.
     */
    static boolean hasPublicHostCandidate(String sdp) {
        for (String line : SdpText.lines(sdp)) {
            Optional<CandidateLine> candidate = line.startsWith("a=candidate:") ? CandidateLine.parse(line)
                    : Optional.empty();
            if (candidate.isPresent() && candidate.get().type() == CandidateType.HOST
                    && candidate.get().ipAddress()
                    .map(address -> EndpointAddress.scope(address) == EndpointAddress.Scope.PUBLIC)
                    .orElse(false)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a candidate's address can never be the peer: multicast ({@code 224.0.0.0/4},
     * {@code ff00::/8}), limited broadcast or unspecified. Its IPv4-mapped form counts as the IPv4
     * address, as {@link CandidateLine#ipAddress} reads it. A dropped candidate is not noted
     * either, so it neither counts as a relay nor shapes inferred candidates.
     */
    private static boolean unusable(CandidateLine candidate) {
        Optional<InetAddress> address = candidate.ipAddress();
        if (address.isEmpty()) {
            return false;
        }
        InetAddress ip = address.get();
        boolean unusable = ip.isMulticastAddress() || ip.isAnyLocalAddress()
                || (ip instanceof Inet4Address && Arrays.equals(ip.getAddress(), LIMITED_BROADCAST));
        if (unusable) {
            log.debug("Dropping a remote candidate at an unusable address: {}", candidate);
        }
        return unusable;
    }

    /** Whether this intake applies an IP-literal candidate; a client's names go through {@link #isName}. */
    private boolean takes(CandidateLine candidate) {
        return switch (this.mode) {
            case RELAYED -> false;
            case SERVER, CLIENT -> candidate.isUdp() && candidate.ipAddress().isPresent();
        };
    }

    private void note(CandidateLine candidate) {
        if (candidate.type() != CandidateType.HOST) {
            this.reachableWithoutGuessing = true;
        } else if (candidate.isUdp() && candidate.port() != 0 && this.hostPorts.size() < MAX_INFERRED_CANDIDATES) {
            this.hostPorts.add(candidate.port());
        }
        if (candidate.type() == CandidateType.RELAY && this.relays.size() < MAX_CANDIDATES) {
            candidate.endpoint().ifPresent(this.relays::add);
        }
    }
}

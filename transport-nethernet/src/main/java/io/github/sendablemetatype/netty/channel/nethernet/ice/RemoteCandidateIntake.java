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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
 * Trickled candidates carry no m-section, so they are tagged with the remote description's first
 * m-section and its {@code a=mid}: libwebrtc refuses a candidate whose mid matches no section.
 * Hand {@link #remoteDescription} the description before any trickled candidate. Safe to call
 * from any thread.
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
    private static final long INFERRED_PRIORITY = 100L << 24 | 255;
    private static final byte[] LIMITED_BROADCAST = {(byte) 255, (byte) 255, (byte) 255, (byte) 255};

    private enum Mode {
        /** IP literals only, capped. */
        SERVER,
        /** Nothing applied, behind PROXY protocol relays. */
        RELAYED,
        /** Names allowed, capped. */
        CLIENT
    }

    private final Mode mode;
    private String mid = "";
    private int mLineIndex;
    private int trickled;
    private final Set<InetSocketAddress> relays = new LinkedHashSet<>();
    /** The UDP ports the peer's host candidates use, for {@link #inferredCandidates}. */
    private final Set<Integer> hostPorts = new LinkedHashSet<>();
    /** Whether the peer signaled anything but host candidates, so it needs no guessing. */
    private boolean reachableWithoutGuessing;

    private RemoteCandidateIntake(Mode mode) {
        this.mode = mode;
    }

    /**
     * @return The intake for a server, which applies IP-literal UDP candidates only
     */
    public static RemoteCandidateIntake server() {
        return new RemoteCandidateIntake(Mode.SERVER);
    }

    /**
     * @return The intake for a server behind PROXY protocol relays, which applies none, so it only
     *         answers the checks a peer sends through its relay
     */
    public static RemoteCandidateIntake relayed() {
        return new RemoteCandidateIntake(Mode.RELAYED);
    }

    /**
     * @return The intake for a client, which also applies names: a server may name a host inside
     *         its own network, such as a cluster DNS name, for the engine to resolve
     */
    public static RemoteCandidateIntake client() {
        return new RemoteCandidateIntake(Mode.CLIENT);
    }

    /**
     * The remote description to hand the engine: blank lines removed, and only the candidates
     * this intake takes. Also notes the description's m-section for trickled candidates.
     *
     * @param sdp The remote description as received
     * @return The description for the engine
     */
    public synchronized String remoteDescription(String sdp) {
        List<String> lines = SdpText.lines(sdp);
        List<String> out = new ArrayList<>(lines.size());
        boolean sectionFound = false;
        boolean inFirstSection = false;
        int section = -1;
        int kept = 0;
        for (String line : lines) {
            if (line.startsWith("m=")) {
                section++;
                inFirstSection = !sectionFound;
                if (!sectionFound) {
                    sectionFound = true;
                    this.mLineIndex = section;
                }
            } else if (inFirstSection && line.startsWith("a=mid:")) {
                this.mid = line.substring("a=mid:".length()).strip();
            }
            if (line.startsWith("a=candidate:")) {
                Optional<CandidateLine> candidate = CandidateLine.parse(line);
                if (candidate.isEmpty()) {
                    log.debug("Dropping an unreadable remote candidate: {}", line);
                    continue;
                }
                if (unusable(candidate.get())) {
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
        return SdpText.join(out);
    }

    /**
     * Decides on a candidate the remote side trickled.
     *
     * @param candidate The candidate as signaled, {@code candidate:...} or {@code a=candidate:...}
     * @return The candidate for the engine, or empty when it is not taken
     */
    public synchronized Optional<RemoteCandidate> trickled(String candidate) {
        Optional<CandidateLine> line = CandidateLine.parse(candidate);
        if (line.isEmpty()) {
            log.debug("Ignoring an unreadable remote candidate: {}", candidate);
            return Optional.empty();
        }
        if (unusable(line.get())) {
            return Optional.empty();
        }
        note(line.get());
        if (!takes(line.get()) || this.trickled >= MAX_CANDIDATES) {
            log.debug("Ignoring remote candidate {}", candidate);
            return Optional.empty();
        }
        this.trickled++;
        return Optional.of(new RemoteCandidate(line.get().withCanonicalAddress().sdp(), this.mid, this.mLineIndex));
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

    private boolean takes(CandidateLine candidate) {
        return switch (this.mode) {
            case RELAYED -> false;
            case SERVER -> candidate.isUdp() && candidate.ipAddress().isPresent();
            case CLIENT -> candidate.isUdp();
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

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

import io.netty.util.NetUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Applies a {@link CandidateAnnouncement} to this side's candidates: to a whole answer that
 * carries them ({@link #fullAnswer}, HTTP), or to candidates as ICE gathers them
 * ({@link #trickled}, Xbox and LAN).
 */
public final class AnswerCandidates {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(AnswerCandidates.class);

    private static final String CANDIDATE = "a=candidate:";

    private AnswerCandidates() {
    }

    /**
     * Rewrites an answer that carries its candidates. Candidates the announcement adds join the
     * end of the candidate block, ahead of any {@code a=end-of-candidates}; append that line after
     * this rewrite, not before. Then each m-section's {@code m=} port and {@code c=} address are
     * set from the candidates it now announces, so that a relayed or narrowed answer names only
     * what it announces. Blank lines are removed.
     * <p>
     * NetherNet descriptions carry one bundled application section, so every candidate belongs to
     * the first m-section; announced candidates join that section.
     *
     * @param sdp          The answer as the engine produced it, after gathering
     * @param announcement What to announce
     * @return The answer to send
     */
    public static String fullAnswer(String sdp, CandidateAnnouncement announcement) {
        List<String> lines = SdpText.lines(sdp);
        List<CandidateLine> gathered = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith(CANDIDATE)) {
                CandidateLine.parse(line).ifPresent(gathered::add);
            }
        }
        List<CandidateLine> udpHosts = gathered.stream()
                .filter(candidate -> candidate.type() == CandidateType.HOST && candidate.isUdp())
                .toList();

        Rewrite rewrite = switch (announcement) {
            case CandidateAnnouncement.Added added -> {
                Translator translator = new Translator(false);
                gathered.forEach(translator::present);
                yield new Rewrite(Optional::of, translator.translate(udpHosts, added.endpoints()));
            }
            case CandidateAnnouncement.Advertised advertised ->
                    advertised(gathered, udpHosts, advertised.endpoints(), new Translator(false));
            case CandidateAnnouncement.Relayed relayed -> {
                List<CandidateLine> translated = new Translator(true).translate(udpHosts, relayed.relays());
                if (translated.isEmpty()) {
                    log.warn("None of the relays {} can be announced from the gathered ICE candidates", relayed.relays());
                }
                yield new Rewrite(AnswerCandidates::relayedKeeps, translated);
            }
        };

        List<String> out = new ArrayList<>(lines.size() + rewrite.added().size());
        List<CandidateLine> added = rewrite.added();
        boolean seenCandidates = false;
        for (String line : lines) {
            if (line.startsWith(CANDIDATE)) {
                seenCandidates = true;
                Optional<CandidateLine> candidate = CandidateLine.parse(line).flatMap(rewrite.kept()::apply);
                candidate.ifPresent(kept -> out.add(kept.attribute()));
                continue;
            }
            if (seenCandidates && !added.isEmpty()) {
                added.forEach(candidate -> out.add(candidate.attribute()));
                added = List.of();
            }
            out.add(line);
        }
        added.forEach(candidate -> out.add(candidate.attribute()));
        return SdpText.join(withConnectionAddresses(out));
    }

    /**
     * @param announcement What to announce
     * @return The filter for one session's trickled candidates
     */
    public static TrickledAnswerCandidates trickled(CandidateAnnouncement announcement) {
        return new TrickledAnswerCandidates(announcement);
    }

    /** What happens to each gathered candidate, and what joins them. */
    record Rewrite(Function<CandidateLine, Optional<CandidateLine>> kept,
                   List<CandidateLine> added) {
    }

    /**
     * Narrows the host candidates to the listed endpoints this host gathered, and translates the
     * others. Nothing can tell a forward from a mistake, so a list naming none of the gathered
     * endpoints narrows nothing, and a wrong endpoint costs the peer a failed check rather than the
     * connection. Reflexive and relayed candidates always stay, since they are what the outside
     * sees rather than an interface.
     */
    static Rewrite advertised(List<CandidateLine> gathered, List<CandidateLine> udpHosts,
                              List<InetSocketAddress> endpoints, Translator translator) {
        List<InetSocketAddress> held = new ArrayList<>();
        List<InetSocketAddress> foreign = new ArrayList<>();
        for (InetSocketAddress endpoint : endpoints) {
            boolean named = gathered.stream().anyMatch(candidate -> Translator.names(endpoint, candidate));
            (named ? held : foreign).add(endpoint);
        }
        // A held host is the more honest base for a translation, so it goes first for its family
        List<CandidateLine> hosts = udpHosts.stream()
                .sorted(Comparator.comparing(host -> !Translator.namedByAny(held, host)))
                .toList();
        List<CandidateLine> translated = translator.translate(hosts, foreign);
        if (held.isEmpty() && translated.isEmpty()) {
            log.warn("None of the gathered ICE candidates match the advertised addresses {}, "
                    + "announcing all of them instead", endpoints);
            return new Rewrite(Optional::of, List.of());
        }
        return new Rewrite(candidate -> held.isEmpty() || candidate.type() != CandidateType.HOST
                || Translator.namedByAny(held, candidate) ? Optional.of(candidate) : Optional.empty(), translated);
    }

    /**
     * Behind PROXY protocol relays only TURN relay candidates stay, without their related address,
     * which for a relay candidate is this host's public mapping. TURN gives a host behind a tunnel
     * relay a second path, since TURN uses sockets of its own and not the relayed port.
     */
    static Optional<CandidateLine> relayedKeeps(CandidateLine candidate) {
        return candidate.type() == CandidateType.RELAY ? Optional.of(candidate.withRelatedAddressHidden())
                : Optional.empty();
    }

    /**
     * Sets each m-section's {@code m=} port and {@code c=} address from the candidates the
     * description now announces, as {@link #fullAnswer} does after its rewrite, for a caller that
     * replaced the candidates itself: the engine set both from what it gathered, so they would
     * keep naming a candidate the description no longer announces.
     *
     * @param sdp The description
     * @return The description, each line ended by CRLF and blank lines removed
     */
    public static String withConnectionAddresses(String sdp) {
        return SdpText.join(withConnectionAddresses(SdpText.lines(sdp)));
    }

    /**
     * Sets each m-section's {@code m=} port and {@code c=} address from its candidates as
     * libwebrtc does ({@code UpdateConnectionAddress} in {@code api/jsep.cc}): component 1 UDP
     * candidates only, relayed over reflexive over host within a family, the first of equals,
     * IPv4 over IPv6; {@code 0.0.0.0} and port 9 when none qualifies. A rejected section (port 0)
     * keeps its port.
     */
    static List<String> withConnectionAddresses(List<String> lines) {
        List<String> out = new ArrayList<>(lines);
        int start = -1;
        String firstConnection = null;
        for (int i = 0; i <= out.size(); i++) {
            if (i == out.size() || out.get(i).startsWith("m=")) {
                if (start >= 0) {
                    String connection = updateSection(out, start, i);
                    if (firstConnection == null) {
                        firstConnection = connection;
                    }
                }
                start = i;
            }
        }
        // A session-level c= line is the default for sections without their own, so it must not
        // keep naming an address the answer no longer announces.
        for (int i = 0; firstConnection != null && i < out.size() && !out.get(i).startsWith("m="); i++) {
            if (out.get(i).startsWith("c=")) {
                out.set(i, firstConnection);
            }
        }
        return out;
    }

    /** Updates one section and returns its connection line. */
    private static String updateSection(List<String> lines, int start, int end) {
        InetAddress address = null;
        int port = 9;
        int preference = 0;
        for (int i = start + 1; i < end; i++) {
            if (!lines.get(i).startsWith(CANDIDATE)) {
                continue;
            }
            Optional<CandidateLine> parsed = CandidateLine.parse(lines.get(i));
            if (parsed.isEmpty() || parsed.get().component() != 1 || !parsed.get().isUdp()) {
                continue;
            }
            CandidateLine candidate = parsed.get();
            InetAddress candidateAddress = candidate.ipAddress().orElse(null);
            if (candidateAddress == null) {
                continue;
            }
            boolean sameFamily = address != null && address.getClass() == candidateAddress.getClass();
            int candidatePreference = candidate.type().defaultPreference();
            if ((sameFamily && candidatePreference <= preference)
                    || (address instanceof Inet4Address && !(candidateAddress instanceof Inet4Address))) {
                continue;
            }
            address = candidateAddress;
            port = candidate.port();
            preference = candidatePreference;
        }

        String[] media = lines.get(start).split(" ", 3);
        if (media.length == 3 && !"0".equals(media[1])) {
            lines.set(start, media[0] + " " + port + " " + media[2]);
        }
        String connection = address == null ? "c=IN IP4 0.0.0.0"
                : "c=IN " + (address instanceof Inet4Address ? "IP4 " : "IP6 ") + NetUtil.toAddressString(address);
        for (int i = start + 1; i < end; i++) {
            if (lines.get(i).startsWith("c=")) {
                lines.set(i, connection);
                break;
            }
        }
        return connection;
    }
}

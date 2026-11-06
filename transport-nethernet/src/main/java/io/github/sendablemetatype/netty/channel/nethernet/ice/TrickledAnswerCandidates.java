package io.github.sendablemetatype.netty.channel.nethernet.ice;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Applies a {@link CandidateAnnouncement} to one session's local candidates as ICE gathers them,
 * for signaling that trickles them (Xbox, LAN). Every candidate the engine reports goes through
 * {@link #local}; whatever it returns goes out as one signal per entry, translations included.
 * Calls must not overlap, which the engine's one callback thread already ensures.
 * <p>
 * Narrowing to {@linkplain CandidateAnnouncement.Advertised advertised addresses} needs to know
 * whether any listed endpoint is gathered here, which a single candidate cannot tell. Host
 * candidates are therefore held until the first candidate of another type, or the end of
 * gathering, whichever comes first. libwebrtc reports every host candidate before any reflexive
 * or relayed one, which needs a round trip to a server, so the hold lasts that long at most.
 * A host candidate that arrives after the decision is kept unless the answer was narrowed and
 * no listed endpoint names it.
 */
public final class TrickledAnswerCandidates {
    private final CandidateAnnouncement announcement;
    private final Translator translator;
    /** Advertised only: host candidates waiting for the narrowing decision. */
    private final List<CandidateLine> held = new ArrayList<>();
    private boolean decided;
    private List<InetSocketAddress> narrowedTo = List.of();
    private List<InetSocketAddress> foreign = List.of();

    TrickledAnswerCandidates(CandidateAnnouncement announcement) {
        this.announcement = announcement;
        this.translator = new Translator(announcement instanceof CandidateAnnouncement.Relayed);
    }

    /**
     * @param candidate A local candidate as the engine reports it, {@code candidate:...}
     * @return The candidates to signal now, in order, possibly none
     */
    public synchronized List<String> local(String candidate) {
        Optional<CandidateLine> parsed = CandidateLine.parse(candidate);
        if (parsed.isEmpty()) {
            // Not ours to judge; the peer's engine decides what to make of it
            return List.of(candidate);
        }
        CandidateLine line = parsed.get();
        List<CandidateLine> out = new ArrayList<>();
        switch (this.announcement) {
            case CandidateAnnouncement.Added added -> {
                this.translator.present(line);
                out.add(line);
                out.addAll(translateFrom(line, added.endpoints()));
            }
            case CandidateAnnouncement.Relayed relayed -> {
                AnswerCandidates.relayedKeeps(line).ifPresent(out::add);
                out.addAll(translateFrom(line, relayed.relays()));
            }
            case CandidateAnnouncement.Advertised advertised -> {
                if (!this.decided && line.type() == CandidateType.HOST) {
                    this.held.add(line);
                    return List.of();
                }
                if (!this.decided) {
                    out.addAll(decide(advertised.endpoints(), line));
                }
                if (line.type() != CandidateType.HOST || this.narrowedTo.isEmpty()
                        || Translator.namedByAny(advertised.endpoints(), line)) {
                    out.add(line);
                }
                out.addAll(translateFrom(line, this.foreign));
            }
        }
        return out.stream().map(CandidateLine::sdp).toList();
    }

    /**
     * @return The candidates still held, to signal now that gathering is complete
     */
    public synchronized List<String> gatheringComplete() {
        if (this.decided || !(this.announcement instanceof CandidateAnnouncement.Advertised advertised)) {
            return List.of();
        }
        return decide(advertised.endpoints(), null).stream().map(CandidateLine::sdp).toList();
    }

    /**
     * Settles the narrowing on the held host candidates, as {@link AnswerCandidates#fullAnswer}
     * would on the whole answer.
     *
     * @param trigger The non-host candidate that ended the hold, or null at the end of gathering
     * @return The held candidates that stay, then the translations
     */
    private List<CandidateLine> decide(List<InetSocketAddress> endpoints, CandidateLine trigger) {
        this.decided = true;
        List<CandidateLine> gathered = new ArrayList<>(this.held);
        if (trigger != null) {
            gathered.add(trigger);
        }
        List<CandidateLine> udpHosts = this.held.stream().filter(CandidateLine::isUdp).toList();
        List<InetSocketAddress> narrowedTo = new ArrayList<>();
        List<InetSocketAddress> foreign = new ArrayList<>();
        for (InetSocketAddress endpoint : endpoints) {
            boolean named = gathered.stream().anyMatch(candidate -> Translator.names(endpoint, candidate));
            (named ? narrowedTo : foreign).add(endpoint);
        }
        this.narrowedTo = List.copyOf(narrowedTo);
        this.foreign = List.copyOf(foreign);

        AnswerCandidates.Rewrite rewrite = AnswerCandidates.advertised(gathered, udpHosts, endpoints, this.translator);
        List<CandidateLine> out = new ArrayList<>();
        for (CandidateLine host : this.held) {
            rewrite.kept().apply(host).ifPresent(out::add);
        }
        out.addAll(rewrite.added());
        this.held.clear();
        return out;
    }

    /** Translations a new UDP host candidate makes possible, for a family no earlier host covered. */
    private List<CandidateLine> translateFrom(CandidateLine candidate, List<InetSocketAddress> endpoints) {
        if (candidate.type() != CandidateType.HOST || !candidate.isUdp() || endpoints.isEmpty()) {
            return List.of();
        }
        return this.translator.translate(List.of(candidate), endpoints);
    }
}

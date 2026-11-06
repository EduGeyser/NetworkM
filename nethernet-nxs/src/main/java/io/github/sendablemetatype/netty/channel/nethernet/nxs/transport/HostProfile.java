package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.DtlsFingerprint;
import io.github.sendablemetatype.netty.channel.nethernet.nxs.admission.StatelessAdmissionCodec;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * What the provider puts into the answers it builds for this host: the candidates, the certificate every peer of
 * this endpoint presents, and the stateless admission parameters. The client turns it into the wire profile and
 * assigns each candidate its foundation and priority.
 *
 * @param candidates      0 to 32 distinct candidates, most preferred first within each type; an empty list
 *                        withdraws the routes without draining the listener
 * @param dtlsFingerprint The fingerprint of the endpoint's DTLS certificate
 * @param admissionKeyId  The installed epoch the provider issues tokens under, the newest that is not retired
 * @param sctpPort        The host's SCTP port
 * @param maxMessageSize  The largest message the host accepts, at most 262144
 * @param incarnation     32 lowercase hex digits, fresh for each bound endpoint; the token audience names it
 */
public record HostProfile(List<HostCandidate> candidates, DtlsFingerprint dtlsFingerprint, String admissionKeyId,
                          int sctpPort, int maxMessageSize, String incarnation) {
    public HostProfile {
        candidates = List.copyOf(candidates);
        Objects.requireNonNull(dtlsFingerprint, "dtlsFingerprint");
        if (candidates.size() > 32 || new HashSet<>(candidates).size() != candidates.size()) {
            throw new IllegalArgumentException("At most 32 distinct candidates");
        }
        if (admissionKeyId == null || !admissionKeyId.matches("[A-Z0-9]{4}")
                || sctpPort < 1 || sctpPort > 65535
                || maxMessageSize < 1 || maxMessageSize > StatelessAdmissionCodec.MAX_MESSAGE_SIZE
                || incarnation == null || !incarnation.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("Invalid host profile");
        }
    }
}

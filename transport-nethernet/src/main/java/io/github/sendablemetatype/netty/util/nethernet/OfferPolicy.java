package io.github.sendablemetatype.netty.util.nethernet;

import java.util.Optional;

/**
 * Decides which offer check results the admission stage admits. Runs after the check, so it may
 * be called on an event loop and must not block.
 */
@FunctionalInterface
public interface OfferPolicy {
    /**
     * Admits offers without an assertion and every offer whose key is proven, attested or not;
     * refuses malformed assertions and failed proofs. LAN discovery's default: retail 1.26.63 sends
     * its LAN offer with no {@code a=identity} (captured 2026-11-07), and vanilla BDS admits it. A
     * join admitted without an assertion has no client key, so its login binding is unbound.
     */
    OfferPolicy ALLOW_UNASSERTED = result -> switch (result.level()) {
        case MALFORMED, PROOF_FAILED -> Optional.of(IdentityRefusal.IDENTITY_INVALID);
        case NO_ASSERTION, KEY_PROVEN, ATTESTED -> Optional.empty();
    };

    /**
     * The default for every source but LAN discovery (ruling C-3): admit every offer whose key is
     * proven, attested or not; refuse offers without an assertion, malformed ones and failed
     * proofs. Education 26.33 offers carry a self-signed token that no issuer attests, and pass on
     * their proof.
     */
    OfferPolicy REQUIRE_KEY_PROVEN = result -> switch (result.level()) {
        case NO_ASSERTION -> Optional.of(IdentityRefusal.IDENTITY_MISSING);
        case MALFORMED, PROOF_FAILED -> Optional.of(IdentityRefusal.IDENTITY_INVALID);
        case KEY_PROVEN, ATTESTED -> Optional.empty();
    };

    /**
     * Admits only offers whose token a trusted issuer signed. Refuses every Education 26.33 client
     * and every join while the issuer's keys are unreachable and uncached.
     */
    OfferPolicy REQUIRE_ATTESTED = result -> switch (result.level()) {
        case KEY_PROVEN -> Optional.of(result.reason() == OfferCheckResult.Reason.TRUST_SOURCE_UNREACHABLE
                ? IdentityRefusal.TRUST_SOURCE_UNREACHABLE
                : IdentityRefusal.NOT_ATTESTED);
        default -> REQUIRE_KEY_PROVEN.refusal(result);
    };

    /**
     * @param result the offer's check result
     * @return empty to admit the offer, otherwise why it is refused
     */
    Optional<IdentityRefusal> refusal(OfferCheckResult result);
}

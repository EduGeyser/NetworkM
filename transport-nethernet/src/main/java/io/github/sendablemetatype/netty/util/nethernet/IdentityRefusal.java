package io.github.sendablemetatype.netty.util.nethernet;

/**
 * Why an {@link OfferPolicy} refuses an offer. The admission stage turns it into its own typed
 * refusal; the initial table of ruling E-7 maps the first three to HTTP 401 and
 * {@link #TRUST_SOURCE_UNREACHABLE} to HTTP 500.
 */
public enum IdentityRefusal {
    /** The offer carries no identity assertion. */
    IDENTITY_MISSING,
    /** The assertion is malformed, or its key proof failed. */
    IDENTITY_INVALID,
    /** The key is proven, but the policy wants an attested token and the token is not. */
    NOT_ATTESTED,
    /** The policy wants an attested token and the issuer's keys could not be reached. */
    TRUST_SOURCE_UNREACHABLE
}

package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

/**
 * The NXS side of the login key binding: what the transport's one-shot identity verifier asks an admitted ticket
 * when the game validates the player's login. The verifier owns the one-shot decision and calls these methods; NXS
 * only answers them.
 *
 * <p>For a stateless ticket the comparison recomputes the HMAC binding with the admitted key epoch, so a key that
 * was rotated or reused since admission still decides the login it admitted. For an assisted join it compares the
 * provider-verified client key directly.
 *
 * <p>All methods are thread safe, never block and perform no I/O.
 */
public interface LoginKeyBinding {
    /**
     * @return Whether the admission behind this binding is still live: not past its login deadline and not revoked
     */
    boolean usable();

    /**
     * Compares in constant time.
     *
     * @param canonicalKey The login's identity key as canonical P-384 SPKI DER, 120 bytes
     * @return Whether it is the key this ticket was admitted for; false once released
     */
    boolean matches(byte[] canonicalKey);

    /**
     * Erases what the binding holds and releases the key epoch it retains. Only the first call has an effect, so the
     * verifier and the admission gate may both call it.
     */
    void release();
}

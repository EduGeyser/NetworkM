package io.github.sendablemetatype.netty.channel.nethernet.engine;

import java.util.Objects;

/**
 * Local ICE credentials. The password is never printed by {@link #toString()}.
 *
 * @param ufrag the username fragment
 * @param password the password
 */
public record IceCredentials(String ufrag, String password) {

    public IceCredentials {
        Objects.requireNonNull(ufrag, "ufrag");
        Objects.requireNonNull(password, "password");
    }

    @Override
    public String toString() {
        return "IceCredentials[ufrag=" + ufrag + ", password=<redacted>]";
    }
}

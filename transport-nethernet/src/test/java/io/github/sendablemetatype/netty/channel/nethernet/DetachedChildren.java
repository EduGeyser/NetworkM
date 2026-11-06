package io.github.sendablemetatype.netty.channel.nethernet;

/** Child channels for tests in other packages that only need the channel type. */
public final class DetachedChildren {
    private DetachedChildren() {
    }

    /** A child with no parent, addresses, peer or signaling. */
    public static NetherNetChildChannel create() {
        return ChildFixture.child(null, null, null);
    }
}

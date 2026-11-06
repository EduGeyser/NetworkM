package io.github.sendablemetatype.netty.channel.nethernet.ice;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A resolver the test answers: each lookup waits until the test answers or fails it, and the
 * intake's follow-up runs on the test's thread as it does. No DNS.
 */
public final class ManualResolver implements RemoteCandidateIntake.Resolver {
    private record Asked(String name, CompletableFuture<InetAddress> address) {
    }

    private final List<Asked> asked = new ArrayList<>();

    @Override
    public synchronized CompletionStage<InetAddress> resolve(String name) {
        CompletableFuture<InetAddress> address = new CompletableFuture<>();
        asked.add(new Asked(name, address));
        return address;
    }

    /** The names looked up so far, in order. */
    public synchronized List<String> names() {
        return asked.stream().map(Asked::name).toList();
    }

    /** Answers every pending lookup of this name with the address. */
    public void answer(String name, InetAddress address) {
        pending(name).forEach(lookup -> lookup.complete(address));
    }

    /** Answers every pending lookup of this name with the literal, read without DNS. */
    public void answer(String name, String literal) {
        answer(name, InetAddress.ofLiteral(literal));
    }

    /** Fails every pending lookup of this name, as for a name that does not resolve. */
    public void fail(String name) {
        pending(name).forEach(lookup -> lookup.completeExceptionally(new UnknownHostException(name)));
    }

    private synchronized List<CompletableFuture<InetAddress>> pending(String name) {
        List<CompletableFuture<InetAddress>> pending = new ArrayList<>();
        for (Asked lookup : asked) {
            if (lookup.name().equals(name) && !lookup.address().isDone()) {
                pending.add(lookup.address());
            }
        }
        return pending;
    }
}

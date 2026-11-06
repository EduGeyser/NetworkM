package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** A {@link JoinSink} standing in for the endpoint: records joins and admission checks. */
final class RecordingSink implements JoinSink {
    /** Joins not yet taken with {@link #next()}. */
    final BlockingQueue<Join> joins = new LinkedBlockingQueue<>();
    /** Every join delivered, in order, taken or not. */
    final List<Join> delivered = new CopyOnWriteArrayList<>();
    final List<SourcedAddress> checked = new CopyOnWriteArrayList<>();
    volatile boolean admits = true;
    /** Runs on each join as it arrives, on the source's thread. */
    volatile Consumer<Join> onJoin = join -> {
    };

    @Override
    public void join(Join join) {
        delivered.add(join);
        joins.add(join);
        onJoin.accept(join);
    }

    @Override
    public boolean admits(SourcedAddress source) {
        checked.add(source);
        return admits;
    }

    Join next() throws InterruptedException {
        Join join = joins.poll(10, TimeUnit.SECONDS);
        assertNotNull(join, "no join arrived");
        return join;
    }

    /** The signals delivered to a join from now on, in a readable form. */
    static List<String> signalsOf(Join join) {
        List<String> signals = new CopyOnWriteArrayList<>();
        join.reply().onRemoteSignal(signal -> signals.add(switch (signal) {
            case RemoteSignal.Candidate candidate -> "candidate " + candidate.candidate();
            case RemoteSignal.Error error -> "error " + error.code();
        }));
        return signals;
    }
}

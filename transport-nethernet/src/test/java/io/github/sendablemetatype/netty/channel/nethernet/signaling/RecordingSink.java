package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.address.SourcedAddress;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** A sink that keeps every join a source delivers and answers {@link #admits} as told. */
final class RecordingSink implements JoinSink {
    final List<Join> joins = new CopyOnWriteArrayList<>();
    volatile boolean admitting = true;

    @Override
    public void join(Join join) {
        joins.add(join);
    }

    @Override
    public boolean admits(SourcedAddress source) {
        return admitting;
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

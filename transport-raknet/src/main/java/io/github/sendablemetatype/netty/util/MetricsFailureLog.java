package io.github.sendablemetatype.netty.util;

import io.netty.util.internal.logging.InternalLogger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Logs a RakNet metrics hook that threw. Hooks are host code, so the transport catches what they
 * throw, logs it here and carries on. The first failure of each hook in the process logs as a
 * warning, later ones at debug: most hooks run per packet, and one that always throws would
 * otherwise write a warning for every packet anyone sends.
 */
public final class MetricsFailureLog {
    // One entry per hook name, so the set stays as small as the metrics interfaces.
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

    private MetricsFailureLog() {
    }

    /**
     * Logs a hook's failure.
     *
     * @param log the logger of the class that called the hook
     * @param hook the hook, as {@code Interface.method}
     * @param subject what the hook reported on, such as an address or channel
     * @param cause what the hook threw
     */
    public static void failed(InternalLogger log, String hook, Object subject, RuntimeException cause) {
        if (WARNED.add(hook)) {
            log.warn("Metrics hook {} failed for {}; later failures of this hook log at debug", hook, subject, cause);
        } else {
            log.debug("Metrics hook {} failed for {}", hook, subject, cause);
        }
    }
}

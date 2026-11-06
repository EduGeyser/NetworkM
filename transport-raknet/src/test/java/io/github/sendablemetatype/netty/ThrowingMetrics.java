package io.github.sendablemetatype.netty;

import java.lang.reflect.Proxy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Metrics whose every hook records its name and throws, for tests that the transport carries on. */
public final class ThrowingMetrics<T> {
    private final Set<String> called = ConcurrentHashMap.newKeySet();
    private final T hooks;

    private ThrowingMetrics(Class<T> type) {
        this.hooks = type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> switch (method.getName()) {
                    case "toString" -> "throwing " + type.getSimpleName();
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> {
                        called.add(method.getName());
                        throw new IllegalStateException("Metrics unavailable");
                    }
                }));
    }

    /** Throwing hooks for a metrics interface. */
    public static <T> ThrowingMetrics<T> of(Class<T> type) {
        return new ThrowingMetrics<>(type);
    }

    /** The hooks to install. */
    public T hooks() {
        return hooks;
    }

    /** The names of the hooks called so far. */
    public Set<String> called() {
        return Set.copyOf(called);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.config;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The NetherNet options one channel config handles, each declared once with its default and the
 * values it allows. The config's {@code getOption}, {@code setOption} and {@code getOptions} go
 * through this table, so adding an option to a config is the one line that declares it.
 * <p>
 * A config handles exactly the options that take effect on its channel. Any other option that
 * Netty's own config does not take either is refused with an {@link IllegalArgumentException}, so
 * a server option set on a client channel, or a socket option of another transport, fails where
 * it is set instead of being kept without effect.
 */
final class NetherOptions {
    // Concurrent, since a subclass adds its options after the base constructor published the table.
    private final Map<ChannelOption<?>, Setting<?>> settings = new ConcurrentHashMap<>();

    /**
     * Declares an option.
     *
     * @param option  the option
     * @param initial its value until one is set, which the rule does not check
     * @param rule    checks a value set for it and returns what is stored
     * @return the option's setting, which the config reads its value from
     */
    <T> Setting<T> add(ChannelOption<T> option, T initial, Rule<T> rule) {
        Setting<T> setting = new Setting<>(name(option), initial, rule);
        if (settings.putIfAbsent(option, setting) != null) {
            throw new IllegalStateException(setting.name + " is declared twice");
        }
        return setting;
    }

    boolean handles(ChannelOption<?> option) {
        return settings.containsKey(option);
    }

    /** @return the option's value; the option must be one this table handles */
    <T> T get(ChannelOption<T> option) {
        return setting(option).get();
    }

    /**
     * Checks and stores a value; the option must be one this table handles.
     *
     * @throws IllegalArgumentException if the value is out of the option's range
     * @throws NullPointerException     if the value is null and the option has no "none"
     */
    <T> void set(ChannelOption<T> option, T value) {
        setting(option).set(value);
    }

    ChannelOption<?>[] options() {
        return settings.keySet().toArray(ChannelOption<?>[]::new);
    }

    /** The refusal for an option neither this table nor Netty's config handles. */
    static IllegalArgumentException unknown(Channel channel, ChannelOption<?> option) {
        return new IllegalArgumentException(channel.getClass().getSimpleName() + " does not take the channel option "
                + name(option));
    }

    // Each setting is stored under its own option, so the types match.
    @SuppressWarnings("unchecked")
    private <T> Setting<T> setting(ChannelOption<T> option) {
        Setting<?> setting = settings.get(option);
        if (setting == null) {
            throw new IllegalArgumentException(name(option) + " is not declared");
        }
        return (Setting<T>) setting;
    }

    /** The option's own name: NetherNet options are named after their class too, which says nothing here. */
    static String name(ChannelOption<?> option) {
        String name = option.name();
        return name.substring(name.lastIndexOf('#') + 1);
    }

    /** One option's current value. Read from any thread. */
    static final class Setting<T> {
        private final String name;
        private final Rule<T> rule;
        private volatile T value;

        private Setting(String name, T initial, Rule<T> rule) {
            this.name = name;
            this.rule = Objects.requireNonNull(rule, "rule");
            this.value = initial;
        }

        T get() {
            return value;
        }

        private void set(T value) {
            this.value = rule.apply(name, value);
        }
    }

    /**
     * Checks a value set for an option and returns what is stored, for example an immutable copy.
     * Rules refuse null unless they say otherwise.
     */
    @FunctionalInterface
    interface Rule<T> {
        /**
         * @param option the option's name, for the message
         * @param value  the value set
         * @return the value to store
         */
        T apply(String option, T value);

        /** Any value but null. */
        static <T> Rule<T> present() {
            return (option, value) -> Objects.requireNonNull(value, () -> option + " must not be null");
        }

        /** Any value; null stands for none. */
        static <T> Rule<T> orNone() {
            return (option, value) -> value;
        }

        /** An integer of at least {@code min}. */
        static Rule<Integer> atLeast(int min) {
            return (option, value) -> {
                Objects.requireNonNull(value, () -> option + " must not be null");
                if (value < min) {
                    throw new IllegalArgumentException(option + " must be at least " + min + ", not " + value);
                }
                return value;
            };
        }

        /** A duration longer than zero. */
        static Rule<Duration> positiveDuration() {
            return (option, value) -> {
                Objects.requireNonNull(value, () -> option + " must not be null");
                if (value.isNegative() || value.isZero()) {
                    throw new IllegalArgumentException(option + " must be longer than zero, not " + value);
                }
                return value;
            };
        }

        /** A list without null elements, stored as an immutable copy. */
        static <E> Rule<List<E>> list() {
            return list(Integer.MAX_VALUE, (option, element) -> element);
        }

        /**
         * A list of at most {@code max} elements, none null, each passing {@code element}, stored as an
         * immutable copy.
         */
        static <E> Rule<List<E>> list(int max, Rule<E> element) {
            return (option, value) -> {
                Objects.requireNonNull(value, () -> option + " must not be null");
                if (value.size() > max) {
                    throw new IllegalArgumentException(option + " takes at most " + max + " entries, not " + value.size());
                }
                return value.stream().map(entry -> element.apply(option,
                        Objects.requireNonNull(entry, () -> option + " must not contain null"))).toList();
            };
        }
    }
}

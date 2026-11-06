package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * One provider reply, from either carrier.
 *
 * @param headers By lowercase field name
 */
record ProviderReply(int status, Map<String, List<String>> headers, String body) {
    ProviderReply {
        headers = Map.copyOf(headers);
    }

    Optional<String> header(String name) {
        List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
        return values == null || values.isEmpty() ? Optional.empty() : Optional.of(values.getFirst());
    }
}

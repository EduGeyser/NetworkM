/*
 * Copyright 2026 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.github.sendablemetatype.netty.util.address;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The proxies allowed to speak for a client, split by role: those whose PROXY protocol header is
 * read, and those whose {@code X-Forwarded-For} header is read. A proxy in one list is not trusted
 * for the other role.
 * <p>
 * An immutable value. Parse each list once with {@link #parseList} at startup, off any I/O
 * thread, since parsing can look names up and fetch lists, and hand the same value to every
 * consumer: HTTP signaling, and the ICE endpoint, whose PROXY protocol relays are the
 * {@link #proxyProtocol} list. Each list keeps the entries that could not be read, so that the
 * ICE endpoint can refuse a relay list none of whose entries could be read (see {@link ProxyList}).
 *
 * @param proxyProtocol The peers whose PROXY protocol header declares the client
 * @param forwardedFor  The peers whose {@code X-Forwarded-For} entries are believed
 */
public record TrustedProxies(ProxyList proxyProtocol, ProxyList forwardedFor) {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(TrustedProxies.class);

    /** Trusts no proxy in either role. */
    public static final TrustedProxies NONE = new TrustedProxies(ProxyList.NONE, ProxyList.NONE);

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    /** Far above any real list; keeps a misconfigured URL from filling the heap. */
    private static final int MAX_LIST_BYTES = 1 << 20;

    /** Fetches a list body; a test seam, the default being an HTTPS GET. */
    @FunctionalInterface
    interface Fetcher {
        String fetch(URI uri) throws IOException, InterruptedException;
    }

    public TrustedProxies {
        Objects.requireNonNull(proxyProtocol, "proxyProtocol");
        Objects.requireNonNull(forwardedFor, "forwardedFor");
    }

    /**
     * Parses one configured list. Each entry is an address, a CIDR range, a host name, or an
     * {@code https://} URL whose body lists more entries, one per line, {@code #} starting a
     * comment. An entry that cannot be read is logged and adds nothing, so it never widens trust,
     * and the result names it ({@link ProxyList#unreadable()}): a name that does not resolve, a
     * list that cannot be fetched, a list that names nothing. Blocks while names resolve and lists
     * download.
     *
     * @param entries The configured entries
     * @return The parsed list, which names nothing when nothing is configured
     * @throws IllegalArgumentException If an entry is an {@code http://} URL: anyone on the path
     *                                  could then add trusted proxies
     */
    public static ProxyList parseList(Collection<String> entries) {
        return parseList(entries, TrustedProxies::fetch);
    }

    static ProxyList parseList(Collection<String> entries, Fetcher fetcher) {
        List<String> unreadable = new ArrayList<>();
        IpRangeSet ranges = IpRangeSet.parse(expand(entries, fetcher, unreadable::add), InetAddress::getAllByName,
                unreadable::add);
        return new ProxyList(ranges, unreadable);
    }

    private static List<String> expand(Collection<String> entries, Fetcher fetcher, Consumer<String> unreadable) {
        List<String> out = new ArrayList<>();
        if (entries == null) {
            return out;
        }
        for (String entry : entries) {
            String trimmed = entry == null ? "" : entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String lower = trimmed.toLowerCase(Locale.ROOT);
            if (lower.startsWith("http://")) {
                throw new IllegalArgumentException("Refusing the trusted proxy list " + trimmed
                        + ": over plain HTTP anyone on the path can add trusted proxies. Use https://.");
            }
            if (!lower.startsWith("https://")) {
                out.add(trimmed);
                continue;
            }
            try {
                List<String> listed = fetcher.fetch(URI.create(trimmed)).lines()
                        .map(String::trim)
                        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                        .toList();
                if (listed.isEmpty()) {
                    // A list is kept to name proxies, so an empty one is more likely a broken
                    // publication than a deliberate one, and must not turn configured relays into none.
                    log.error("The trusted proxy list at {} names no entries, so it counts as unreadable", trimmed);
                    unreadable.accept(trimmed);
                }
                out.addAll(listed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while fetching the trusted proxy list at {}, no addresses taken from it",
                        trimmed);
                unreadable.accept(trimmed);
            } catch (IOException | RuntimeException e) {
                log.error("Could not fetch the trusted proxy list at {}, no addresses taken from it", trimmed, e);
                unreadable.accept(trimmed);
            }
        }
        return out;
    }

    private static String fetch(URI uri) throws IOException, InterruptedException {
        // A redirect could lead to plain HTTP, so none is followed
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build()) {
            HttpResponse<InputStream> response = client.send(
                    HttpRequest.newBuilder(uri).timeout(TIMEOUT).GET().build(),
                    HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream body = response.body()) {
                if (response.statusCode() != 200) {
                    throw new IOException("returned HTTP " + response.statusCode());
                }
                byte[] bytes = body.readNBytes(MAX_LIST_BYTES + 1);
                if (bytes.length > MAX_LIST_BYTES) {
                    throw new IOException("longer than " + MAX_LIST_BYTES + " bytes");
                }
                return new String(bytes, StandardCharsets.UTF_8);
            }
        }
    }

    /**
     * @param peer The address a connection came from
     * @return Whether its PROXY protocol header may declare the client
     */
    public boolean trustsProxyProtocol(InetSocketAddress peer) {
        return this.proxyProtocol.ranges().contains(peer);
    }

    /**
     * @param peer The address a connection came from
     * @return Whether its {@code X-Forwarded-For} entries may name the client
     */
    public boolean trustsForwardedFor(InetSocketAddress peer) {
        return this.forwardedFor.ranges().contains(peer);
    }

    /**
     * @param peer The address a connection came from
     * @return Whether it is a trusted proxy in either role, so that what it sends names the proxy
     *         rather than the client, such as its {@code Host} header
     */
    public boolean isProxy(InetSocketAddress peer) {
        return trustsProxyProtocol(peer) || trustsForwardedFor(peer);
    }

    /**
     * The client a trusted forwarder speaks for. Each trusted proxy appends the address it took the
     * request from, and everything left of that is whatever the client sent, so the client is the
     * rightmost entry no trusted proxy accounts for, or the leftmost when every entry is trusted.
     * <p>
     * Every header line counts, in order: a proxy that adds a line of its own instead of extending
     * the client's must not leave the client's line to decide. An entry may carry a port
     * ({@code 1.2.3.4:5678}, {@code [2001:db8::1]:5678}); without one, the client takes the
     * forwarder connection's port. Names are never looked up, so the caller's thread never waits on
     * DNS, and an entry at or right of the client that is not an IP literal leaves the connection
     * its own address.
     *
     * @param peer         The address the request came from
     * @param headerValues Every {@code X-Forwarded-For} header line, in the order received
     * @return The client, or empty when the peer is not a trusted forwarder or the header names
     *         no client that can be read
     */
    public Optional<InetSocketAddress> forwardedClient(InetSocketAddress peer, List<String> headerValues) {
        if (!trustsForwardedFor(peer) || headerValues.isEmpty()) {
            return Optional.empty();
        }
        String[] entries = String.join(",", headerValues).split(",", -1);
        InetSocketAddress client = null;
        for (int i = entries.length - 1; i >= 0; i--) {
            String entry = entries[i].trim();
            try {
                client = EndpointAddress.parseEndpoint(entry);
            } catch (IllegalArgumentException notAnAddress) {
                return Optional.empty();
            }
            if (!this.forwardedFor.ranges().contains(client)) {
                break;
            }
        }
        return Optional.of(client.getPort() != 0 ? client : new InetSocketAddress(client.getAddress(), peer.getPort()));
    }
}

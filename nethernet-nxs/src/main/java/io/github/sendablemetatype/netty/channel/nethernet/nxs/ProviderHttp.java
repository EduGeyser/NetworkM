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

package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The HTTPS carrier: one request at a time from the client, never following a redirect, bodies capped before parsing. */
final class ProviderHttp {
    static final int MAX_BODY_BYTES = 65536;

    /**
     * The HTTP proxy between this host and the provider wants credentials, so the request never arrived. Warnings
     * name a failure by its type, which says this one.
     */
    static final class ProxyAuthenticationRequiredException extends IOException {
        ProxyAuthenticationRequiredException() {
            super("The HTTP proxy requires a login (HTTP 407), which is not supported");
        }
    }

    private final HttpClient client;

    ProviderHttp(HttpClient client) {
        if (client.followRedirects() != HttpClient.Redirect.NEVER) {
            throw new IllegalArgumentException("Provider clients must not follow redirects");
        }
        this.client = client;
    }

    static HttpClient defaultClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * @param body    The exact bytes to send and sign, or null for none
     * @param headers Extra request headers, including the signature when signed
     */
    CompletableFuture<ProviderReply> send(URI uri, String method, String body, Map<String, String> headers,
                                          Duration timeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("accept", "application/json")
                .method(method, body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (body != null) {
            builder.header("content-type", "application/json");
        }
        headers.forEach(builder::header);
        return client.sendAsync(builder.build(), info -> new LimitedBodySubscriber(MAX_BODY_BYTES))
                .thenApply(response -> {
                    byte[] bytes = response.body();
                    if (bytes == null && response.statusCode() == 407) {
                        // The JDK answers a proxy's 407 to the HTTPS tunnel itself, without a body, as this client
                        // has no proxy credentials to retry with
                        throw new java.util.concurrent.CompletionException(new ProxyAuthenticationRequiredException());
                    }
                    Map<String, List<String>> lower = new HashMap<>();
                    response.headers().map().forEach((name, values) -> lower.merge(name.toLowerCase(Locale.ROOT),
                            List.copyOf(values), (a, b) -> a));
                    return new ProviderReply(response.statusCode(), lower,
                            bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8));
                });
    }
}

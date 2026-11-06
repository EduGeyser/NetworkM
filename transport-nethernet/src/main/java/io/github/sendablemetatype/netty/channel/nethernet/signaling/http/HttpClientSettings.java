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

package io.github.sendablemetatype.netty.channel.nethernet.signaling.http;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetSignaling.IceServerInfo;

import javax.net.ssl.TrustManagerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * How {@code NetherNetHttpClientSignaling} reaches a server's {@code /v1/join} endpoint.
 * Immutable; {@link #DEFAULT} does what the retail client does.
 *
 * @param scheme        which scheme to speak
 * @param iceServers    the STUN and TURN servers to gather through. Every one of them has to
 *                      answer or time out before the offer can go, since HTTP carries no
 *                      candidates after it
 * @param trust         the trust the server's certificate is checked against, or null for the
 *                      JDK's default trust store. The certificate must also name the host the
 *                      client dialed
 * @param clientInfo    what the status request tells the server about the client, or null to
 *                      tell it nothing
 * @param answerTimeout how long to wait for the answer to an offer. The default, 20 s, outlasts
 *                      the server's default negotiation deadline of 15 s, so the client sees the
 *                      server's own timeout refusal rather than giving up first
 */
public record HttpClientSettings(Scheme scheme, List<IceServerInfo> iceServers, TrustManagerFactory trust,
                                 ClientInfo clientInfo, Duration answerTimeout) {

    /** Which scheme the client speaks. */
    public enum Scheme {
        /**
         * Asks {@code GET /v1/join} over HTTPS first and falls back to plain HTTP only when the
         * server refused TLS: closed or reset the connection before answering, or answered with
         * something other than TLS. A certificate the client rejects ends the attempt, as on the
         * retail client. Whoever can refuse TLS on the path gets a plain HTTP join, so a hop that
         * has to stay private uses {@link #HTTPS}.
         */
        AUTO,
        /** HTTPS only: no status request, no fallback. */
        HTTPS,
        /** Plain HTTP only, with no status request. */
        HTTP
    }

    /** The default answer wait. */
    public static final Duration DEFAULT_ANSWER_TIMEOUT = Duration.ofSeconds(20);

    /** {@link Scheme#AUTO}, no ICE servers, the JDK's trust, no client info, a 20 s answer wait. */
    public static final HttpClientSettings DEFAULT =
            new HttpClientSettings(Scheme.AUTO, List.of(), null, null, DEFAULT_ANSWER_TIMEOUT);

    /** Checks the settings; a null trust or client info is allowed, see the components. */
    public HttpClientSettings {
        Objects.requireNonNull(scheme, "scheme");
        iceServers = List.copyOf(iceServers);
        Objects.requireNonNull(answerTimeout, "answerTimeout");
        if (answerTimeout.isNegative() || answerTimeout.isZero()) {
            throw new IllegalArgumentException("answerTimeout must be positive: " + answerTimeout);
        }
    }

    /**
     * @param scheme the scheme
     * @return these settings with that scheme
     */
    public HttpClientSettings withScheme(Scheme scheme) {
        return new HttpClientSettings(scheme, iceServers, trust, clientInfo, answerTimeout);
    }

    /**
     * @param iceServers the STUN and TURN servers
     * @return these settings with those ICE servers
     */
    public HttpClientSettings withIceServers(List<IceServerInfo> iceServers) {
        return new HttpClientSettings(scheme, iceServers, trust, clientInfo, answerTimeout);
    }

    /**
     * @param trust the trust, for a private CA, or null for the JDK's default trust store
     * @return these settings checking certificates against that trust
     */
    public HttpClientSettings withTrust(TrustManagerFactory trust) {
        return new HttpClientSettings(scheme, iceServers, trust, clientInfo, answerTimeout);
    }

    /**
     * @param clientInfo what to tell the server about the client, as the retail client does, or null
     * @return these settings telling the server that
     */
    public HttpClientSettings withClientInfo(ClientInfo clientInfo) {
        return new HttpClientSettings(scheme, iceServers, trust, clientInfo, answerTimeout);
    }

    /**
     * @param answerTimeout how long to wait for an answer, positive
     * @return these settings waiting that long
     */
    public HttpClientSettings withAnswerTimeout(Duration answerTimeout) {
        return new HttpClientSettings(scheme, iceServers, trust, clientInfo, answerTimeout);
    }
}

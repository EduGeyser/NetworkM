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

package io.github.sendablemetatype.netty.channel.nethernet.config;

import java.net.InetSocketAddress;

/**
 * Events that belong to the listener rather than to one connection, which is
 * the only place a join that never became a channel can be seen. Anything
 * that happens once a connection exists belongs on its channel instead. An
 * exception thrown here is logged at debug and otherwise ignored.
 *
 * <p>The server channel reports the joins it admits and refuses. HTTP
 * signaling reports what it refuses on its own, before or instead of the
 * endpoint; give its builder the same instance, and nothing is counted twice.
 *
 * @see NetherChannelMetrics for the per connection counterpart
 */
public interface NetherServerMetrics {

    /**
     * A join became a child channel.
     *
     * @param networkId the remote peer's network ID from signaling
     * @param verifiedIdentity whether a trusted issuer attested the offer's
     *        token; false for a proven key with a self-signed token
     */
    default void connectionAccepted(String networkId, boolean verifiedIdentity) {
    }

    /**
     * A join was refused, by the endpoint or by HTTP signaling itself. The
     * status names the reason ({@code JoinRefusal#httpStatus()}), and is what
     * the peer was refused with over HTTP; joins over other sources report the
     * status of the same refusal. HTTP signaling also reports an offer it
     * turned away before reading it whole: {@code 413} for a body over its cap,
     * {@code 417} for an expectation it does not meet. A join counts once, for the refusal that
     * settled it: one that comes after the join was settled is not counted.
     */
    default void joinRefused(int httpStatus) {
    }

    /** The address already holds as many signaling connections as it may. */
    default void addressRefused(InetSocketAddress address) {
    }

    /**
     * HTTP signaling holds as many connections as it may in all, so it
     * refused one from this address, unread.
     *
     * @param address the connection's peer, which is the proxy for a proxied
     *        client
     */
    default void connectionCapRefused(InetSocketAddress address) {
    }

    /**
     * An HTTP signaling request did not arrive whole within its deadline
     * (10 s), so its connection closed unanswered. A connection that just
     * sends no second request after its status request is not reported.
     *
     * @param address the client a PROXY protocol header declared, else the
     *        connection's peer
     */
    default void requestTimedOut(InetSocketAddress address) {
    }

    /** A request arrived without TLS where it is required. */
    default void plaintextRefused(InetSocketAddress address) {
    }

    /**
     * The server's throttle turned a join from the address away. The endpoint
     * also reports the join to {@link #joinRefused} with the status of the
     * refusal, whatever its source, so this breaks those down rather than
     * adding to them.
     */
    default void joinThrottled(InetSocketAddress address) {
    }
}

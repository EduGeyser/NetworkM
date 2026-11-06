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

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.PeerState;
import io.github.sendablemetatype.netty.channel.nethernet.engine.TransportPath;

/**
 * Per connection counters and events. Called from the transport's threads,
 * not one of your own: any of them may fire on an engine thread or on the
 * channel's event loop, so an implementation is thread safe or hands off. An
 * exception thrown here is logged and otherwise ignored.
 */
public interface NetherChannelMetrics {

    default void bytesIn(int count) {
    }

    default void bytesOut(int count) {
    }

    /** Whole messages, however many segments each took. */
    default void messagesIn(int count) {
    }

    /** Whole messages, however many segments each took. */
    default void messagesOut(int count) {
    }

    /** Segments carrying part of a split message other than the last, which counts in {@link #messagesIn}. */
    default void fragmentsIn(int count) {
    }

    /** Segments carrying part of a split message other than the last, which counts in {@link #messagesOut}. */
    default void fragmentsOut(int count) {
    }

    /**
     * Inbound data that cannot be delivered: a broken reliable stream, and an
     * unreliable message that is empty, split or too large.
     */
    default void decodeFail(int count) {
    }

    default void peerStateChange(PeerState state) {
    }

    default void iceStateChange(IceState state) {
    }

    /** The candidate types of the selected pair, reported when the peer connects. */
    default void pathSelected(TransportPath.CandidateType localType, TransportPath.CandidateType remoteType) {
    }

    /** At most once per attempt, so a client that retries reports one per attempt. */
    default void connectionFailed(NetherConnectionFailure reason) {
    }

    default void handshakeRetry() {
    }
}

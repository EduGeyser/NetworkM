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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.transport;

import io.github.sendablemetatype.netty.channel.nethernet.ice.AnswerCandidates;

import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;

/**
 * The answer the host returns for an assisted join, which the provider passes on to the client.
 *
 * @param sdp At most 16384 characters. A player's answer carries the endpoint's {@code a=identity} line; a
 *            diagnostic answer carries none, as the diagnostic profile forbids it
 */
public record AssistedAnswer(String sdp) {
    public static final int MAX_LENGTH = 16384;

    public AssistedAnswer {
        if (sdp == null || sdp.isEmpty() || sdp.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("An assisted answer has 1 to 16384 characters");
        }
    }

    /** Whether the answer carries an identity assertion. */
    public boolean asserted() {
        return sdp.lines().anyMatch(line -> line.startsWith("a=identity:"));
    }

    /**
     * Builds the answer for a peer created on the gameplay port: the peer's local description with its gathered
     * candidates replaced by the one public endpoint gathered for this join, then signed. The {@code m=} port and
     * {@code c=} address follow that one candidate. Upstream keeps the engine's, which libwebrtc sets from the
     * gathered candidates, so the answer would still name a private address and port it no longer announces.
     *
     * @param localDescription The answering peer's local description
     * @param candidate        The candidate the client should check, on the gameplay port
     * @param signer           Adds the server identity; null for a diagnostic answer, which carries none
     */
    public static AssistedAnswer build(String localDescription, HostCandidate candidate, AnswerSigner signer)
            throws GeneralSecurityException {
        InetSocketAddress endpoint = candidate.endpoint();
        String answer = AnswerCandidates.withConnectionAddresses(String.join("\r\n", localDescription.lines()
                .filter(line -> !line.startsWith("a=candidate:")
                        && !line.startsWith("a=remote-candidates:")
                        && !line.equals("a=end-of-candidates"))
                .toList())
                + "\r\na=candidate:1 1 UDP 2130706431 " + endpoint.getAddress().getHostAddress()
                + " " + endpoint.getPort() + " typ " + candidate.typeName() +"\r\na=end-of-candidates\r\n");
        return new AssistedAnswer(signer == null ? answer : signer.sign(answer));
    }

    @Override
    public String toString() {
        return "AssistedAnswer[redacted]";
    }
}

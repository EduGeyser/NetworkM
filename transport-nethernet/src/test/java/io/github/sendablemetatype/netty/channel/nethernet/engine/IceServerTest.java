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

package io.github.sendablemetatype.netty.channel.nethernet.engine;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IceServerTest {

    @Test
    void keepsTheSecretOutOfItsOwnForm() {
        String printed = new IceServer(List.of("turn:turn.example:3478", "turn:someone:hunter2@relay.example:3478",
                "turns://other:pw@tls.example:5349"), "relayuser", "s3cret").toString();

        assertFalse(printed.contains("s3cret"));
        assertFalse(printed.contains("hunter2"));
        assertFalse(printed.contains("relayuser"));
        assertFalse(printed.contains("someone"));
        assertFalse(printed.contains("other:pw"));
        // Still worth reading
        assertTrue(printed.contains("turn.example:3478"));
        assertTrue(printed.contains("turn:<redacted>@relay.example:3478"));
        assertTrue(printed.contains("turns://<redacted>@tls.example:5349"));
    }

    @Test
    void leavesAStunServerAlone() {
        // No credentials to lose, and the host is the whole point of the line
        assertEquals("IceServer[urls=[stun:stun.example:3478], username=null, credential=null]",
                new IceServer(List.of("stun:stun.example:3478"), null, null).toString());
    }
}

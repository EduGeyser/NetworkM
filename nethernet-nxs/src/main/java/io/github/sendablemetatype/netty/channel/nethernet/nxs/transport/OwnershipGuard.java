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

/**
 * Says whether something captured earlier, such as a host profile or an assisted-join authority, still describes
 * the live endpoint. The client checks it again after persisting, inside the send queue and before applying a
 * reply, so a replaced endpoint is never acknowledged or answered for.
 */
@FunctionalInterface
public interface OwnershipGuard {
    /** A guard that never expires, for material that cannot be replaced. */
    OwnershipGuard ALWAYS = () -> {
    };

    /**
     * Returns when the capture is still current. Never waits, takes a lock or performs I/O; any thread may call it.
     *
     * @throws ProfileReplacedException If the endpoint is live but its material was replaced, so a fresh capture is
     *                                  needed
     * @throws IllegalStateException    If the endpoint behind the capture is gone
     */
    void requireCurrent();

    /** Both guards must hold. */
    default OwnershipGuard and(OwnershipGuard other) {
        return () -> {
            requireCurrent();
            other.requireCurrent();
        };
    }
}

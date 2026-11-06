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

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A host profile captured from the live endpoint, with the guard that says whether it still describes it. The
 * client holds the guard through persistence, the send queue and the reply.
 *
 * @param probeCandidates    Where connection checks may test this host, which may include endpoints withdrawn from
 *                           players; at most 32
 * @param assistedFamilies   The families the endpoint could serve assisted joins on, used only when assistance is
 *                           configured
 * @param candidateRevision  Changes with candidate material or mapping ownership, not with unchanged STUN
 *                           refreshes; 0 for an endpoint that tracks no revisions
 * @param publicationVersion A local counter that changes whenever what would be published changes
 */
public record HostProfileSnapshot(HostProfile profile, List<HostCandidate> probeCandidates,
                                  Set<AddressFamily> assistedFamilies, long candidateRevision,
                                  long publicationVersion, OwnershipGuard guard) {
    public HostProfileSnapshot {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(guard, "guard");
        probeCandidates = List.copyOf(probeCandidates);
        assistedFamilies = Set.copyOf(assistedFamilies);
        if (probeCandidates.size() > 32) {
            throw new IllegalArgumentException("At most 32 probe candidates");
        }
        if (candidateRevision < 0 || candidateRevision > 9007199254740991L || publicationVersion < 0) {
            throw new IllegalArgumentException("Invalid candidate revision");
        }
    }

    /** A profile from an endpoint without revisioned candidates or assistance. */
    public static HostProfileSnapshot of(HostProfile profile, OwnershipGuard guard) {
        return new HostProfileSnapshot(profile, profile.candidates(), Set.of(), 0, 0, guard);
    }

    /** @see OwnershipGuard#requireCurrent() */
    public void requireCurrent() {
        guard.requireCurrent();
    }
}

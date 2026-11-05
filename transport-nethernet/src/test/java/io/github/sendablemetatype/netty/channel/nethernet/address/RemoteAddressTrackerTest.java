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

package io.github.sendablemetatype.netty.channel.nethernet.address;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteAddressTrackerTest {
    private static final InetSocketAddress SIGNALED = new InetSocketAddress("198.51.100.20", 40000);
    private static final InetSocketAddress PLAYER = new InetSocketAddress("203.0.113.7", 54321);
    private static final InetSocketAddress ROAMED = new InetSocketAddress("203.0.113.8", 50000);
    private static final InetSocketAddress TURN = new InetSocketAddress("192.0.2.9", 3478);

    private static final SourcedAddress SIGNALING = new SourcedAddress(SIGNALED, AddressSource.SIGNALING, null);

    private static SourcedAddress ice(InetSocketAddress address) {
        return new SourcedAddress(address, AddressSource.ICE, null);
    }

    @Test
    void theSignaledAddressStandsUntilActive() {
        RemoteAddressTracker tracker = new RemoteAddressTracker(SIGNALING, false);

        assertEquals(Optional.empty(), tracker.selectedPair(ice(PLAYER)), "no event before activation");
        assertEquals(SIGNALING, tracker.reported());
        assertEquals(ice(PLAYER), tracker.activate());
        assertEquals(SIGNALING, tracker.signaled(), "what admission counted stays");
    }

    @Test
    void activationTakesTheNewestPair() {
        RemoteAddressTracker tracker = new RemoteAddressTracker(SourcedAddress.unknown(), false);
        // As a retail friends join did: a relay pair, then the client's reflexive address
        tracker.selectedPair(new SourcedAddress(TURN, AddressSource.TURN, null));
        tracker.selectedPair(ice(PLAYER));

        assertEquals(ice(PLAYER), tracker.activate());
    }

    @Test
    void withoutAPairTheSignaledAddressStaysOnActivation() {
        RemoteAddressTracker tracker = new RemoteAddressTracker(SIGNALING, false);
        assertEquals(SIGNALING, tracker.activate());
    }

    @Test
    void followsRenominationAfterActivationAndSaysSo() {
        RemoteAddressTracker tracker = new RemoteAddressTracker(SIGNALING, false);
        tracker.selectedPair(ice(PLAYER));
        tracker.activate();

        assertEquals(Optional.of(new RemoteAddressChangedEvent(ice(PLAYER), ice(ROAMED))),
                tracker.selectedPair(ice(ROAMED)));
        assertEquals(ice(ROAMED), tracker.reported());
        assertEquals(Optional.empty(), tracker.selectedPair(ice(ROAMED)), "the same address is no change");
    }

    @Test
    void anotherRelayForTheSameClientChangesOnlyTheSource() {
        RemoteAddressTracker tracker = new RemoteAddressTracker(SIGNALING, false);
        tracker.selectedPair(new SourcedAddress(PLAYER, AddressSource.ICE_PROXY_PROTOCOL, TURN));
        tracker.activate();

        SourcedAddress direct = ice(PLAYER);
        assertEquals(Optional.empty(), tracker.selectedPair(direct));
        assertEquals(direct, tracker.reported());
    }

    /** P10 / G-3: the transport peer actually connected, as with a proxy, never what the client claims. */
    @Test
    void aTurnPairReportsTheRelayWhateverSignalingSaid() {
        SourcedAddress turn = new SourcedAddress(TURN, AddressSource.TURN, null);
        for (AddressSource source : AddressSource.values()) {
            if (source == AddressSource.SIGNALING_PROXY_PROTOCOL || source == AddressSource.SIGNALING_FORWARDED_FOR) {
                continue;
            }
            SourcedAddress signaled = new SourcedAddress(SIGNALED, source, null);
            assertEquals(turn, RemoteAddressTracker.reported(signaled, turn, true), source.name());
        }
    }

    @Test
    void aProxiedAddressWinsOverAnyPairWhenTold() {
        List<SourcedAddress> pairs = List.of(ice(PLAYER),
                new SourcedAddress(PLAYER, AddressSource.ICE_PROXY_PROTOCOL, TURN),
                new SourcedAddress(TURN, AddressSource.TURN, null));
        for (AddressSource source : List.of(AddressSource.SIGNALING_PROXY_PROTOCOL,
                AddressSource.SIGNALING_FORWARDED_FOR)) {
            SourcedAddress signaled = new SourcedAddress(SIGNALED, source, null);
            for (SourcedAddress media : pairs) {
                assertEquals(signaled, RemoteAddressTracker.reported(signaled, media, true), source + " over " + media);
                assertEquals(media, RemoteAddressTracker.reported(signaled, media, false), "only when told");
            }
        }
    }

    @Test
    void aProxiedAddressStaysThroughRenominationWhenTold() {
        SourcedAddress proxied = new SourcedAddress(SIGNALED, AddressSource.SIGNALING_FORWARDED_FOR,
                new InetSocketAddress("10.0.0.2", 443));
        RemoteAddressTracker tracker = new RemoteAddressTracker(proxied, true);
        tracker.selectedPair(ice(PLAYER));

        assertEquals(proxied, tracker.activate());
        assertTrue(tracker.selectedPair(ice(ROAMED)).isEmpty());
        assertEquals(Optional.of(ice(ROAMED)), tracker.media());
    }

    @Test
    void anAddressNoProxyDeclaredFollowsThePairEvenWhenTold() {
        for (AddressSource source : List.of(AddressSource.SIGNALING, AddressSource.HOST,
                AddressSource.SIGNALING_LAN, AddressSource.NXS_ADMISSION, AddressSource.UNKNOWN)) {
            SourcedAddress signaled = new SourcedAddress(SIGNALED, source, null);
            assertEquals(ice(PLAYER), RemoteAddressTracker.reported(signaled, ice(PLAYER), true), source.name());
        }
    }
}

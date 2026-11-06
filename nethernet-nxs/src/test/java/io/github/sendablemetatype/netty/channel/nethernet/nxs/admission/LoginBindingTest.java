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

package io.github.sendablemetatype.netty.channel.nethernet.nxs.admission;

import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.ECGenParameterSpec;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** The NXS side of the login binding, which the transport's one-shot verifier consults. */
class LoginBindingTest extends AdmissionFixture {
    /** The fixture's client key, as canonical SPKI DER. */
    byte[] key() {
        return Base64.getDecoder().decode(f.getAsJsonObject("identity").get("canonicalCpk").getAsString());
    }

    AdmissionKey epoch() {
        return new AdmissionKey("K001", f.getAsJsonObject("context").get("secret").getAsString());
    }

    /** A P-384 key the ticket was not admitted for. */
    static PublicKey otherKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp384r1"));
        return generator.generateKeyPair().getPublic();
    }

    @Test
    void anAcceptedLoginIsNeverTakenForALapsedOne() {
        // Accepting the login erases the binding, which the sweep must not read as revoked
        var gate = new TicketLedger(TicketLedger.Limits.defaults(), validator());
        var r = gate.reserve(request(), now, 0);
        assertTrue(gate.ready(r));
        gate.connected(r);
        assertEquals(TransportIdentityBinding.Result.ACCEPTED, gate.verifier(r).acceptForwardedIdentity());
        assertEquals(List.of(), gate.sweep(now, 0));
    }

    @Test
    void theAdmittedKeyMatchesAndNoOtherDoes() {
        LoginKeyBinding binding = validator().validate(request(), now).loginBinding();
        assertTrue(binding.usable());
        assertTrue(binding.matches(key()));
        byte[] other = key();
        other[other.length - 1] ^= 1;
        assertFalse(binding.matches(other));
        binding.release();
        binding.release();
        assertFalse(binding.matches(key()), "a released binding decides nothing");
        assertFalse(binding.usable());
    }

    @Test
    void refreshRotationAndSameIdReplacementKeepTheActualAdmittedEpoch() {
        var v = validator();
        var refresh = v.validate(request(), now).loginBinding();
        v.installKeys(List.of(epoch()));
        assertTrue(refresh.matches(key()));
        var replaced = v.validate(request(), now).loginBinding();
        v.installKeys(List.of(new AdmissionKey("K001", "D-RcoACxXuZnQ8oyLohoC-ctg5Q-bFMgVYPLDCsYb3g")));
        assertNull(v.validate(request(), now));
        assertTrue(replaced.matches(key()), "the replaced secret still decides the login it admitted");
        v.installKeys(List.of(new AdmissionKey("K001", epoch().secret(), now + 1000)));
        var retired = v.validate(request(), now).loginBinding();
        v.retireKeys(now + 1000);
        assertFalse(v.ready());
        assertTrue(retired.matches(key()));
    }

    @Test
    void clearRevokesRetainedEpochs() {
        var v = validator();
        var retained = v.validate(request(), now).loginBinding();
        v.installKeys(List.of());
        v.clear();
        assertFalse(retained.usable());
        assertFalse(retained.matches(key()));
    }

    @Test
    void theLoginDeadlineIsMonotonic() {
        var nanos = new AtomicLong(0);
        var v = new StatelessAdmissionValidator(f.getAsJsonObject("context").get("audience").getAsString(), 60_000,
                nanos::get);
        v.installKeys(List.of(epoch()));
        var binding = v.validate(request(), now).loginBinding();
        assertTrue(binding.usable());
        nanos.set(30_000_000_000L);
        assertFalse(binding.usable(), "the ticket's remaining 30 seconds are counted from admission");
    }

    @Test
    void connectedWithoutLoginTimesOutButDecidedSessionsContinue() throws Exception {
        for (int mode = 0; mode < 3; mode++) {
            var gate = new TicketLedger(TicketLedger.Limits.defaults(), validator());
            var r = gate.reserve(request(), now, 0);
            assertTrue(gate.ready(r));
            gate.connected(r);
            if (mode == 1) {
                assertEquals(TransportIdentityBinding.Result.ACCEPTED, gate.verifier(r).acceptForwardedIdentity());
            }
            if (mode == 2) {
                assertEquals(TransportIdentityBinding.Result.MISMATCH, gate.verifier(r).verify(otherKey()));
            }
            List<TicketLedger.Reservation> expected = mode == 1 ? List.of() : List.of(r);
            assertEquals(expected, gate.sweep(now + 30_000, 30_000_000_000L));
            gate.finish(r);
        }
    }

    @Test
    void aRefusedLoginClosesAtOnce() throws Exception {
        var gate = new TicketLedger(TicketLedger.Limits.defaults(), validator());
        var r = gate.reserve(request(), now, 0);
        assertTrue(gate.ready(r));
        gate.connected(r);
        gate.verifier(r).verify(otherKey());
        assertEquals(List.of(r), gate.sweep(now, 0));
    }

    @Test
    void reserveRejectionAndTeardownReleaseTheirOwnBinding() {
        var v = validator();
        var issued = new ArrayList<VerifiedAdmission>();
        var gate = new TicketLedger(TicketLedger.Limits.defaults(), (request, time) -> {
            var a = v.validate(request, time);
            issued.add(a);
            return a;
        });
        var accepted = gate.reserve(request(), now, 0);
        assertNull(gate.reserve(request(), now, 0));
        assertFalse(issued.get(1).loginBinding().usable(), "a replayed ticket's binding is released at once");
        assertTrue(issued.get(0).loginBinding().usable());
        gate.finish(accepted);
        assertFalse(issued.get(0).loginBinding().usable());
        gate.drain();
        assertNull(gate.reserve(request(), now, 0));
        assertFalse(issued.get(2).loginBinding().usable());
    }
}

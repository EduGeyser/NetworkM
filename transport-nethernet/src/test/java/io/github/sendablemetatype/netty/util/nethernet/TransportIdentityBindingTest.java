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

package io.github.sendablemetatype.netty.util.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.DetachedChildren;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding.Result;
import io.github.sendablemetatype.netty.util.nethernet.TransportIdentityBinding.State;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;

import static io.github.sendablemetatype.netty.util.nethernet.OfferFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

class TransportIdentityBindingTest {

    /** A child opened by the given offer, carrying what the server channel installs. */
    private static Channel openedBy(String offer) {
        Channel channel = DetachedChildren.create();
        TransportIdentityBinding.install(channel, TransportIdentityBinding.forOffer(retailCheck().check(offer)));
        return channel;
    }

    @Test
    void acceptsALoginSignedByTheKeyThatOpenedTheTransport() {
        Channel retail = openedBy(retailOffer());
        Channel education = openedBy(educationOffer());

        assertEquals(State.PENDING, TransportIdentityBinding.state(retail));
        assertEquals(Result.ACCEPTED, TransportIdentityBinding.verifyLogin(retail, CLIENT_KEY.getPublic()));
        assertEquals(Result.ACCEPTED, TransportIdentityBinding.verifyLogin(education, CLIENT_KEY.getPublic()),
                "a proven key binds the login whether or not the token was attested");
        assertEquals(State.ACCEPTED, TransportIdentityBinding.state(retail));
    }

    @Test
    void refusesALoginSignedByAnyOtherKey() {
        // A login captured from another player, replayed over a transport its key never opened
        Channel channel = openedBy(retailOffer());

        assertEquals(Result.MISMATCH, TransportIdentityBinding.verifyLogin(channel, keyPair("EC", "secp384r1").getPublic()));
        assertEquals(State.REJECTED, TransportIdentityBinding.state(channel));
    }

    @Test
    void spendsTheBindingOnTheFirstLoginItDecides() {
        Channel channel = openedBy(retailOffer());

        assertEquals(Result.ACCEPTED, TransportIdentityBinding.verifyLogin(channel, CLIENT_KEY.getPublic()));
        assertEquals(Result.SPENT, TransportIdentityBinding.verifyLogin(channel, CLIENT_KEY.getPublic()),
                "a second login on the same transport has nothing left to bind to");
        assertEquals(Result.SPENT, TransportIdentityBinding.acceptForwardedIdentity(channel));
    }

    @Test
    void tellsUnboundNetherNetFromOtherTransports() {
        Channel noKey = openedBy(offer(null, DIGEST));
        Channel neverInstalled = DetachedChildren.create();
        EmbeddedChannel raknet = new EmbeddedChannel();
        try {
            assertEquals(State.NONE, TransportIdentityBinding.state(noKey));
            assertEquals(Result.UNBOUND, TransportIdentityBinding.verifyLogin(noKey, CLIENT_KEY.getPublic()));
            assertEquals(Result.UNBOUND, TransportIdentityBinding.verifyLogin(neverInstalled, CLIENT_KEY.getPublic()),
                    "a NetherNet child that missed its binding must not pass as another transport");
            assertEquals(State.NOT_NETHERNET, TransportIdentityBinding.state(raknet));
            assertEquals(Result.NOT_NETHERNET, TransportIdentityBinding.verifyLogin(raknet, CLIENT_KEY.getPublic()));
            assertEquals(Result.NOT_NETHERNET, TransportIdentityBinding.acceptForwardedIdentity(raknet));
        } finally {
            raknet.finishAndReleaseAll();
        }
    }

    @Test
    void wrapperChannelsAreFoundByAttribute() {
        // Any channel that delegates attributes to the child sees its binding
        EmbeddedChannel wrapper = new EmbeddedChannel();
        try {
            TransportIdentityBinding.install(wrapper, TransportIdentityBinding.forKey(CLIENT_KEY.getPublic()));
            assertEquals(Result.MISMATCH, TransportIdentityBinding.verifyLogin(wrapper, keyPair("EC", "secp384r1").getPublic()));
        } finally {
            wrapper.finishAndReleaseAll();
        }
    }

    @Test
    void closingTheChannelClosesTheBinding() {
        EmbeddedChannel channel = new EmbeddedChannel();
        TransportIdentityBinding.install(channel, TransportIdentityBinding.forKey(CLIENT_KEY.getPublic()));
        channel.close();

        assertEquals(State.REJECTED, TransportIdentityBinding.state(channel));
        assertEquals(Result.SPENT, TransportIdentityBinding.verifyLogin(channel, CLIENT_KEY.getPublic()));
        channel.finishAndReleaseAll();
    }

    @Test
    void installsOnce() {
        Channel channel = openedBy(retailOffer());

        assertThrows(IllegalStateException.class,
                () -> TransportIdentityBinding.install(channel, TransportIdentityBinding.unbound()));
    }

    @Test
    void forwardedIdentitySpendsTheBinding() {
        Channel channel = openedBy(retailOffer());

        assertEquals(Result.ACCEPTED, TransportIdentityBinding.acceptForwardedIdentity(channel));
        assertEquals(Result.SPENT, TransportIdentityBinding.verifyLogin(channel, CLIENT_KEY.getPublic()));
    }

    @Test
    void lapsedAdmissionSpendsTheBinding() {
        // How NXS stateless admission plugs in: its own material, its own expiry
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            TransportIdentityBinding.install(channel, TransportIdentityBinding.of(new IdentityKeyVerifier() {
                @Override
                protected boolean usable() {
                    return false;
                }

                @Override
                protected boolean matches(byte[] canonicalKey) {
                    return true;
                }

                @Override
                protected void release() {
                }
            }));
            assertEquals(State.REJECTED, TransportIdentityBinding.state(channel));
            assertEquals(Result.SPENT, TransportIdentityBinding.verifyLogin(channel, CLIENT_KEY.getPublic()));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void comparesKeysCanonicallyWhateverTheirEncoding() {
        ECPublicKey original = (ECPublicKey) CLIENT_KEY.getPublic();
        ECPublicKey otherProvider = new ECPublicKey() {
            @Override public ECPoint getW() { return original.getW(); }
            @Override public ECParameterSpec getParams() { return original.getParams(); }
            @Override public String getAlgorithm() { return "EC"; }
            @Override public String getFormat() { return "provider-specific"; }
            @Override public byte[] getEncoded() { return null; }
        };
        KeyPair other = keyPair("EC", "secp384r1");

        assertEquals(Result.ACCEPTED, TransportIdentityBinding.verifyLogin(boundTo(original), otherProvider));
        assertEquals(Result.MISMATCH, TransportIdentityBinding.verifyLogin(boundTo(original), other.getPublic()));
        assertEquals(Result.MISMATCH, TransportIdentityBinding.verifyLogin(boundTo(original), null));
    }

    @Test
    void refusesKeysOffTheCurve() {
        ECPublicKey key = (ECPublicKey) CLIENT_KEY.getPublic();
        ECPublicKey offCurve = new ECPublicKey() {
            @Override public ECPoint getW() { return new ECPoint(BigInteger.ZERO, BigInteger.ZERO); }
            @Override public ECParameterSpec getParams() { return key.getParams(); }
            @Override public String getAlgorithm() { return "EC"; }
            @Override public String getFormat() { return "X.509"; }
            @Override public byte[] getEncoded() { return key.getEncoded(); }
        };

        assertThrows(IllegalArgumentException.class, () -> IdentityPublicKey.canonical(keyPair("EC", "secp256r1").getPublic()));
        assertThrows(IllegalArgumentException.class, () -> IdentityPublicKey.canonical(offCurve));
        assertEquals(Result.MISMATCH, TransportIdentityBinding.verifyLogin(boundTo(key), offCurve));
    }

    private static Channel boundTo(PublicKey key) {
        Channel channel = DetachedChildren.create();
        TransportIdentityBinding.install(channel, TransportIdentityBinding.forKey(key));
        return channel;
    }
}

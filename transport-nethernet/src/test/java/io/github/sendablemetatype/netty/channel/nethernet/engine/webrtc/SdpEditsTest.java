package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import io.github.sendablemetatype.netty.channel.nethernet.engine.IceCredentials;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SdpEditsTest {

    @Test
    void identityLinesAreTheOnlyOnesRemoved() {
        String offer = "v=0\r\n"
                + "o=- 1 2 IN IP4 127.0.0.1\r\n"
                + "a=fingerprint:sha-256 AA:BB\r\n"
                + "a=identity:eyJpZHAiOnsiZG9tYWluIjoiYXV0aC5taW5lY3JhZnQub3JnIn19\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=max-message-size:262144\r\n";
        String stripped = SdpEdits.withoutIdentity(offer);

        assertEquals("v=0\r\n"
                + "o=- 1 2 IN IP4 127.0.0.1\r\n"
                + "a=fingerprint:sha-256 AA:BB\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
                + "a=max-message-size:262144\r\n", stripped);
        assertEquals(stripped, SdpEdits.withoutIdentity(stripped));
    }

    @Test
    void credentialsReplaceEveryMediaSectionsOwn() {
        String sdp = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=ice-ufrag:abcd\r\na=ice-pwd:x\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=ice-ufrag:abcd\r\na=ice-pwd:x\r\n";

        String edited = SdpEdits.withCredentials(sdp, new IceCredentials("NXS1$ufrag", "pass$1\\word0000000000000"));

        assertEquals("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=ice-ufrag:NXS1$ufrag\r\n"
                + "a=ice-pwd:pass$1\\word0000000000000\r\n"
                + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=ice-ufrag:NXS1$ufrag\r\n"
                + "a=ice-pwd:pass$1\\word0000000000000\r\n", edited);
    }

    @Test
    void endOfCandidatesIsAppendedOnce() {
        assertEquals("v=0\r\na=end-of-candidates\r\n", SdpEdits.withEndOfCandidates("v=0\r\n"));
        assertEquals("v=0\r\na=end-of-candidates\r\n", SdpEdits.withEndOfCandidates("v=0"));
        assertEquals("v=0\r\na=end-of-candidates\r\n", SdpEdits.withEndOfCandidates("v=0\r\na=end-of-candidates\r\n"));
    }

    @Test
    void candidatesLoseTheirAttributePrefixAndLineBreak() {
        assertEquals("candidate:1 1 udp 1 192.0.2.1 5000 typ host",
                SdpEdits.candidateAttribute("a=candidate:1 1 udp 1 192.0.2.1 5000 typ host\r\n"));
        assertEquals("candidate:1 1 udp 1 192.0.2.1 5000 typ host",
                SdpEdits.candidateAttribute("candidate:1 1 udp 1 192.0.2.1 5000 typ host"));
    }
}

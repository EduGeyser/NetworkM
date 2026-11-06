package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Sample lines in the form libwebrtc hands to log sinks, from the sources at WebRTC main 885c77558b. */
class RoutineNativeLineTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "(dcsctp_transport.cc:561): DcSctpTransport3->OnAborted(error=PEER_REPORTED, message=User-Initiated Abort, reason=Close called).",
            // Thread and timestamp prefixes, which webrtc-java adds when its properties ask for them
            "[012:345][7932:signaling] (dcsctp_transport.cc:561): DcSctpTransport0->OnAborted(error=PEER_REPORTED, message=User-Initiated Abort, reason=Close called).",
            "(stun_port.cc:490): Port[1f2e3d4c:0:1:0:local:Net[eth0:2a01:4f8:c17:x:x:x:x:x/64:Ethernet:id=2]]: StunPort: stun host lookup received error 11004",
            "(turn_port.cc:943): Port[1f2e3d4c:0:1:0:relay:Net[Wi-Fi:2001:db8:1:x:x:x:x:x/64:Wifi:id=3]]: TURN host lookup received error 0",
            "(turn_port.cc:1736): Port[1f2e3d4c:0:1:0:relay:Net[eth0:192.168.1.x/24:Ethernet:id=1]]: Received TURN create permission error response, id=4a6f2c9d01b3e5f7a8c0d2e4, code=403, rtt=31",
            "(turn_port.cc:1933): Received TURN CreatePermission error response, code=403; pruned connection.",
    })
    void routineLinesMatch(String line) {
        assertTrue(RoutineNativeLine.matches(line), line);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // Other aborts: another cause from the peer, and a local failure
            "(dcsctp_transport.cc:561): DcSctpTransport3->OnAborted(error=PEER_REPORTED, message=Protocol Violation, additional_information=x).",
            "(dcsctp_transport.cc:561): DcSctpTransport3->OnAborted(error=TOO_MANY_RETRIES, message=Too many retransmissions).",
            // A lookup that fails on an IPv4 network, also behind a network name with a colon
            "(stun_port.cc:490): Port[1f2e3d4c:0:1:0:local:Net[eth0:192.168.1.x/24:Ethernet:id=1]]: StunPort: stun host lookup received error 11001",
            "(turn_port.cc:943): Port[1f2e3d4c:0:1:0:relay:Net[eth0:1:10.0.0.x/8:Ethernet:id=4]]: TURN host lookup received error 11001",
            // Permission errors other than 403
            "(turn_port.cc:1736): Port[1f2e3d4c:0:1:0:relay:Net[eth0:192.168.1.x/24:Ethernet:id=1]]: Received TURN create permission error response, id=4a6f2c9d01b3e5f7a8c0d2e4, code=508, rtt=31",
            "(turn_port.cc:1933): Received TURN CreatePermission error response, code=401; pruned connection.",
            // A routine message from another source file, and a line without a source prefix
            "(turn_port.cc:561): DcSctpTransport3->OnAborted(error=PEER_REPORTED, message=User-Initiated Abort, reason=Close called).",
            "DcSctpTransport3->OnAborted(error=PEER_REPORTED, message=User-Initiated Abort, reason=Close called).",
    })
    void otherLinesDoNotMatch(String line) {
        assertFalse(RoutineNativeLine.matches(line), line);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * libwebrtc lines that report routine NetherNet events at ERROR or WARNING.
 * {@link NativeLogBridge} logs them at DEBUG; every other line keeps its level.
 *
 * <p>A line matches by the source file named in the prefix libwebrtc writes
 * before each message, {@code (file.cc:line): }, and a fixed part of the
 * message. Line numbers move between WebRTC versions, so they are not part of
 * the match. Sources and line numbers below are from WebRTC main
 * {@code 885c77558b} (2026-11-06); the texts are the same in the branch
 * webrtc-java builds. When libwebrtc changes a text, its line stops matching
 * and logs at its own level again.
 */
enum RoutineNativeLine {
    /**
     * {@code media/sctp/dcsctp_transport.cc:561}, ERROR, for example
     * {@code DcSctpTransport0->OnAborted(error=PEER_REPORTED, message=User-Initiated Abort, reason=Close called).}
     * libwebrtc sends an SCTP ABORT with a user-initiated cause whenever its
     * side closes, so an ordinary client quit arrives as this abort (captured
     * from Education 26.33, 2026-11-06). Aborts of another kind or with
     * another first cause keep their level. The match is anchored at the start
     * of the message: libwebrtc copies a peer's error cause text into the
     * message, so an unanchored match would let a peer demote any abort.
     */
    PEER_CLOSED("dcsctp_transport.cc",
            Pattern.compile("^DcSctpTransport\\d*->OnAborted\\(error=PEER_REPORTED, message=User-Initiated Abort[,)]"),
            false),
    /**
     * {@code p2p/base/stun_port.cc:490}, WARNING, for example
     * {@code Port[...:Net[eth0:2a01:4f8:c17:x:x:x:x:x/64:Unknown:id=2]]: StunPort: stun host lookup received error <code>}.
     * libwebrtc looks the STUN host up for the address family of each local
     * network. On a dual-stack host the lookup for the IPv6 network fails
     * when the name has no IPv6 address, as Xbox's STUN host most likely has
     * none (observed on an Xbox join on a dual-stack host, 2026-11-06); the
     * IPv4 network still gathers its candidate. A failure on an IPv4 network
     * keeps its level, and so does one on an IPv6 network when the host has no
     * IPv4 network, since then nothing else gathers.
     */
    STUN_LOOKUP_FAILED_ON_IPV6("stun_port.cc",
            Pattern.compile(": StunPort: stun host lookup received error ", Pattern.LITERAL),
            true),
    /**
     * {@code p2p/base/turn_port.cc:943}, WARNING, for example
     * {@code Port[...:Net[eth0:2a01:4f8:c17:x:x:x:x:x/64:Unknown:id=2]]: TURN host lookup received error <code>}.
     * The same family-specific lookup as {@link #STUN_LOOKUP_FAILED_ON_IPV6},
     * for the TURN host, with the same rules for IPv4 and IPv6-only hosts.
     */
    TURN_LOOKUP_FAILED_ON_IPV6("turn_port.cc",
            Pattern.compile(": TURN host lookup received error ", Pattern.LITERAL),
            true),
    /**
     * {@code p2p/base/turn_port.cc:1736}, WARNING, for example
     * {@code Port[...]: Received TURN create permission error response, id=5a3c..., code=403, rtt=31}.
     * A TURN server answers 403 (Forbidden) when it refuses a permission for
     * a peer address, most likely a client's private host candidate (RFC 8656
     * lets servers refuse such addresses). libwebrtc drops that candidate pair
     * and the join goes on over the others (observed on an Xbox join,
     * 2026-11-06). Other error codes keep their level.
     */
    TURN_PERMISSION_REFUSED("turn_port.cc",
            Pattern.compile(": Received TURN create permission error response, id=[^,]*, code=403,"),
            false),
    /**
     * {@code p2p/base/turn_port.cc:1933}, ERROR:
     * {@code Received TURN CreatePermission error response, code=403; pruned connection.}
     * libwebrtc logs this right after {@link #TURN_PERMISSION_REFUSED} when it
     * drops the refused pair. Other error codes keep their level.
     */
    TURN_PERMISSION_PRUNED("turn_port.cc",
            Pattern.compile("^Received TURN CreatePermission error response, code=403; pruned connection"),
            false),
    /**
     * {@code pc/sdp_munging_detector.cc:54}, WARNING:
     * {@code SDP munging: ice-ufrag does not match last created description.}, or the same for
     * {@code ice-pwd} ({@code :59}). It is expected for host-chosen ICE credentials, which NXS
     * stateless and authenticated joins bring: the adapter sets them by editing the local
     * description before it applies it ({@code SdpEdits.withCredentials}), so every such join logs
     * one of these. Other kinds of munging keep their level.
     *
     * <p>The risk: libwebrtc accepts the edit only while field trial
     * {@code WebRTC-NoSdpMangleUfrag} is off and {@code WebRTC-NoSdpMangleReject} does not list
     * munging type 22 ({@code kIceUfrag}) or 21 ({@code kIcePwd}) ({@code DoSetLocalDescription},
     * {@code pc/sdp_offer_answer.cc:3074}). With either, every join with host-chosen credentials
     * fails its negotiation; with {@code WebRTC-NoSdpMangleUfragRestrictedAddresses} such a join
     * refuses remote candidates at the listed addresses. A host can turn them on through
     * {@link WebRtcEngine.Builder#fieldTrials}, and the trials exist to roll the rejection out,
     * so a later WebRTC may reject the edit by default.
     */
    HOST_CHOSEN_ICE_CREDENTIALS("sdp_munging_detector.cc",
            Pattern.compile("^SDP munging: ice-(?:ufrag|pwd) does not match last created description\\.$"),
            false);

    /** The prefix libwebrtc writes before a message: the source file name and line. */
    private static final Pattern SOURCE = Pattern.compile("\\(([\\w.+-]+):\\d+\\): ");
    /**
     * A local network in a port's description, {@code Net[<name>:<prefix>/<length>:...}.
     * libwebrtc shortens the prefix: IPv4 as {@code 192.168.1.x}, IPv6 as
     * {@code 2a01:4f8:c17:x:x:x:x:x}.
     */
    private static final Pattern NETWORK = Pattern.compile("Net\\[[^:\\]]*:([^/\\]]+)/\\d+:");
    private static final List<RoutineNativeLine> ALL = List.of(values());
    /** How long a check for an IPv4 network stays good; interfaces rarely change. */
    private static final long IPV4_CHECK_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static volatile long ipv4CheckedAt;
    private static volatile boolean hasIpv4Network;
    private static volatile boolean ipv4Checked;

    private final String file;
    private final Pattern message;
    private final boolean ipv6NetworkOnly;

    RoutineNativeLine(String file, Pattern message, boolean ipv6NetworkOnly) {
        this.file = file;
        this.message = message;
        this.ipv6NetworkOnly = ipv6NetworkOnly;
    }

    /**
     * Whether a line, as libwebrtc formats it for log sinks, is one of these.
     *
     * @param line the line, with the source prefix
     * @return whether it reports a routine event
     */
    static boolean matches(String line) {
        return matches(line, RoutineNativeLine::hostHasIpv4Network);
    }

    /**
     * @param line           the line, with the source prefix
     * @param hasIpv4Network whether the host has a usable IPv4 network, asked
     *                       only for a lookup that failed on an IPv6 network
     * @return whether it reports a routine event
     */
    static boolean matches(String line, BooleanSupplier hasIpv4Network) {
        Matcher source = SOURCE.matcher(line);
        if (!source.find()) {
            return false;
        }
        String file = source.group(1);
        String text = line.substring(source.end());
        for (RoutineNativeLine routine : ALL) {
            if (routine.file.equals(file) && routine.message.matcher(text).find()
                    && (!routine.ipv6NetworkOnly || (onIpv6Network(text) && hasIpv4Network.getAsBoolean()))) {
                return true;
            }
        }
        return false;
    }

    private static boolean onIpv6Network(String text) {
        Matcher network = NETWORK.matcher(text);
        if (!network.find()) {
            return false;
        }
        String prefix = network.group(1);
        return prefix.indexOf(':') >= 0 && prefix.indexOf('.') < 0;
    }

    /**
     * Whether an interface that is up has a non-loopback, non-link-local IPv4
     * address, checked at most once a minute. On failure the answer is no, so
     * the line keeps its level.
     */
    private static boolean hostHasIpv4Network() {
        long now = System.nanoTime();
        if (ipv4Checked && now - ipv4CheckedAt < IPV4_CHECK_NANOS) {
            return hasIpv4Network;
        }
        boolean found = false;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (!found && interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface network = interfaces.nextElement();
                if (!network.isUp() || network.isLoopback()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = network.getInetAddresses();
                while (!found && addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    found = address instanceof Inet4Address && !address.isLoopbackAddress()
                            && !address.isLinkLocalAddress();
                }
            }
        } catch (SocketException | RuntimeException e) {
            found = false;
        }
        hasIpv4Network = found;
        ipv4CheckedAt = now;
        ipv4Checked = true;
        return found;
    }
}

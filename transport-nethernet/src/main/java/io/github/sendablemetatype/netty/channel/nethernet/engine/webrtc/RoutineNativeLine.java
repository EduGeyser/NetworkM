package io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc;

import java.util.List;
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
     * another first cause keep their level.
     */
    PEER_CLOSED("dcsctp_transport.cc",
            Pattern.compile("->OnAborted(error=PEER_REPORTED, message=User-Initiated Abort", Pattern.LITERAL),
            false),
    /**
     * {@code p2p/base/stun_port.cc:490}, WARNING, for example
     * {@code Port[...:Net[eth0:2a01:4f8:c17:x:x:x:x:x/64:Unknown:id=2]]: StunPort: stun host lookup received error <code>}.
     * libwebrtc looks the STUN host up for the address family of each local
     * network. On a dual-stack host the lookup for the IPv6 network fails
     * when the name has no IPv6 address, as Xbox's STUN host most likely has
     * none (observed on an Xbox join on a dual-stack host, 2026-11-06); the
     * IPv4 network still gathers its candidate. A failure on an IPv4 network
     * keeps its level.
     */
    STUN_LOOKUP_FAILED_ON_IPV6("stun_port.cc",
            Pattern.compile(": StunPort: stun host lookup received error ", Pattern.LITERAL),
            true),
    /**
     * {@code p2p/base/turn_port.cc:943}, WARNING, for example
     * {@code Port[...:Net[eth0:2a01:4f8:c17:x:x:x:x:x/64:Unknown:id=2]]: TURN host lookup received error <code>}.
     * The same family-specific lookup as {@link #STUN_LOOKUP_FAILED_ON_IPV6},
     * for the TURN host. A failure on an IPv4 network keeps its level.
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
        Matcher source = SOURCE.matcher(line);
        if (!source.find()) {
            return false;
        }
        String file = source.group(1);
        String text = line.substring(source.end());
        for (RoutineNativeLine routine : ALL) {
            if (routine.file.equals(file) && routine.message.matcher(text).find()
                    && (!routine.ipv6NetworkOnly || onIpv6Network(text))) {
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
}

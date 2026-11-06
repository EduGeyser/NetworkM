package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import io.github.sendablemetatype.netty.channel.nethernet.signaling.EmbeddedHttp.Response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Speaks HTTP over a socket by hand, for tests that need control of the connection itself. */
final class SocketHttp {
    private SocketHttp() {
    }

    static Socket open(InetSocketAddress address) throws IOException {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        socket.setSoTimeout(10_000);
        return socket;
    }

    static void write(Socket socket, byte[]... parts) throws IOException {
        OutputStream out = socket.getOutputStream();
        for (byte[] part : parts) {
            out.write(part);
        }
        out.flush();
    }

    static void write(Socket socket, String text) throws IOException {
        write(socket, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Reads one response, or null when the server closed without one.
     */
    static Response read(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        String head = readHead(in);
        if (head == null) {
            return null;
        }
        String[] lines = head.split("\r\n");
        int status = Integer.parseInt(lines[0].split(" ")[1]);
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
        }
        byte[] body = in.readNBytes(Integer.parseInt(headers.getOrDefault("content-length", "0")));
        return new Response(status, headers, new String(body, StandardCharsets.UTF_8));
    }

    /** Whether the server closed the connection without sending anything more. */
    static boolean closedByServer(Socket socket) throws IOException {
        try {
            return socket.getInputStream().read() < 0;
        } catch (SocketException reset) {
            return true;
        }
    }

    private static String readHead(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        while (matched < 4) {
            int b;
            try {
                b = in.read();
            } catch (SocketException reset) {
                b = -1;
            }
            if (b < 0) {
                if (head.size() == 0) {
                    return null;
                }
                throw new IOException("the response ended in its head: " + head);
            }
            head.write(b);
            matched = (b == (matched % 2 == 0 ? '\r' : '\n')) ? matched + 1 : (b == '\r' ? 1 : 0);
        }
        String text = head.toString(StandardCharsets.ISO_8859_1);
        return text.substring(0, text.length() - 4);
    }

    static String get(String uri, String... headers) {
        return EmbeddedHttp.get(uri, headers);
    }

    static String post(String networkId, String body, String... headers) {
        return EmbeddedHttp.post(networkId, body, headers);
    }

    static byte[] proxyV1(String source, int port) {
        return ("PROXY TCP4 " + source + " 127.0.0.1 " + port + " 19132\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    /** A PROXY v2 header declaring an IPv4 source, from upstream's test helper. */
    static byte[] proxyV2(String source, int sourcePort) {
        byte[] signature = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
        byte[] out = new byte[signature.length + 4 + 12];
        System.arraycopy(signature, 0, out, 0, signature.length);
        out[12] = 0x21; // version 2, PROXY
        out[13] = 0x11; // TCP over IPv4
        out[14] = 0;
        out[15] = 12; // address block length
        String[] octets = source.split("\\.");
        for (int i = 0; i < 4; i++) {
            out[16 + i] = (byte) Integer.parseInt(octets[i]);
        }
        out[20] = 127;
        out[21] = 0;
        out[22] = 0;
        out[23] = 1;
        out[24] = (byte) (sourcePort >> 8);
        out[25] = (byte) sourcePort;
        out[26] = (byte) (19132 >> 8);
        out[27] = (byte) 19132;
        return out;
    }
}

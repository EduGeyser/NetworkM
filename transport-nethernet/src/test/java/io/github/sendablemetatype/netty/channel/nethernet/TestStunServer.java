package io.github.sendablemetatype.netty.channel.nethernet;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Answers STUN binding requests over IPv4 with the XOR-MAPPED-ADDRESS it is told to report, as a STUN server behind
 * a NAT would, or with the requester's own address. Public so the tests of modules built on the transport can use it.
 */
public final class TestStunServer implements AutoCloseable {
    private static final int MAGIC_COOKIE = 0x2112A442;

    private final DatagramSocket socket;
    private final InetSocketAddress reported;
    private final AtomicInteger answered = new AtomicInteger();

    /**
     * @param bind     the address to listen on
     * @param reported the mapping to report, or null for the requester's own address
     */
    public TestStunServer(InetAddress bind, InetSocketAddress reported) throws IOException {
        if (reported != null && !(reported.getAddress() instanceof Inet4Address)) {
            throw new IllegalArgumentException("An IPv4 mapping only");
        }
        this.socket = new DatagramSocket(new InetSocketAddress(bind, 0));
        this.reported = reported;
        Thread.ofPlatform().daemon().name("test-stun-server").start(this::serve);
    }

    public InetSocketAddress address() {
        return (InetSocketAddress) socket.getLocalSocketAddress();
    }

    /** How many binding requests it answered. */
    public int answered() {
        return answered.get();
    }

    @Override
    public void close() {
        socket.close();
    }

    private void serve() {
        byte[] buffer = new byte[2048];
        while (!socket.isClosed()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                ByteBuffer request = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
                if (packet.getLength() < 20 || request.getShort(0) != 0x0001 || request.getInt(4) != MAGIC_COOKIE) {
                    continue;
                }
                InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
                InetSocketAddress mapped = reported != null ? reported : source;
                // A binding success response with one XOR-MAPPED-ADDRESS attribute
                ByteBuffer response = ByteBuffer.allocate(32);
                response.putShort((short) 0x0101).putShort((short) 12).putInt(MAGIC_COOKIE);
                response.put(packet.getData(), 8, 12);
                response.putShort((short) 0x0020).putShort((short) 8);
                response.put((byte) 0).put((byte) 0x01);
                response.putShort((short) (mapped.getPort() ^ (MAGIC_COOKIE >>> 16)));
                response.putInt(ByteBuffer.wrap(mapped.getAddress().getAddress()).getInt() ^ MAGIC_COOKIE);
                socket.send(new DatagramPacket(response.array(), response.capacity(), source));
                answered.incrementAndGet();
            } catch (IOException e) {
                return;
            }
        }
    }
}

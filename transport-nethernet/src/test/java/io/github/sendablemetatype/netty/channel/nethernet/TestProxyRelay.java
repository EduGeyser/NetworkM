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

package io.github.sendablemetatype.netty.channel.nethernet;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

/**
 * A UDP relay in front of a server's ICE port that speaks PROXY protocol version 2, as DDoS-filtering
 * edges do: each client gets a source port of its own toward the server, every datagram carries a
 * header declaring the client, and the server's replies on that port go back to the client.
 * IPv4 only. Ported from upstream's {@code RelayedJoinTest.Relay}.
 */
final class TestProxyRelay implements AutoCloseable {
    private static final byte[] SIGNATURE = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

    /** One client's port toward the server, and where its replies go. */
    static final class Flow {
        final DatagramSocket socket;
        volatile InetSocketAddress client;
        volatile InetSocketAddress declared;

        private Flow(DatagramSocket socket, InetSocketAddress client, InetSocketAddress declared) {
            this.socket = socket;
            this.client = client;
            this.declared = declared;
        }

        /** The relay port as the server sees it. */
        InetSocketAddress port() {
            return (InetSocketAddress) socket.getLocalSocketAddress();
        }
    }

    private final DatagramSocket front;
    private final UnaryOperator<InetSocketAddress> declare;
    /** By the client's real address. */
    private final Map<InetSocketAddress, Flow> flows = new ConcurrentHashMap<>();
    /** Clients the relay no longer forwards for. */
    private final Set<InetSocketAddress> dropped = ConcurrentHashMap.newKeySet();
    private volatile InetSocketAddress upstream;
    private volatile Flow handOver;
    final AtomicInteger up = new AtomicInteger();
    final AtomicInteger down = new AtomicInteger();

    /**
     * @param address where the relay listens for clients, and the address its ports toward the server use
     * @param declare the client a header declares for a client's real address
     */
    TestProxyRelay(InetAddress address, UnaryOperator<InetSocketAddress> declare) throws IOException {
        this.front = new DatagramSocket(new InetSocketAddress(address, 0));
        this.declare = declare;
        daemon(this::forward);
    }

    /** Where clients send to. */
    InetSocketAddress address() {
        return (InetSocketAddress) front.getLocalSocketAddress();
    }

    /** Sets the server's ICE port; datagrams before that are dropped. */
    void forwardTo(InetSocketAddress upstream) {
        this.upstream = upstream;
    }

    /** The flow whose header declares this client, or null. */
    Flow flowDeclaring(InetSocketAddress declared) {
        return flows.values().stream().filter(flow -> flow.declared.equals(declared)).findFirst().orElse(null);
    }

    /**
     * Gives the next new client the port of an existing flow, as a relay reuses a source port once it
     * considers the old client gone: the old client is no longer forwarded, the port declares the new
     * client, and the server's replies on it go to the new client.
     */
    void handOverNextClient(Flow flow) {
        dropped.add(flow.client);
        flows.remove(flow.client);
        this.handOver = flow;
    }

    private void forward() {
        byte[] buffer = new byte[65535];
        try {
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                front.receive(packet);
                InetSocketAddress client = (InetSocketAddress) packet.getSocketAddress();
                InetSocketAddress server = upstream;
                if (server == null || dropped.contains(client)) {
                    continue;
                }
                Flow flow = flows.computeIfAbsent(client, this::open);
                InetSocketAddress declared = flow.declared;
                InetSocketAddress destination = address();
                ByteBuffer out = ByteBuffer.allocate(SIGNATURE.length + 16 + packet.getLength());
                // Version 2, PROXY command; IPv4 over UDP; 12 address bytes
                out.put(SIGNATURE).put((byte) 0x21).put((byte) 0x12).putShort((short) 12);
                out.put(ipv4(declared.getAddress())).put(ipv4(destination.getAddress()));
                out.putShort((short) declared.getPort()).putShort((short) destination.getPort());
                out.put(buffer, 0, packet.getLength());
                flow.socket.send(new DatagramPacket(out.array(), out.position(), server));
                up.incrementAndGet();
            }
        } catch (IOException closed) {
            // The relay closed
        }
    }

    private Flow open(InetSocketAddress client) {
        Flow taken = handOver;
        if (taken != null) {
            handOver = null;
            taken.client = client;
            taken.declared = declare.apply(client);
            return taken;
        }
        try {
            Flow flow = new Flow(new DatagramSocket(new InetSocketAddress(address().getAddress(), 0)), client,
                    declare.apply(client));
            daemon(() -> back(flow));
            return flow;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void back(Flow flow) {
        byte[] buffer = new byte[65535];
        try {
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                flow.socket.receive(packet);
                front.send(new DatagramPacket(buffer, packet.getLength(), flow.client));
                down.incrementAndGet();
            }
        } catch (IOException closed) {
            // The relay closed
        }
    }

    private static byte[] ipv4(InetAddress address) {
        if (!(address instanceof Inet4Address)) {
            throw new IllegalArgumentException("The test relay speaks IPv4 only: " + address);
        }
        return address.getAddress();
    }

    private static void daemon(Runnable task) {
        Thread thread = new Thread(task, "proxy-relay");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void close() {
        front.close();
        flows.values().forEach(flow -> flow.socket.close());
        Flow pending = handOver;
        if (pending != null) {
            pending.socket.close();
        }
    }
}

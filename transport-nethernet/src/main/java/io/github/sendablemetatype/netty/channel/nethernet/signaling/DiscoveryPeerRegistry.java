package io.github.sendablemetatype.netty.channel.nethernet.signaling;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class DiscoveryPeerRegistry {
    private final int maxPassive;
    private final long idleNanos;
    private final LongSupplier clock;
    private final Map<Long, Peer> peers = new HashMap<>();
    private final LinkedHashMap<Long, Peer> passive = new LinkedHashMap<>(16, 0.75f, true);
    private final Map<Long, Peer> registeredConnections = new HashMap<>();
    private boolean passiveExpiryScheduled;
    private long nextPassiveExpiry;

    DiscoveryPeerRegistry() {
        this(1024, TimeUnit.MINUTES.toNanos(5), System::nanoTime);
    }

    DiscoveryPeerRegistry(int maxPassive, long idleNanos, LongSupplier clock) {
        if (maxPassive <= 0 || idleNanos <= 0) {
            throw new IllegalArgumentException("Peer capacity and idle timeout must be positive");
        }
        this.maxPassive = maxPassive;
        this.idleNanos = idleNanos;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    synchronized void remember(long peerId, InetSocketAddress address) {
        long now = clock.getAsLong();
        expire(now);
        remember(peerId, address, now);
        trimPassive();
    }

    /**
     * A signal for a connection this side registered a handler for: the peer that sent it stays
     * in the table while the connection is registered.
     */
    synchronized void rememberSignal(long connectionId, long peerId, InetSocketAddress address) {
        long now = clock.getAsLong();
        expire(now);
        Peer peer = remember(peerId, address, now);
        if (registeredConnections.containsKey(connectionId)) {
            Peer previous = registeredConnections.put(connectionId, peer);
            if (previous != peer) {
                if (previous != null) {
                    unpin(previous, now);
                }
                pin(peer);
            }
        }
        trimPassive();
    }

    synchronized void register(long connectionId) {
        expire(clock.getAsLong());
        if (!registeredConnections.containsKey(connectionId)) {
            registeredConnections.put(connectionId, null);
        }
    }

    synchronized void unregister(long connectionId) {
        long now = clock.getAsLong();
        expire(now);
        Peer peer = registeredConnections.remove(connectionId);
        if (peer != null) {
            unpin(peer, now);
            trimPassive();
        }
    }

    /** Keeps a peer in the table, at this address, until a matching {@link #release}: one join from it. */
    synchronized void hold(long peerId, InetSocketAddress address) {
        long now = clock.getAsLong();
        expire(now);
        pin(remember(peerId, address, now));
    }

    /** Ends one {@link #hold} of the peer. */
    synchronized void release(long peerId) {
        long now = clock.getAsLong();
        expire(now);
        Peer peer = peers.get(peerId);
        if (peer != null && peer.pins > 0) {
            unpin(peer, now);
            trimPassive();
        }
    }

    synchronized InetSocketAddress get(long peerId) {
        long now = clock.getAsLong();
        expire(now);
        Peer peer = peers.get(peerId);
        if (peer != null) {
            peer.lastUsed = now;
            if (peer.pins == 0) {
                passive.get(peerId);
            }
        }
        return peer == null ? null : peer.address;
    }

    synchronized void clear() {
        registeredConnections.clear();
        passive.clear();
        peers.clear();
        passiveExpiryScheduled = false;
    }

    synchronized int passiveSize() {
        expire(clock.getAsLong());
        return passive.size();
    }

    private Peer remember(long id, InetSocketAddress address, long now) {
        Peer peer = peers.computeIfAbsent(id, Peer::new);
        peer.address = address;
        peer.lastUsed = now;
        if (peer.pins == 0) {
            passive.put(id, peer);
            considerExpiry(peer.lastUsed);
        }
        return peer;
    }

    private void pin(Peer peer) {
        peer.pins++;
        passive.remove(peer.id);
    }

    private void unpin(Peer peer, long now) {
        if (--peer.pins == 0) {
            if (now - peer.lastUsed >= idleNanos) {
                peers.remove(peer.id);
            } else {
                passive.put(peer.id, peer);
                considerExpiry(peer.lastUsed);
            }
        }
    }

    private void expire(long now) {
        if (passiveExpiryScheduled && now - nextPassiveExpiry >= 0) {
            passiveExpiryScheduled = false;
            Iterator<Peer> iterator = passive.values().iterator();
            while (iterator.hasNext()) {
                Peer peer = iterator.next();
                if (now - peer.lastUsed >= idleNanos) {
                    iterator.remove();
                    peers.remove(peer.id);
                } else {
                    considerExpiry(peer.lastUsed);
                }
            }
        }
    }

    private void considerExpiry(long lastUsed) {
        long expiry = lastUsed + idleNanos;
        if (!passiveExpiryScheduled || expiry - nextPassiveExpiry < 0) {
            nextPassiveExpiry = expiry;
            passiveExpiryScheduled = true;
        }
    }

    private void trimPassive() {
        while (passive.size() > maxPassive) {
            peers.remove(passive.pollFirstEntry().getKey());
        }
    }

    private static final class Peer {
        private final long id;
        private InetSocketAddress address;
        private long lastUsed;
        private int pins;

        private Peer(long id) {
            this.id = id;
        }
    }
}

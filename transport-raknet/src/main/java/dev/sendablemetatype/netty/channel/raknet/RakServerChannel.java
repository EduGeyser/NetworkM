/*
 * Copyright 2022 CloudburstMC
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

package dev.sendablemetatype.netty.channel.raknet;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import dev.sendablemetatype.netty.channel.proxy.ProxyChannel;
import dev.sendablemetatype.netty.channel.raknet.config.DefaultRakServerConfig;
import dev.sendablemetatype.netty.channel.raknet.config.DefaultRakServerThrottle;
import dev.sendablemetatype.netty.channel.raknet.config.RakChannelOption;
import dev.sendablemetatype.netty.channel.raknet.config.RakServerChannelConfig;
import dev.sendablemetatype.netty.channel.raknet.config.RakServerCookieMode;
import dev.sendablemetatype.netty.channel.raknet.config.RakServerMetrics;
import dev.sendablemetatype.netty.channel.raknet.config.RakServerThrottle;
import dev.sendablemetatype.netty.handler.codec.raknet.common.UnconnectedPongEncoder;
import dev.sendablemetatype.netty.handler.codec.raknet.server.RakProxyServerHandler;
import dev.sendablemetatype.netty.handler.codec.raknet.server.RakServerOfflineHandler;
import dev.sendablemetatype.netty.handler.codec.raknet.server.RakServerRateLimiter;
import dev.sendablemetatype.netty.handler.codec.raknet.server.RakServerRouteHandler;
import dev.sendablemetatype.netty.handler.codec.raknet.server.RakServerTailHandler;
import dev.sendablemetatype.netty.util.RakUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RakServerChannel extends ProxyChannel<DatagramChannel> implements ServerChannel {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(RakServerChannel.class);
    private static final AttributeKey<ThrottleReservation> THROTTLE_RESERVATION =
            AttributeKey.valueOf(RakServerChannel.class, "throttleReservation");

    private final RakServerChannelConfig config;
    private final Map<SocketAddress, RakChildChannel> childChannelMap = new ConcurrentHashMap<>();
    private final Consumer<RakChannel> childConsumer;

    private boolean pipelineInitialized;
    private ExpiringMap<InetSocketAddress, InetSocketAddress> clientAddresses = null;

    public RakServerChannel(DatagramChannel channel) {
        this(channel, null);
    }

    public RakServerChannel(DatagramChannel channel, Consumer<RakChannel> childConsumer) {
        super(channel);
        this.childConsumer = childConsumer;
        this.config = new DefaultRakServerConfig(this);

        channel.closeFuture().addListener(future -> {
            if (!future.isSuccess()) {
                log.warn("RakServerChannel closed unsuccessfully", future.cause());
            } else if (future.cause() != null) {
                log.warn("RakServerChannel closed with cause", future.cause());
            }
        });
    }

    @Override
    public ChannelPipeline pipeline() {
        if (!this.pipelineInitialized) {
            this.pipelineInitialized = true;
            initPipeline();
        }
        return super.pipeline();
    }

    protected void initPipeline() {
        this.clientAddresses = this.config().getProxyProtocol()
                ? ExpiringMap.builder()
                  .expiration(RakConstants.SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .expirationPolicy(ExpirationPolicy.ACCESSED).build()
                : null;
        if (this.config().getProxyProtocol()) {
            this.pipeline().addLast(RakProxyServerHandler.NAME, new RakProxyServerHandler(this));
        }
        this.pipeline().addLast(UnconnectedPongEncoder.NAME, UnconnectedPongEncoder.INSTANCE);
        if (this.config().getPacketLimit() > 0) { // No point in enabling this.
            this.pipeline().addLast(RakServerRateLimiter.NAME, new RakServerRateLimiter(this));
        }
        this.pipeline().addLast(RakServerOfflineHandler.NAME, new RakServerOfflineHandler(this));
        this.pipeline().addLast(RakServerRouteHandler.NAME, new RakServerRouteHandler(this));
        this.pipeline().addLast(RakServerTailHandler.NAME, RakServerTailHandler.INSTANCE);
    }

    /**
     * Create new child channel assigned to remote address.
     *
     * @param address         remote address of new connection.
     * @param protocolVersion RakNet protocol version from the handshake cookie, or 0 if not available.
     * @return RakChildChannel instance of new channel, or {@code null} if a non-replaceable channel already exists.
     */
    public RakChildChannel createChildChannel(InetSocketAddress address, InetSocketAddress localAddress, long clientGuid, int mtu, int protocolVersion) {
        RakChildChannel existingChannel = this.childChannelMap.get(address);
        if (existingChannel != null && (this.config().getCookieMode() == RakServerCookieMode.INVALID
                || this.config().getCookieMode() == RakServerCookieMode.OFF)) {
            // Could be spoofed, so we don't close the existing channel.
            return null;
        }

        InetSocketAddress clientAddress = this.getClientAddress(address);
        RakServerThrottle throttle = this.config().getThrottle();
        ThrottleReservation previous = existingChannel == null ? null : existingChannel.attr(THROTTLE_RESERVATION).get();
        ThrottleReservation reservation = null;
        boolean replacingReservation = false;
        if (previous != null && throttle != null && throttle.getClass() == DefaultRakServerThrottle.class) {
            ReplacementAdmission admission = previous.beginReplacement(throttle, clientAddress);
            if (admission == ReplacementAdmission.REJECTED) {
                return null;
            }
            if (admission == ReplacementAdmission.ACCEPTED) {
                reservation = previous;
                replacingReservation = true;
            }
        }
        if (!replacingReservation && throttle != null) {
            if (!throttle.accept(clientAddress)) {
                return null;
            }
            reservation = new ThrottleReservation(throttle, clientAddress);
        }

        RakChildChannel channel;
        try {
            channel = new RakChildChannel(address, localAddress, clientAddress, this, clientGuid, mtu, childConsumer);
        } catch (RuntimeException | Error cause) {
            if (reservation != null) {
                if (replacingReservation) {
                    reservation.cancelReplacement();
                } else {
                    reservation.release(null);
                }
            }
            throw cause;
        }
        if (reservation != null) {
            reservation.attach(channel);
            channel.attr(THROTTLE_RESERVATION).set(reservation);
        }
        ThrottleReservation acceptedReservation = reservation;
        channel.closeFuture().addListener((GenericFutureListener<ChannelFuture>) future -> this.onChildClosed(future, acceptedReservation));
        // Set before fireChannelRead because initChannel runs async on the child worker thread.
        if (protocolVersion != 0) {
            channel.config().setOption(RakChannelOption.RAK_PROTOCOL_VERSION, protocolVersion);
        }
        // Publish before registration, since a child initializer may close the channel immediately.
        this.childChannelMap.put(address, channel);

        RakServerMetrics metrics = this.config().getMetrics();
        if (metrics != null) {
            try {
                metrics.channelOpen(clientAddress);
            } catch (RuntimeException cause) {
                log.warn("Failed to report channel open for {}", clientAddress, cause);
            }
        }
        if (existingChannel != null) {
            existingChannel.close();
        }
        this.pipeline().fireChannelRead(channel).fireChannelReadComplete();
        return channel;
    }

    public RakChildChannel getChildChannel(SocketAddress address) {
        return this.childChannelMap.get(address);
    }

    private void onChildClosed(ChannelFuture channelFuture, ThrottleReservation reservation) {
        RakChildChannel channel = (RakChildChannel) channelFuture.channel();
        this.childChannelMap.remove(channel.remoteOrProxyAddress(), channel);

        try {
            RakServerMetrics metrics = this.config().getMetrics();
            if (metrics != null) {
                try {
                    metrics.channelClose(channel.remoteAddress());
                } catch (RuntimeException cause) {
                    log.warn("Failed to report channel close for {}", channel.remoteAddress(), cause);
                }
            }

            channel.rakPipeline().fireChannelInactive();
            channel.rakPipeline().fireChannelUnregistered();
            // Need to use reflection to destroy pipeline because
            // DefaultChannelPipeline.destroy() is only called when channel.isOpen() is false,
            // but the method is called on parent channel, and there is no other way to destroy pipeline.
            RakUtils.destroyChannelPipeline(channel.rakPipeline());
        } finally {
            if (reservation != null) {
                reservation.release(channel);
            }
        }
    }

    private enum ReplacementAdmission {
        UNAVAILABLE, ACCEPTED, REJECTED
    }

    private static final class ThrottleReservation {
        private final RakServerThrottle throttle;
        private final InetSocketAddress address;
        private RakChildChannel owner;
        private boolean replacing;
        private boolean ownerClosed;
        private boolean released;

        private ThrottleReservation(RakServerThrottle throttle, InetSocketAddress address) {
            this.throttle = throttle;
            this.address = address;
        }

        private synchronized ReplacementAdmission beginReplacement(RakServerThrottle throttle, InetSocketAddress address) {
            if (this.released || this.throttle != throttle || !this.address.getAddress().equals(address.getAddress())) {
                return ReplacementAdmission.UNAVAILABLE;
            }
            if (this.replacing || !((DefaultRakServerThrottle) throttle).acceptReplacement(address)) {
                return ReplacementAdmission.REJECTED;
            }
            // Keep the slot reserved if the old child closes while its replacement is being constructed.
            this.replacing = true;
            return ReplacementAdmission.ACCEPTED;
        }

        private synchronized void attach(RakChildChannel channel) {
            this.owner = channel;
            this.ownerClosed = false;
            this.replacing = false;
        }

        private void cancelReplacement() {
            boolean release;
            synchronized (this) {
                this.replacing = false;
                release = this.ownerClosed && !this.released;
                this.released |= release;
            }
            if (release) {
                this.throttle.closed(this.address);
            }
        }

        private void release(RakChildChannel channel) {
            boolean release;
            synchronized (this) {
                if (this.released || this.owner != channel) {
                    return;
                }
                this.ownerClosed = true;
                release = !this.replacing;
                this.released = release;
            }
            if (release) {
                this.throttle.closed(this.address);
            }
        }
    }

    @Override
    public void onCloseTriggered(ChannelPromise promise) {
        if (log.isTraceEnabled()) {
            log.trace("Closing RakServerChannel: {}", Thread.currentThread().getName(), new Throwable());
        }
        PromiseCombiner combiner = new PromiseCombiner(this.eventLoop());
        this.childChannelMap.values().forEach(channel -> combiner.add(channel.close()));

        ChannelPromise combinedPromise = this.newPromise();
        combinedPromise.addListener(future -> super.onCloseTriggered(promise));
        combiner.finish(combinedPromise);
    }

    public boolean tryBlockAddress(InetSocketAddress address, long time, TimeUnit unit) {
        RakServerRateLimiter rateLimiter = this.pipeline().get(RakServerRateLimiter.class);
        if (rateLimiter != null) {
            return rateLimiter.blockAddress(address, time, unit);
        }
        return false;
    }

    @Override
    public RakServerChannelConfig config() {
        return this.config;
    }

    public InetSocketAddress getClientAddress(InetSocketAddress address) {
        return this.clientAddresses != null ? this.clientAddresses.get(address) : address;
    }

    public void setClientAddress(InetSocketAddress address, InetSocketAddress clientAddress) {
        this.clientAddresses.put(address, clientAddress);
    }
}

package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetClientSignaling;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetServerSignaling;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;

import java.util.function.Supplier;

/**
 * Netty channel factories for NetherNet. Engine ownership is explicit: a
 * channel given an engine never closes it, and a channel given an engine
 * supplier owns and closes the engine it creates, one per channel.
 */
public class NetherNetChannelFactory<T extends Channel> implements ChannelFactory<T> {

    private final Supplier<T> channelCreator;

    private NetherNetChannelFactory(Supplier<T> channelCreator) {
        this.channelCreator = channelCreator;
    }

    @Override
    public T newChannel() {
        return channelCreator.get();
    }

    /**
     * Server channels that each create the default engine once their signaling
     * bound, and close it with themselves.
     *
     * @param signaling the signaling the joins arrive over
     * @return a ChannelFactory for NetherNetServerChannel
     */
    public static ChannelFactory<NetherNetServerChannel> server(NetherNetServerSignaling signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetServerChannel(signaling));
    }

    /**
     * Server channels on the caller's engine. The channels never close it, so
     * it can serve several binds; its owner closes it after the last.
     *
     * @param engine the engine
     * @param signaling the signaling the joins arrive over
     * @return a ChannelFactory for NetherNetServerChannel
     */
    public static ChannelFactory<NetherNetServerChannel> server(NetherNetEngine engine, NetherNetServerSignaling signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetServerChannel(engine, signaling));
    }

    /**
     * Server channels that each own the engine the supplier returns, created
     * once their signaling bound, so a bind that fails creates no engine
     * state. Each channel closes its engine with itself.
     *
     * @param engineSupplier creates one engine per channel
     * @param signaling the signaling the joins arrive over
     * @return a ChannelFactory for NetherNetServerChannel
     */
    public static ChannelFactory<NetherNetServerChannel> server(Supplier<? extends NetherNetEngine> engineSupplier,
                                                                NetherNetServerSignaling signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetServerChannel(engineSupplier, signaling));
    }

    /**
     * Client channels on the caller's engine, which they never close.
     *
     * @param engine the engine
     * @param signaling the signaling to connect over; each channel closes it
     * @return a ChannelFactory for NetherNetClientChannel
     */
    public static ChannelFactory<NetherNetClientChannel> client(NetherNetEngine engine, NetherNetClientSignaling signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetClientChannel(engine, signaling));
    }

    /**
     * Client channels that each own the engine the supplier returns, created
     * when the first attempt needs it and closed with the channel.
     *
     * @param engineSupplier creates one engine per channel
     * @param signaling the signaling to connect over; each channel closes it
     * @return a ChannelFactory for NetherNetClientChannel
     */
    public static ChannelFactory<NetherNetClientChannel> client(Supplier<? extends NetherNetEngine> engineSupplier,
                                                                NetherNetClientSignaling signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetClientChannel(engineSupplier, signaling));
    }
}

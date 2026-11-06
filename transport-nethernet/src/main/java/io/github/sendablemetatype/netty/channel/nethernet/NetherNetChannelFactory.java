package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.NetherNetEngine;
import io.github.sendablemetatype.netty.channel.nethernet.signaling.NetherNetClientSignaling;
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
     * Server endpoints that each create the default engine when they bind, and
     * close it with themselves. Add join sources to each endpoint with
     * {@link NetherNetServerChannel#addSource}.
     *
     * @return a ChannelFactory for NetherNetServerChannel
     */
    public static ChannelFactory<NetherNetServerChannel> server() {
        return new NetherNetChannelFactory<>(NetherNetServerChannel::new);
    }

    /**
     * Server endpoints on the caller's engine. The endpoints never close it, so
     * it can serve several binds; its owner closes it after the last.
     *
     * @param engine the engine
     * @return a ChannelFactory for NetherNetServerChannel
     */
    public static ChannelFactory<NetherNetServerChannel> server(NetherNetEngine engine) {
        return new NetherNetChannelFactory<>(() -> new NetherNetServerChannel(engine));
    }

    /**
     * Server endpoints that each own the engine the supplier returns, created
     * when they bind and closed with them.
     *
     * @param engineSupplier creates one engine per endpoint
     * @return a ChannelFactory for NetherNetServerChannel
     */
    public static ChannelFactory<NetherNetServerChannel> server(Supplier<? extends NetherNetEngine> engineSupplier) {
        return new NetherNetChannelFactory<>(() -> new NetherNetServerChannel(engineSupplier));
    }

    /**
     * Client channels on the caller's engine, which they never close.
     *
     * @param engine the engine
     * @param signaling creates the signaling for each channel, which owns and closes it
     * @return a ChannelFactory for NetherNetClientChannel
     */
    public static ChannelFactory<NetherNetClientChannel> client(NetherNetEngine engine,
                                                                Supplier<? extends NetherNetClientSignaling> signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetClientChannel(engine, signaling.get()));
    }

    /**
     * Client channels that each own the engine the supplier returns, created
     * when the first attempt needs it and closed with the channel.
     *
     * @param engineSupplier creates one engine per channel
     * @param signaling creates the signaling for each channel, which owns and closes it
     * @return a ChannelFactory for NetherNetClientChannel
     */
    public static ChannelFactory<NetherNetClientChannel> client(Supplier<? extends NetherNetEngine> engineSupplier,
                                                                Supplier<? extends NetherNetClientSignaling> signaling) {
        return new NetherNetChannelFactory<>(() -> new NetherNetClientChannel(engineSupplier, signaling.get()));
    }
}

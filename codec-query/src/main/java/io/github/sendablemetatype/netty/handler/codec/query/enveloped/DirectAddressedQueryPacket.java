package io.github.sendablemetatype.netty.handler.codec.query.enveloped;

import io.netty.channel.DefaultAddressedEnvelope;
import io.github.sendablemetatype.netty.handler.codec.query.QueryPacket;

import java.net.InetSocketAddress;

public class DirectAddressedQueryPacket extends DefaultAddressedEnvelope<QueryPacket, InetSocketAddress> {
    public DirectAddressedQueryPacket(QueryPacket message, InetSocketAddress recipient, InetSocketAddress sender) {
        super(message, recipient, sender);
    }

    public DirectAddressedQueryPacket(QueryPacket message, InetSocketAddress recipient) {
        super(message, recipient);
    }
}

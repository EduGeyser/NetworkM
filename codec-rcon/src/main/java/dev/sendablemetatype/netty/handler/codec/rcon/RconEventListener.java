package dev.sendablemetatype.netty.handler.codec.rcon;

public interface RconEventListener {

    String onMessage(String message);
}

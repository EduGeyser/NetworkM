/*
 * Copyright 2026 CloudburstMC
 * Licensed under the Apache License, Version 2.0.
 */

package io.github.sendablemetatype.netty.channel.raknet.config;

/**
 * Notifies the RakNet pipeline of settings that affect an already active session.
 * Pipeline delivery applies the change on the transport event loop, including when
 * ServerBootstrap installs child options after the session has become active.
 */
public enum RakSessionConfigUpdate {
    SESSION_TIMEOUT,
    AUTO_FLUSH,
    QUEUE_LIMITS
}

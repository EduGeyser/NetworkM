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

import io.github.sendablemetatype.netty.channel.nethernet.config.NetherChannelOption;
import io.github.sendablemetatype.netty.util.address.ProxyList;
import io.github.sendablemetatype.netty.util.address.TrustedProxies;

import java.io.Serial;

/**
 * Fails a server channel's bind, before it touches a socket, when the PROXY protocol list of
 * {@link NetherChannelOption#NETHER_SERVER_TRUSTED_PROXIES}, which names the relays in front of the
 * shared ICE port, named entries and none of them could be read ({@link ProxyList#allUnreadable()}).
 * Bound without its relays, the endpoint would take players directly and announce this host's
 * addresses, which a setup behind relays hides. A list from which some relays could be read binds
 * with those.
 * <p>
 * A host can catch this to report it under the name of its own setting. To bind without relays on
 * purpose, set the option to a value whose PROXY protocol list is {@link ProxyList#NONE}, such as
 * {@link TrustedProxies#NONE}.
 */
public final class UnreadableRelaysException extends IllegalStateException {
    @Serial
    private static final long serialVersionUID = 1L;

    UnreadableRelaysException() {
        super("The PROXY protocol relay list (NETHER_SERVER_TRUSTED_PROXIES) names relays, none of which could be "
                + "read (each was logged when the list was parsed), and without them players would reach this "
                + "server directly. Fix the list, or bind without relays on purpose with a PROXY protocol list of "
                + "ProxyList.NONE.");
    }
}

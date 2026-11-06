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

package io.github.sendablemetatype.netty.channel.raknet.config;

import io.github.sendablemetatype.netty.channel.raknet.RakChildChannel;
import io.github.sendablemetatype.netty.channel.raknet.RakServerChannel;
import io.github.sendablemetatype.netty.channel.raknet.RakState;
import io.github.sendablemetatype.netty.util.MetricsFailureLog;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A child channel's metrics: hands each event to the server's {@link RakServerMetrics} with the
 * child, if the server has metrics. A server hook that throws is logged and ignored.
 */
public class DefaultChannelToServerProxyMetrics implements RakChannelMetrics {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(DefaultChannelToServerProxyMetrics.class);

    public RakServerChannel parent;
    public RakChildChannel channel;

    public DefaultChannelToServerProxyMetrics(RakServerChannel parent, RakChildChannel channel) {
        this.parent = parent;
        this.channel = channel;
    }

    @Override
    public void bytesIn(int count) {
        this.report(RakServerMetrics::bytesIn, "bytesIn", count);
    }

    @Override
    public void bytesOut(int count) {
        this.report(RakServerMetrics::bytesOut, "bytesOut", count);
    }

    @Override
    public void rakDatagramsIn(int count) {
        this.report(RakServerMetrics::rakDatagramsIn, "rakDatagramsIn", count);
    }

    @Override
    public void rakDatagramsOut(int count) {
        this.report(RakServerMetrics::rakDatagramsOut, "rakDatagramsOut", count);
    }

    @Override
    public void encapsulatedIn(int count) {
        this.report(RakServerMetrics::encapsulatedIn, "encapsulatedIn", count);
    }

    @Override
    public void encapsulatedOut(int count) {
        this.report(RakServerMetrics::encapsulatedOut, "encapsulatedOut", count);
    }

    @Override
    public void rakStaleDatagrams(int count) {
        this.report(RakServerMetrics::rakStaleDatagrams, "rakStaleDatagrams", count);
    }

    @Override
    public void ackIn(int count) {
        this.report(RakServerMetrics::ackIn, "ackIn", count);
    }

    @Override
    public void ackOut(int count) {
        this.report(RakServerMetrics::ackOut, "ackOut", count);
    }

    @Override
    public void nackOut(int count) {
        this.report(RakServerMetrics::nackOut, "nackOut", count);
    }

    @Override
    public void nackIn(int count) {
        this.report(RakServerMetrics::nackIn, "nackIn", count);
    }

    @Override
    public void stateChange(RakState state) {
        RakServerMetrics metrics = this.parent.config().getMetrics();
        if (metrics != null) {
            try {
                metrics.stateChange(this.channel, state);
            } catch (RuntimeException cause) {
                this.failed("stateChange", cause);
            }
        }
    }

    @Override
    public void queuedPacketBytes(int count) {
        this.report(RakServerMetrics::queuedPacketBytes, "queuedPacketBytes", count);
    }

    // Unbound method references capture nothing, so a per-packet report allocates nothing.
    private void report(CountHook hook, String name, int count) {
        RakServerMetrics metrics = this.parent.config().getMetrics();
        if (metrics != null) {
            try {
                hook.report(metrics, this.channel, count);
            } catch (RuntimeException cause) {
                this.failed(name, cause);
            }
        }
    }

    private void failed(String hook, RuntimeException cause) {
        MetricsFailureLog.failed(log, "RakServerMetrics." + hook, this.channel.remoteAddress(), cause);
    }

    @FunctionalInterface
    private interface CountHook {
        void report(RakServerMetrics metrics, RakChildChannel channel, int count);
    }
}

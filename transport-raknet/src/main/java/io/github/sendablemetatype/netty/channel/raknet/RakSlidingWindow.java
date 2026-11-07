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

package io.github.sendablemetatype.netty.channel.raknet;

import io.github.sendablemetatype.netty.channel.raknet.packet.RakDatagramPacket;
import io.github.sendablemetatype.netty.util.RakSequence;

import static io.github.sendablemetatype.netty.channel.raknet.RakConstants.*;

public class RakSlidingWindow {
    private final int mtu;
    private double cwnd;
    private double ssThresh;
    private double estimatedRTT = -1;
    private double deviationRTT = -1;
    private long nextCongestionControlBlock;
    private boolean backoffThisBlock;
    private int unackedBytes;
    // Doublings of the retransmission timeout since the last RTT sample, and when it may double again
    private int rtoBackoff;
    private long nextRtoBackoff;

    public RakSlidingWindow(int mtu) {
        this.mtu = mtu;
        this.cwnd = mtu;
    }

    public int getRetransmissionBandwidth() {
        return unackedBytes;
    }

    public int getTransmissionBandwidth() {
        if (this.unackedBytes <= this.cwnd) {
            return (int) (this.cwnd - this.unackedBytes);
        } else {
            return 0;
        }
    }

    public void onResend(long curSequenceIndex) {
        if (!this.backoffThisBlock && this.cwnd > this.mtu * 2D) {
            this.ssThresh = this.cwnd * 0.5D;

            if (this.ssThresh < this.mtu) {
                this.ssThresh = this.mtu;
            }
            this.cwnd = this.mtu;

            this.nextCongestionControlBlock = curSequenceIndex;
            this.backoffThisBlock = true;
        }
    }

    public void onNak() {
        if (!this.backoffThisBlock) {
            this.ssThresh = this.cwnd * 0.75D;
        }
    }

    /**
     * Doubles the retransmission timeout when resends fall due, up to {@link RakConstants#CC_MAXIMUM_THRESHOLD}, as
     * RFC 6298 section 5.5 does; the next {@link #onAck} drops the backoff. RakNet does not back off. Without it, a
     * timeout shorter than the round trip never grows, since only ACKs give RTT samples.
     * <p>
     * Each datagram has its own deadline, so deadlines falling due within one backed-off timeout of the last doubling
     * count as the same timeout. {@link #onResend} is no substitute: it cuts the window once per congestion block, and
     * a block only ends with an ACK.
     *
     * @param curTime the time the resends fell due
     */
    public void onRetransmissionTimeout(long curTime) {
        if (curTime < this.nextRtoBackoff) {
            return;
        }
        if (this.getRtoForRetransmission() < CC_MAXIMUM_THRESHOLD) {
            this.rtoBackoff++;
        }
        this.nextRtoBackoff = curTime + this.getRtoForRetransmission();
    }

    /**
     * Applies the acknowledgement of a reliable datagram.
     *
     * @param rtt              time since the acknowledged transmission was sent
     * @param size             the datagram's size, as counted by {@link #onReliableSend}
     * @param sequenceIndex    the datagram number the ACK named
     * @param curSequenceIndex the next datagram number to be sent
     */
    public void onAck(long rtt, int size, int sequenceIndex, long curSequenceIndex) {
        this.unackedBytes -= size;
        // Every transmission has its own datagram number, so a sample from a resend is as valid as any
        this.rtoBackoff = 0;
        this.nextRtoBackoff = 0;

        if (this.estimatedRTT == -1) {
            this.estimatedRTT = rtt;
            this.deviationRTT = rtt;
        } else {
            double d = 0.05D;
            double difference = rtt - this.estimatedRTT;
            this.estimatedRTT += d * difference;
            this.deviationRTT += d * (Math.abs(difference) - this.deviationRTT);
        }

        boolean isNewCongestionControlPeriod = RakSequence.difference(sequenceIndex,
                (int) this.nextCongestionControlBlock) > 0;

        if (isNewCongestionControlPeriod) {
            this.backoffThisBlock = false;
            this.nextCongestionControlBlock = curSequenceIndex;
        }

        if (this.isInSlowStart()) {
            this.cwnd += this.mtu;

            if (this.cwnd > this.ssThresh && this.ssThresh != 0) {
                this.cwnd = this.ssThresh + this.mtu * this.mtu / this.cwnd;
            }
        } else if (isNewCongestionControlPeriod) {
            this.cwnd += this.mtu * this.mtu / this.cwnd;
        }
    }

    public void onReliableSend(RakDatagramPacket datagram) {
        this.unackedBytes += datagram.getSize();
    }

    public boolean isInSlowStart() {
        return this.cwnd <= this.ssThresh || this.ssThresh == 0;
    }

    public long getRtoForRetransmission() {
        if (this.estimatedRTT == -1) {
            return CC_MAXIMUM_THRESHOLD;
        }

        long threshold = (long) ((2.0D * this.estimatedRTT + 4.0D * this.deviationRTT) + CC_ADDITIONAL_VARIANCE);
        // The backoff only grows while the result is below the cap, so the shift cannot overflow
        return Math.min(threshold << this.rtoBackoff, CC_MAXIMUM_THRESHOLD);
    }

    public double getRTT() {
        return this.estimatedRTT;
    }

    public int getUnackedBytes() {
        return unackedBytes;
    }
}

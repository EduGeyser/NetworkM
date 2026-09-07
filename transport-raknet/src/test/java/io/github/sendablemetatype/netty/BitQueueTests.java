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

package io.github.sendablemetatype.netty;

import io.github.sendablemetatype.netty.util.BitQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

public class BitQueueTests {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void bulkAppendsMatchScalarWritesAcrossWrapAndGrowth(boolean value) {
        int[] offsets = {0, 1, 7, 63, 64, 65, 127};
        int[] counts = {0, 1, 2, 7, 8, 63, 64, 65, 127, 128, 129, 511, 512, 513, 8192};
        for (boolean wrapped : new boolean[]{false, true}) {
            for (int offset : offsets) {
                for (int count : counts) {
                    BitQueue bulk = new BitQueue(256);
                    BitQueue scalar = new BitQueue(256);
                    for (int i = 0; i < 255; i++) {
                        bulk.add(i % 3 == 0);
                        scalar.add(i % 3 == 0);
                    }
                    if (wrapped) {
                        for (int i = 0; i < 137; i++) {
                            bulk.poll();
                            scalar.poll();
                        }
                        for (int i = 0; i < 64; i++) {
                            bulk.add((i & 1) == 0);
                            scalar.add((i & 1) == 0);
                        }
                    }
                    for (int i = 0; i < offset; i++) {
                        bulk.poll();
                        scalar.poll();
                    }

                    bulk.add(value, count);
                    for (int i = 0; i < count; i++) {
                        scalar.add(value);
                    }
                    Assertions.assertEquals(scalar.size(), bulk.size());
                    String scenario = "wrapped=" + wrapped + ", offset=" + offset + ", count=" + count;
                    while (!scalar.isEmpty()) {
                        Assertions.assertEquals(scalar.poll(), bulk.poll(), scenario);
                    }
                    Assertions.assertTrue(bulk.isEmpty());
                }
            }
        }
    }

    @Test
    public void invalidBulkCountsLeaveExistingBitsUntouched() {
        BitQueue queue = new BitQueue(64);
        queue.add(true);
        queue.add(false);
        Assertions.assertThrows(IllegalArgumentException.class, () -> queue.add(true, -1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> queue.add(true, Integer.MAX_VALUE));
        queue.add(false, 0);
        Assertions.assertEquals(2, queue.size());
        Assertions.assertTrue(queue.poll());
        Assertions.assertFalse(queue.poll());
    }

    @Test
    public void settingBitsWorksAcrossWordsAndAWrappedTail() {
        BitQueue queue = new BitQueue(256);
        for (int i = 0; i < 200; i++) {
            queue.add(false);
        }
        for (int i = 0; i < 73; i++) {
            queue.poll();
        }
        for (int i = 0; i < 100; i++) {
            queue.add(false);
        }
        for (int i = 0; i < queue.size(); i++) {
            queue.set(i, true);
            Assertions.assertTrue(queue.get(i), "Bit " + i);
        }
        for (int i = 0; i < queue.size(); i += 2) {
            queue.set(i, false);
        }
        for (int i = 0; !queue.isEmpty(); i++) {
            Assertions.assertEquals((i & 1) != 0, queue.poll());
        }
    }

    @Test
    public void testQueue() {
        Queue<Boolean> bits = new ArrayDeque<>();
        BitQueue queue = new BitQueue();

        for (int i = 0; i < 256; i++) {
            boolean value = ThreadLocalRandom.current().nextBoolean();
            queue.add(value);
            bits.add(value);
        }

        while (!queue.isEmpty() && !bits.isEmpty()) {
            boolean expected = bits.poll();
            boolean actual = queue.poll();
            Assertions.assertEquals(expected, actual, "Expected %s but got %s");
        }
        Assertions.assertTrue(queue.isEmpty() && bits.isEmpty(), "Queue is not empty");
    }
}

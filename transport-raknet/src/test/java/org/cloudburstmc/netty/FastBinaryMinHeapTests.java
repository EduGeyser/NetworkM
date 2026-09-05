package org.cloudburstmc.netty;

import org.cloudburstmc.netty.util.FastBinaryMinHeap;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class FastBinaryMinHeapTests {
    @Test
    void seriesChecksTheLastExistingParent() {
        FastBinaryMinHeap<Integer> heap = new FastBinaryMinHeap<>();
        try {
            heap.insert(10, 10);
            heap.insertSeries(1, new Integer[]{1, 1});
            assertEquals(1, heap.poll());
            assertEquals(1, heap.poll());
            assertEquals(10, heap.poll());
            assertNull(heap.peek());
        } finally {
            heap.release();
        }
    }

    @Test
    void mixedSeriesAndSingleInsertionsMatchAPriorityQueue() {
        FastBinaryMinHeap<Integer> heap = new FastBinaryMinHeap<>(0);
        PriorityQueue<Integer> expected = new PriorityQueue<>();
        Random random = new Random(742918);
        try {
            assertNull(heap.peek());
            for (int i = 0; i < 2000; i++) {
                int value = random.nextInt(100) - 50;
                int count = random.nextInt(8) + 1;
                if (count == 1) {
                    heap.insert(value, value);
                    expected.add(value);
                } else {
                    Integer[] series = new Integer[count];
                    java.util.Arrays.fill(series, value);
                    heap.insertSeries(value, series);
                    for (int j = 0; j < count; j++) {
                        expected.add(value);
                    }
                }
                int removals = Math.min(expected.size(), random.nextInt(8));
                for (int j = 0; j < removals; j++) {
                    assertEquals(expected.poll(), heap.poll());
                }
            }
            while (!expected.isEmpty()) {
                assertEquals(expected.poll(), heap.poll());
            }
            assertTrue(heap.isEmpty());
            Iterator<Integer> iterator = heap.iterator();
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        } finally {
            heap.release();
        }
    }

    @Test
    void releasingANonemptyHeapRecyclesEachEntryOnce() {
        FastBinaryMinHeap<Integer> heap = new FastBinaryMinHeap<>();
        heap.insertSeries(1, new Integer[]{1, 2, 3});
        assertDoesNotThrow(() -> {
            heap.release();
        });
        FastBinaryMinHeap<Integer> replacement = new FastBinaryMinHeap<>();
        try {
            replacement.insert(2, 2);
            replacement.insert(1, 1);
            assertEquals(1, replacement.poll());
            assertEquals(2, replacement.poll());
        } finally {
            replacement.release();
        }
    }
}

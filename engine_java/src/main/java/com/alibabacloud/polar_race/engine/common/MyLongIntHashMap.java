package com.alibabacloud.polar_race.engine.common;

import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dongxu.xiu
 * @since 2018-11-13 下午2:46
 */
public class MyLongIntHashMap {

    public long[] keys;
    public int[] values;
    protected int keyMixer;

    protected int assigned;

    protected int mask;

    protected int resizeAt;

    protected boolean hasEmptyKey;

    protected double loadFactor;

    private static String testsSeedProperty;
    private final static String NOT_AVAILABLE = new String();
    protected final AtomicLong seedMixer;


    public MyLongIntHashMap(int expectedElements, double loadFactor) {
        this.seedMixer = new AtomicLong(randomSeed64());
        this.loadFactor = loadFactor;
        ensureCapacity(expectedElements);
    }

    public long randomSeed64() {
        if (testsSeedProperty == null) {
            try {
                testsSeedProperty = java.security.AccessController.doPrivileged(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        return System.getProperty("tests.seed", NOT_AVAILABLE);
                    }
                });
            } catch (SecurityException e) {
                testsSeedProperty = NOT_AVAILABLE;
            }
        }
        long initialSeed;
        if (testsSeedProperty != NOT_AVAILABLE) {
            initialSeed = testsSeedProperty.hashCode();
        } else {
            initialSeed = System.nanoTime() ^
                    System.identityHashCode(new Object());
        }
        return mix64(initialSeed);
    }

    public void ensureCapacity(int expectedElements) {
        if (expectedElements > resizeAt || keys == null) {
            final long[] prevKeys = this.keys;
            final int[] prevValues = this.values;
            allocateBuffers(minBufferSize(expectedElements, loadFactor));
            if (prevKeys != null && !isEmpty()) {
                rehash(prevKeys, prevValues);
            }
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        return assigned + (hasEmptyKey ? 1 : 0);
    }

    public int minBufferSize(int elements, double loadFactor) {
        long length = (long) Math.ceil(elements / loadFactor);
        if (length == elements) {
            length++;
        }
        length = Math.max(4, nextHighestPowerOfTwo(length));
        return (int) length;
    }

    public int put(long key, int value) {

        final int mask = this.mask;
        if (((key) == 0)) {
            hasEmptyKey = true;
            int previousValue = values[mask + 1];
            values[mask + 1] = value;
            return previousValue;
        } else {
            final long[] keys = this.keys;
            int slot = hashKey(key) & mask;

            long existing;
            while (!((existing = keys[slot]) == 0)) {
                if (((existing) == (key))) {
                    final int previousValue = values[slot];
                    values[slot] = value;
                    return previousValue;
                }
                slot = (slot + 1) & mask;
            }
            if (assigned == resizeAt) {
                allocateThenInsertThenRehash(slot, key, value);
            } else {
                keys[slot] = key;
                values[slot] = value;
            }
            assigned++;
            return 0;
        }
    }

    public int get(long key) {
        if (((key) == 0)) {
            return hasEmptyKey ? values[mask + 1] : 0;
        } else {
            final long[] keys = this.keys;
            final int mask = this.mask;
            int slot = hashKey(key) & mask;

            long existing;
            while (!((existing = keys[slot]) == 0)) {
                if (((existing) == (key))) {
                    return values[slot];
                }
                slot = (slot + 1) & mask;
            }

            return 0;
        }
    }

    protected int hashKey(long key) {
        return mix(key, this.keyMixer);
    }

    public int mix(long key, int seed)   { return (int) mix64(key ^ seed); }

    protected void rehash(long[] fromKeys, int[] fromValues) {
        final long[] keys = this.keys;
        final int[] values = this.values;
        final int mask = this.mask;
        long existing;
        int from = fromKeys.length - 1;
        keys[keys.length - 1] = fromKeys[from];
        values[values.length - 1] = fromValues[from];
        while (--from >= 0) {
            if (!((existing = fromKeys[from]) == 0)) {
                int slot = hashKey(existing) & mask;
                while (!((keys[slot]) == 0)) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = existing;
                values[slot] = fromValues[from];
            }
        }
    }

    protected void allocateBuffers(int arraySize) {
        final int newKeyMixer = newKeyMixer();
        long[] prevKeys = this.keys;
        int[] prevValues = this.values;
        try {
            int emptyElementSlot = 1;
            this.keys = (new long[arraySize + emptyElementSlot]);
            this.values = (new int[arraySize + emptyElementSlot]);
        } catch (OutOfMemoryError e) {
            this.keys = prevKeys;
            this.values = prevValues;
            throw new RuntimeException(
                    "Not enough memory to allocate buffers for rehashing: %,d -> %,d");
        }

        this.resizeAt = expandAtCount(arraySize, loadFactor);
        this.keyMixer = newKeyMixer;
        this.mask = arraySize - 1;
    }

    public int newKeyMixer() {
        return (int) mix64(seedMixer.incrementAndGet());
    }

    public long mix64(long z) {
        z = (z ^ (z >>> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >>> 29)) * 0xfc12c5b19d3259e9L;
        return z ^ (z >>> 32);
    }

    public int expandAtCount(int arraySize, double loadFactor) {
        return Math.min(arraySize - 1, (int) Math.ceil(arraySize * loadFactor));
    }

    protected void allocateThenInsertThenRehash(int slot, long pendingKey, int pendingValue) {
        final long[] prevKeys = this.keys;
        final int[] prevValues = this.values;
        allocateBuffers(nextBufferSize(mask + 1));
        prevKeys[slot] = pendingKey;
        prevValues[slot] = pendingValue;
        rehash(prevKeys, prevValues);
    }

    public int nextBufferSize(int arraySize) {
        return arraySize << 1;
    }

    public long nextHighestPowerOfTwo(long v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }
}

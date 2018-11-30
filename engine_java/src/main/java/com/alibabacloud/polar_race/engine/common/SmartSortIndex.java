package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartSortIndex {

    private int limit;

    private int plusNum = 0;

    public static SmartSortIndex instance = new SmartSortIndex();

    private long[] sortIndex = new long[Constant.TOTAL_KV_COUNT];

    private AtomicInteger index = new AtomicInteger(0);

    private AtomicInteger negativeCount = new AtomicInteger(0);

    private SmartSortIndex() {
        Arrays.fill(sortIndex, Long.MAX_VALUE);
    }

    public void sort() {
        //Arrays.parallelSort(sortIndex);
        Arrays.sort(sortIndex);
        this.limit = negativeCount.get();
        plusNum = index.get() - this.limit;
    }

    public int calcIndex(int index) {
        return index + 1 > plusNum ? index - plusNum : index + limit;
    }

    public void set(long element) {
        sortIndex[index.getAndIncrement()] = element;
    }

    public long get(int index) {
        return sortIndex[calcIndex(index)];
    }

    public void negativeAdd(int count) {
        negativeCount.getAndAdd(count);
    }

    public int getTotalKvCount() {
        return index.get();
    }
}

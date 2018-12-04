package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartSortIndex {

    private int limit;
    private int plusNum;
    private int needSlotCount;
    public static SmartSortIndex instance = new SmartSortIndex();
    private long[] sortIndex = new long[Constant.TOTAL_KV_COUNT];
    private AtomicInteger totalKvCount = new AtomicInteger(0);
    private AtomicInteger negativeCount = new AtomicInteger(0);

    private SmartSortIndex() {
        Arrays.fill(sortIndex, Long.MAX_VALUE);
    }

    public void sort() {
        Arrays.sort(sortIndex);
        this.limit = negativeCount.get();
        plusNum = totalKvCount.get() - this.limit;
        int temp = totalKvCount.get() / Constant.SLOT_SIZE;
        needSlotCount = totalKvCount.get() % Constant.SLOT_SIZE == 0 ? temp : temp + 1;
    }

    public int calcIndex(int index) {
        return index + 1 > plusNum ? index - plusNum : index + limit;
    }

    public void set(long element) {
        sortIndex[totalKvCount.getAndIncrement()] = element;
    }

    public long get(int index) {
        return sortIndex[calcIndex(index)];
    }

    public void negativeAdd(int count) {
        negativeCount.getAndAdd(count);
    }

    public int getTotalKvCount() {
        return totalKvCount.get();
    }

    public int getNeedSlotCount() {
        return needSlotCount;
    }
}

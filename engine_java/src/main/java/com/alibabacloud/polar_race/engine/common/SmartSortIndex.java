package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartSortIndex {


    public static SmartSortIndex instance = new SmartSortIndex();

    private long[] sortIndexs = new long[Constant.TOTAL_KV_COUNT];

    private int limit;

    private int plusNum = 0;

    private AtomicInteger index = new AtomicInteger(0);

    private SmartSortIndex() {
        for (int i = 0; i < sortIndexs.length; i++) {
            sortIndexs[i] = Long.MAX_VALUE;
        }
    }

    public void sort(int negative) {
        Arrays.sort(sortIndexs);
        this.limit = negative;
        plusNum = index.get() - this.limit;
    }

    public int calcuIndex(int index) {
        return index + 1 > plusNum ? index - plusNum : index + limit;
    }

    public void set(long element) {
        sortIndexs[index.getAndIncrement()] = element;
    }

    public long get(int index) {
        return sortIndexs[calcuIndex(index)];
    }

}

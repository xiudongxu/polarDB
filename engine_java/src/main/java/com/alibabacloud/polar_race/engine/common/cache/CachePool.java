package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Data;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CachePool {
    private volatile int readCursor;
    private volatile int loadCursor;

    private Data[] datas;
    private CacheBlock[] blocks;

    private AtomicInteger totalKvCount = new AtomicInteger(0);
    private AtomicInteger negativeCount = new AtomicInteger(0);

    public CachePool(Data[] datas, CacheBlock[] blocks) {
        this.datas = datas;
        this.blocks = blocks;
    }

    public void addNegative(int count) {
        negativeCount.getAndAdd(count);
    }

    public AtomicInteger getNegativeCount() {
        return negativeCount;
    }

    public AtomicInteger getTotalKvCount() {
        return totalKvCount;
    }

    public void addTotal(int count) {
        totalKvCount.getAndAdd(count);
    }

    public Data[] getDatas() {
        return datas;
    }

    public void setDatas(Data[] datas) {
        this.datas = datas;
    }

    public int getReadCursor() {
        return readCursor;
    }

    public void setReadCursor(int readCursor) {
        this.readCursor = readCursor;
    }

    public int getLoadCursor() {
        return loadCursor;
    }

    public void setLoadCursor(int loadCursor) {
        this.loadCursor = loadCursor;
    }

    public CacheBlock[] getBlocks() {
        return blocks;
    }

    public void setBlocks(CacheBlock[] blocks) {
        this.blocks = blocks;
    }
}

package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.SmartSortIndex;
import com.alibabacloud.polar_race.engine.common.ThreadContext;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangshuo
 * @version 2018-11-29
 */
public class CacheSlot {

    private int totalKvCount;
    private RingCachePool cachePool;
    private LongObjectHashMap<byte[]> map;
    private AtomicInteger readCount = new AtomicInteger(0);
    private volatile int slotStatus = Integer.MIN_VALUE;

    public CacheSlot(RingCachePool cachePool) {
        this.cachePool = cachePool;
        this.map = new LongObjectHashMap<>(Constant.SLOT_SIZE);
        this.totalKvCount = SmartSortIndex.instance.getTotalKvCount();
    }

    public void addReadCount(int sIndex, int eIndex, int readCursor) {
        readCount.getAndIncrement();
        if (eIndex >= totalKvCount) ThreadContext.setReadCursor(readCursor + 1);
        if (readCount.get() == Constant.THREAD_COUNT) {
            this.map.clear();
            this.readCount.set(0);
            if (sIndex + Constant.CACHE_SIZE >= totalKvCount) {
                this.slotStatus = Integer.MIN_VALUE;
            } else {
                this.slotStatus = this.slotStatus | Integer.MIN_VALUE;
            }
        }
    }

    public void setFullStatus(int generation) {
        this.slotStatus = generation;
    }

    public int getSlotStatus() {
        return slotStatus;
    }

    public LongObjectHashMap<byte[]> getMap() {
        return map;
    }
}

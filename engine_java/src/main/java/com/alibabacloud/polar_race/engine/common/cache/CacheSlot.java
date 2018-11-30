package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangshuo
 * @version 2018-11-29
 */
public class CacheSlot {

    private LongObjectHashMap<byte[]> map;
    private AtomicInteger readCount = new AtomicInteger(0);
    private volatile int slotStatus = Integer.MIN_VALUE;

    public CacheSlot() {
        this.map = new LongObjectHashMap<>(Constant.SLOT_SIZE);
    }

    public void addReadCount(boolean readDone) {
        readCount.getAndIncrement();
        if (readCount.get() == Constant.THREAD_COUNT) {
            this.map.clear();
            this.readCount.set(0);
            if (readDone) {
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

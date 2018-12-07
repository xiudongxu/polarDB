package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.SmartSortIndex;
import com.alibabacloud.polar_race.engine.common.ThreadContext;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangshuo
 * @version 2018-11-29
 */
public class CacheSlot {

    private int totalKvCount;
    private ByteBuffer slotValues;

    private volatile int slotStatus = Integer.MIN_VALUE;
    private AtomicInteger readCount = new AtomicInteger(0);

    public CacheSlot(ByteBuffer byteBuffer) {
        slotValues = byteBuffer;
        this.totalKvCount = SmartSortIndex.instance.getTotalKvCount();
    }

    public void addReadCount(int eIndex, int readCursor) {
        readCount.getAndIncrement();
        if (eIndex >= totalKvCount) ThreadContext.setReadCursor(readCursor + 1);
        if (readCount.get() == Constant.THREAD_COUNT) {
            this.readCount.set(0);
            slotValues.clear();
            this.slotStatus = this.slotStatus | Integer.MIN_VALUE;
        }
    }

    public void setFullStatus(int generation) {
        this.slotStatus = generation;
    }

    public int getSlotStatus() {
        return slotStatus;
    }

    public ByteBuffer getSlotValues() {
        return slotValues;
    }
}

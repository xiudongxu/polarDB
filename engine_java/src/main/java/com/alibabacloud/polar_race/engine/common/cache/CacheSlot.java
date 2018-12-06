package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.SmartSortIndex;
import com.alibabacloud.polar_race.engine.common.ThreadContext;
import java.util.concurrent.atomic.AtomicInteger;
import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;

/**
 * @author wangshuo
 * @version 2018-11-29
 */
public class CacheSlot {

    private int totalKvCount;
    //private byte[][] slotValues;

    private AlignedDirectByteBuffer slotValues;
    private volatile int slotStatus = Integer.MIN_VALUE;
    private AtomicInteger readCount = new AtomicInteger(0);

    public CacheSlot(String path) {
        DirectIoLib directIoLib = DirectIoLib.getLibForPath(path);
        slotValues = AlignedDirectByteBuffer
                .allocate(directIoLib, Constant.SLOT_SIZE * Constant.VALUE_SIZE);
        //slotValues = new byte[Constant.SLOT_SIZE][Constant.VALUE_SIZE];
        this.totalKvCount = SmartSortIndex.instance.getTotalKvCount();
    }

    public void addReadCount(int eIndex, int readCursor) {
        readCount.getAndIncrement();
        if (eIndex >= totalKvCount) ThreadContext.setReadCursor(readCursor + 1);
        if (readCount.get() == Constant.THREAD_COUNT) {
            this.readCount.set(0);
            this.slotStatus = this.slotStatus | Integer.MIN_VALUE;
        }
    }

    public void setFullStatus(int generation) {
        this.slotStatus = generation;
    }

    public int getSlotStatus() {
        return slotStatus;
    }

    /*public byte[][] getSlotValues() {
        return slotValues;
    }*/

    public AlignedDirectByteBuffer getSlotValues() {
        return slotValues;
    }
}

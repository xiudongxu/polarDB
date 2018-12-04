package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SmartSortIndex;
import com.alibabacloud.polar_race.engine.common.cache.CacheSlot;
import com.alibabacloud.polar_race.engine.common.cache.RingCachePool;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangshuo
 * @version 2018-11-29
 */
public class CacheSlotThread implements Runnable {

    private Data[] datas;
    private int totalKvCount;
    private int needTotalSlot;
    private RingCachePool cachePool;
    private AtomicInteger slotCursor;

    public CacheSlotThread(RingCachePool cachePool) {
        this.cachePool = cachePool;
        this.datas = cachePool.getDatas();
        this.slotCursor = cachePool.getSlotCursor();
        this.totalKvCount = SmartSortIndex.instance.getTotalKvCount();
        this.needTotalSlot = SmartSortIndex.instance.getNeedSlotCount();
    }

    @Override
    public void run() {
        int slotCursor = this.slotCursor.getAndAdd(1);
        while (slotCursor < (needTotalSlot << 1)) {
            int realCursor = slotCursor & (Constant.SLOT_COUNT - 1);
            CacheSlot cacheSlot = cachePool.getCacheSlots()[realCursor];
            loadToSlot(cacheSlot, slotCursor);
            slotCursor = this.slotCursor.getAndAdd(1);
        }
    }

    private void loadToSlot(CacheSlot cacheSlot, int slotCursor) {
        int loadCursor = slotCursor % needTotalSlot; //理论上是第loadCursor个槽该加载的位置
        int startIndex = loadCursor * Constant.SLOT_SIZE;
        int tmpEnd = startIndex + Constant.SLOT_SIZE;
        int endIndex = tmpEnd > totalKvCount ? totalKvCount : tmpEnd;
        int generation = (slotCursor >> 6) + 1;
        int slotStatus = generation | Integer.MIN_VALUE;
        byte[][] slotValues = cacheSlot.getSlotValues();
        for (;;) {
            if (slotStatus != cacheSlot.getSlotStatus() + 1) {
                this.cacheSleep(1);
                continue;
            }
            for (int i = startIndex, j = 0; i < endIndex; i++, j++) {
                long keyL = SmartSortIndex.instance.get(i);
                int modulus = (int) (keyL & (datas.length - 1));
                Data data = datas[modulus];
                try {
                    byte[] bytes = data.readValue(data.get(keyL));
                    slotValues[j] = bytes;
                } catch (EngineException e) {
                    System.out.println("during load to cache : read value IO exception!!!");
                }
            }
            cacheSlot.setFullStatus(generation);
            break;
        }
    }

    private void cacheSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

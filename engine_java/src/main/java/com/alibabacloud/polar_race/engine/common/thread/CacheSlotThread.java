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
public class CacheSlotThread extends Thread {

    private Data[] datas;
    private int totalKvCount;
    private RingCachePool cachePool;
    private AtomicInteger slotCursor;

    public CacheSlotThread(RingCachePool cachePool) {
        this.cachePool = cachePool;
        this.datas = cachePool.getDatas();
        this.slotCursor = cachePool.getSlotCursor();
        this.totalKvCount = SmartSortIndex.instance.getTotalKvCount();
    }

    @Override
    public void run() {
        int slotCursor = this.slotCursor.get();
        while (slotCursor < (totalKvCount * 2) / Constant.SLOT_SIZE) {
            slotCursor = this.slotCursor.getAndAdd(1);
            int realCursor = slotCursor % Constant.SLOT_COUNT;
            CacheSlot cacheSlot = cachePool.getCacheSlots()[realCursor];
            loadToSlot(cacheSlot, slotCursor);
        }
    }

    private void loadToSlot(CacheSlot cacheSlot, int slotCursor) {
        int startIndex = slotCursor * Constant.SLOT_SIZE % totalKvCount;
        int endIndex = startIndex + Constant.SLOT_SIZE;
        int generation = startIndex / Constant.CACHE_SIZE + 1;
        int slotStatus = generation | Integer.MIN_VALUE;
        for (;;) {
            if (slotStatus != cacheSlot.getSlotStatus() + 1) {
                continue;
            }
            for (int i = startIndex; i < endIndex; i++) {
                long keyL = SmartSortIndex.instance.get(i);
                int modulus = (int) (keyL & (datas.length - 1));
                Data data = datas[modulus];
                try {
                    byte[] bytes = data.readValue(data.get(keyL));
                    cacheSlot.getMap().put(keyL, bytes);
                } catch (EngineException e) {
                    System.out.println("during load to cache : read value IO exception!!!");
                }
            }
            cacheSlot.setFullStatus(generation);
            break;
        }
    }
}

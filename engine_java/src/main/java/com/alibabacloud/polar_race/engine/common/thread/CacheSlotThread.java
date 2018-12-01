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
    private int totalSlot;
    private int totalKvCount;
    private RingCachePool cachePool;
    private AtomicInteger slotCursor;

    public CacheSlotThread(RingCachePool cachePool) {
        this.cachePool = cachePool;
        this.datas = cachePool.getDatas();
        this.slotCursor = cachePool.getSlotCursor();
        this.totalKvCount = SmartSortIndex.instance.getTotalKvCount();
        int temp = totalKvCount / Constant.SLOT_SIZE;
        totalSlot = totalKvCount % Constant.SLOT_SIZE == 0 ? temp : temp + 1;
    }

    @Override
    public void run() {
        int slotCursor = this.slotCursor.getAndAdd(1);
        while (slotCursor < (totalSlot << 1)) {
            int realCursor = slotCursor % Constant.SLOT_COUNT;
            CacheSlot cacheSlot = cachePool.getCacheSlots()[realCursor];
            loadToSlot(cacheSlot, slotCursor);
            slotCursor = this.slotCursor.getAndAdd(1);
        }
    }

    private void loadToSlot(CacheSlot cacheSlot, int slotCursor) {
        int loadCursor = slotCursor % totalSlot; //理论上第loadCursor个槽该加载的位置
        int startIndex = loadCursor * Constant.SLOT_SIZE;
        int tmpEnd = startIndex + Constant.SLOT_SIZE;
        int endIndex = tmpEnd > totalKvCount ? totalKvCount : tmpEnd;
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

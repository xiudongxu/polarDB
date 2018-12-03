package com.alibabacloud.polar_race.engine.common.cache;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangshuo
 * @version 2018-11-29
 */
public class RingCachePool {

    private Data[] datas;
    private CacheSlot[] cacheSlots;
    private AtomicInteger slotCursor = new AtomicInteger(0);

    public RingCachePool(Data[] datas) {
        this.datas = datas;
        this.cacheSlots = new CacheSlot[Constant.SLOT_COUNT];
    }

    public Data[] getDatas() {
        return datas;
    }

    public CacheSlot[] getCacheSlots() {
        return cacheSlots;
    }

    public AtomicInteger getSlotCursor() {
        return slotCursor;
    }

}

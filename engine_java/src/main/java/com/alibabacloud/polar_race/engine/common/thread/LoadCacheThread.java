package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.CachePool;
import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.carrotsearch.hppc.LongObjectHashMap;

/**
 * @author wangshuo
 * @version 2018-11-23
 */
public class LoadCacheThread extends Thread {

    private Data[] datas;
    private CachePool cachePool;
    private final LongObjectHashMap<byte[]>[] maps; //缓存数组，为了读写分离

    public LoadCacheThread(CachePool cachePool) {
        this.cachePool = cachePool;
        this.maps = cachePool.getMaps();
        this.datas = cachePool.getDatas();
    }

    @Override
    public void run() {
        while (true) {
            int loadCursor = cachePool.getLoadCursor();
            int readCursor = cachePool.getReadCursor();

            if (Constant.TOTAL_CAP - (loadCursor - readCursor) <= 0) {
                synchronized (cachePool) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            int mapIndex = (loadCursor / Constant.CACHE_SIZE) & Constant.POOL_COUNT - 1;
            maps[mapIndex].clear();
            for (int i = loadCursor; i < loadCursor + Constant.CACHE_SIZE; i++) {
                long key = SortIndex.instance.get(i);
                if (key == Long.MAX_VALUE) {
                    cachePool.setLoadCursor(i);
                    return;
                }
                int modulus = (int) (key & (datas.length - 1));
                Data data = datas[modulus];

                try {
                    byte[] bytes = data.readValue(data.get(key));
                    maps[mapIndex].put(key, bytes);
                } catch (EngineException e) {
                    System.out.println("during load cache : read value IO exception!!!");
                }
            }
            cachePool.setLoadCursor(cachePool.getLoadCursor() + Constant.CACHE_SIZE);
            synchronized (cachePool) {
                cachePool.notify();
            }
        }
    }
}

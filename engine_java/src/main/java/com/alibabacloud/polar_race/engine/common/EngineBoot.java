package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.thread.InitDataThread;
import com.alibabacloud.polar_race.engine.common.thread.LoadCacheThread;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-10
 */
public class EngineBoot {

    public static Data[] initDataFile(String path) throws InterruptedException {
        Data[] datas = new Data[Constant.DATA_FILE_COUNT];
        CountDownLatch downLatch = new CountDownLatch(Constant.DATA_FILE_COUNT);
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            new InitDataThread(i, path, datas, downLatch).start();
        }
        downLatch.await();

        long begin = System.currentTimeMillis();
        System.out.println("start sort time :" + begin);
        SortIndex.instance.sort();
        System.out.println("sort cost time :" + (System.currentTimeMillis() - begin));
        return datas;
    }

    public static CachePool initCachePool(Data[] datas) {
        LongObjectHashMap<byte[]>[] maps = new LongObjectHashMap[Constant.POOL_COUNT];
        for (int i = 0; i < Constant.POOL_COUNT; i++) {
            maps[i] = new LongObjectHashMap<>(Constant.CACHE_SIZE);
        }
        return new CachePool(datas, 0, 0, maps);
    }

    public static void loadCache(CachePool cachePool) throws InterruptedException {
        new LoadCacheThread(cachePool).start();
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }
}

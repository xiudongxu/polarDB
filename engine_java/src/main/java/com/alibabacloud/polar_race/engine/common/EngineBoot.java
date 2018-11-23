package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.thread.LoadCacheThread;
import com.alibabacloud.polar_race.engine.common.thread.InitDataThread;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

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

    public static LongObjectHashMap<byte[]>[] initCacheMap(Data[] datas, Semaphore semaphore,
            CyclicBarrier barrier) {
        LongObjectHashMap<byte[]>[] maps = new LongObjectHashMap[Constant.POOL_COUNT];
        for (int i = 0; i < Constant.POOL_COUNT; i++) {
            maps[i] = new LongObjectHashMap<>(Constant.CACHE_SIZE);
            new LoadCacheThread(datas, i, semaphore, barrier, maps[i]).start();
        }
        return maps;
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }
}

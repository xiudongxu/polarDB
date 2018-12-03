package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.cache.CacheSlot;
import com.alibabacloud.polar_race.engine.common.cache.RingCachePool;
import com.alibabacloud.polar_race.engine.common.thread.CacheSlotThread;
import com.alibabacloud.polar_race.engine.common.thread.InitDataThread;
import com.alibabacloud.polar_race.engine.common.thread.LoadIndexThread;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

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
        return datas;
    }

    public static void loadAndSortIndex(Data[] datas) {
        CountDownLatch countDownLatch = new CountDownLatch(Constant.DATA_FILE_COUNT);
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            new LoadIndexThread(datas[i], countDownLatch).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            System.out.println("load index down latch await error");
        }
        SmartSortIndex.instance.sort();
    }

    public static RingCachePool initRingCache(Data[] datas) {
        RingCachePool cachePool = new RingCachePool(datas);
        CacheSlot[] cacheSlots = cachePool.getCacheSlots();
        for (int i = 0; i < Constant.SLOT_COUNT; i++) {
            cacheSlots[i] = new CacheSlot();
        }
        return cachePool;
    }

    public static void loadToCachePool(RingCachePool cachePool, ExecutorService executorService) {
        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            executorService.execute(new CacheSlotThread(cachePool));
        }
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }

    public static void stopLoadCacheThread(ExecutorService executorService) {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}

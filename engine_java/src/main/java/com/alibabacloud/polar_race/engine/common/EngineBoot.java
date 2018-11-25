package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.cache.CacheBlock;
import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import com.alibabacloud.polar_race.engine.common.thread.CacheThread;
import com.alibabacloud.polar_race.engine.common.thread.InitDataThread;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

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

        System.out.println("start sort index at " + LocalDateTime.now());
        SortIndex.instance.sort();
        System.out.println("finish sort index at " + LocalDateTime.now());
        return datas;
    }

    public static CachePool initCachePool(Data[] datas) {
        System.out.println("start init cache pool at " + LocalDateTime.now());
        CacheBlock[] cacheBlocks = new CacheBlock[Constant.THREAD_COUNT];
        CachePool cachePool = new CachePool(datas, cacheBlocks);
        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            cacheBlocks[i] = new CacheBlock();
        }
        System.out.println("finish init cache pool at " + LocalDateTime.now());
        return cachePool;
    }

    public static void loadDataToCachePool(CachePool cachePool, CyclicBarrier loadBB,
            CyclicBarrier loadEB, CountDownLatch downLatch) throws InterruptedException {
        System.out.println("start load to cache pool at " + LocalDateTime.now());
        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            new CacheThread(i, cachePool, loadBB, loadEB, downLatch).start();
        }
        downLatch.await();
        System.out.println("finish load to cache pool at " + LocalDateTime.now());
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }
}

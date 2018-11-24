package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.cache.CacheBlock;
import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import com.alibabacloud.polar_race.engine.common.thread.CacheThread;
import com.alibabacloud.polar_race.engine.common.thread.InitDataThread;
import java.io.IOException;
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

        long begin = System.currentTimeMillis();
        System.out.println("start sort time :" + begin);
        SortIndex.instance.sort();
        System.out.println("sort cost time :" + (System.currentTimeMillis() - begin));
        return datas;
    }

    public static CachePool initCachePool(Data[] datas, CyclicBarrier loadBB, CyclicBarrier loadEB) {
        CountDownLatch downLatch = new CountDownLatch(Constant.DATA_FILE_COUNT);
        CacheBlock[] cacheBlocks = new CacheBlock[Constant.THREAD_COUNT];
        CachePool cachePool = new CachePool(datas, cacheBlocks);
        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            cacheBlocks[i] = new CacheBlock();
            new CacheThread(i, cachePool, loadBB, loadEB, downLatch).start();
        }
        return cachePool;
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }
}

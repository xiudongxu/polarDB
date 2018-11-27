package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-25
 */
public class LoadIndexThread extends Thread {

    private Data data;
    private CachePool cachePool;
    private CountDownLatch downLatch;

    public LoadIndexThread(Data data, CountDownLatch downLatch, CachePool cachePool) {
        this.data = data;
        this.downLatch = downLatch;
        this.cachePool = cachePool;
    }

    @Override
    public void run() {
        try {
            int totalCount = 0;
            int negativeCount = 0;
            long[] keys = data.getMap().keys;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == 0) continue;
                if (keys[i] < 0) {
                    negativeCount++;
                }
                SortIndex.instance.set(keys[i]);
                totalCount++;
            }

            String name = Thread.currentThread().getName();
            System.out.println(
                    "thread name: " + name + " keys count: " + keys.length + " totalCount: "
                            + totalCount + " negative count: " + negativeCount);
            cachePool.addNegative(negativeCount);
            cachePool.addTotal(totalCount);
        } finally {
            downLatch.countDown();
        }
    }
}

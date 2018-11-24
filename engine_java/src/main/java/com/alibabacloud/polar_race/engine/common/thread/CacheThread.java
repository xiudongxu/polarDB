package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import com.alibabacloud.polar_race.engine.common.cache.CacheBlock;
import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CacheThread extends Thread {

    private Data[] datas;
    private CacheBlock block;
    private boolean firstLoad = true;

    private int threadNum;
    private CachePool cachePool;
    private CountDownLatch downLatch;
    private CyclicBarrier loadBarrier;

    public CacheThread(int threadNum, CachePool cachePool, CyclicBarrier barrier,
            CountDownLatch downLatch) {
        this.threadNum = threadNum;
        this.cachePool = cachePool;
        this.loadBarrier = barrier;
        this.downLatch = downLatch;
        this.datas = cachePool.getDatas();
        this.block = cachePool.getBlocks()[threadNum];
    }

    @Override
    public void run() {
        while (true) {
            if (cachePool.getLoadCursor() >= Constant.TOTAL_KV_COUNT) return;

            int loadCursor = cachePool.getLoadCursor();
            //判断能不能继续添加缓存
            synchronized (cachePool) {
                if (Constant.CACHE_CAP - (loadCursor - cachePool.getReadCursor()) < 0) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }

            //加载入缓存
            int startIndex = loadCursor + threadNum * Constant.CACHE_SIZE;
            if (startIndex >= Constant.TOTAL_KV_COUNT) return;
            int tmpEnd = startIndex + Constant.CACHE_SIZE;
            int endIndex = tmpEnd > Constant.TOTAL_KV_COUNT ? Constant.TOTAL_KV_COUNT : tmpEnd;
            int mapIndex = (loadCursor / Constant.ONE_CACHE_SIZE) & (Constant.MAPS_PER_BLOCK - 1);
            LongObjectHashMap<byte[]> map = block.getMaps()[mapIndex];
            for (int i = startIndex; i < endIndex; i++) {
                long key = SortIndex.instance.get(i);
                if (key == Long.MAX_VALUE) return; //TOTAL_KV_COUNT 必须能被 ONE_CACHE_SIZE 整除

                int modulus = (int) (key & (datas.length - 1));
                Data data = datas[modulus];

                try {
                    byte[] bytes = data.readValue(data.get(key));
                    map.put(key, bytes);
                } catch (EngineException e) {
                    System.out.println("during load cache : read value IO exception!!!");
                }
            }

            try {
                loadBarrier.await();
                if (firstLoad) {
                    firstLoad = false;
                    downLatch.countDown();
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
}

package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.cache.CacheBlock;
import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CacheThread extends Thread {

    private Data[] datas;
    private CacheBlock block;
    private int totalKvCount;

    private int threadNum;
    private CachePool cachePool;
    private CyclicBarrier beginLoadBarrier;
    private CyclicBarrier endLoadBarrier;

    public CacheThread(int threadNum, CachePool cachePool, CyclicBarrier bBarrier,
            CyclicBarrier eBarrier) {
        this.threadNum = threadNum;
        this.cachePool = cachePool;
        this.beginLoadBarrier = bBarrier;
        this.endLoadBarrier = eBarrier;
        this.datas = cachePool.getDatas();
        this.block = cachePool.getBlocks()[threadNum];
        this.totalKvCount = cachePool.getTotalKvCount().get();
    }

    @Override
    public void run() {
        while (true) {
            try {
                beginLoadBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            int loadCursor = cachePool.getLoadCursor();
            if (loadCursor >= totalKvCount) return;
            doLoad(loadCursor);

            try {
                endLoadBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    private void doLoad(int loadCursor) {
        /*int startIndex = loadCursor + threadNum * Constant.BLOCK_SIZE;
        if (startIndex >= totalKvCount) return;

        int tmpEnd = startIndex + Constant.BLOCK_SIZE;
        int endIndex = tmpEnd > totalKvCount ? totalKvCount : tmpEnd;
        int mapIndex = (loadCursor / Constant.ONE_CACHE_SIZE) & (Constant.MAPS_PER_BLOCK - 1);
        LongObjectHashMap<byte[]> map = block.getMaps()[mapIndex];
        map.clear();
        for (int i = startIndex; i < endIndex; i++) {
            //long keyL = SortIndex.instance.get(i);
            long keyL = SmartSortIndex.instance.get(i);
            int modulus = (int) (keyL & (datas.length - 1));
            Data data = datas[modulus];
            try {
                byte[] bytes = data.readValue(data.get(keyL));
                map.put(keyL, bytes);
            } catch (EngineException e) {
                System.out.println("during load to cache : read value IO exception!!!");
            }
        }*/
    }
}

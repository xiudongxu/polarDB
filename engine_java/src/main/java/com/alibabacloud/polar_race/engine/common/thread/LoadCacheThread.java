package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

/**
 * 有些线程没有停掉，会一直阻塞，没有想到好的方法
 *
 * @author wangshuo
 * @version 2018-11-23
 */
public class LoadCacheThread extends Thread {

    private Data[] datas;
    private int threadNum;
    private boolean firstLoad = true;
    private Semaphore loadSemaphore;
    private CountDownLatch downLatch;
    private CyclicBarrier loadBarrier;
    private LongObjectHashMap<byte[]> map;

    public LoadCacheThread(Data[] datas, int threadNum, Semaphore loadSemaphore,
            CyclicBarrier loadBarrier, LongObjectHashMap<byte[]> map, CountDownLatch downLatch) {
        this.datas = datas;
        this.threadNum = threadNum;
        this.loadSemaphore = loadSemaphore;
        this.loadBarrier = loadBarrier;
        this.map = map;
        this.downLatch = downLatch;
    }

    @Override
    public void run() {
        int loadCursor = this.threadNum * Constant.CACHE_SIZE;
        while (true) {
            try {
                loadSemaphore.acquire();
                map.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (loadCursor >= Constant.TOTAL_KV_COUNT) {
                loadBarrierAwait();
                return;
            }
            for (int i = loadCursor; i < loadCursor + Constant.CACHE_SIZE; i++) {
                long key = SortIndex.instance.get(i);
                if (key == Long.MAX_VALUE) {
                    loadBarrierAwait();
                    return;
                }
                try {
                    int modulus = (int) (key & (datas.length - 1));
                    Data data = datas[modulus];
                    byte[] bytes = data.readValue(data.get(key));
                    map.put(key, bytes);
                } catch (EngineException e) {
                    System.out.println("during load cache : read value IO exception!!!");
                }
            }

            loadCursor += Constant.TOTAL_CACHE_COUNT;
            loadBarrierAwait();
        }
    }

    private void loadBarrierAwait() {
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

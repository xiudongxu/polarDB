package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

/**
 * @author wangshuo
 * @version 2018-11-23
 */
public class LoadCacheThread extends Thread {

    private Data[] datas;
    private int threadNum;
    private Semaphore loadSemaphore;
    private CyclicBarrier loadBarrier;
    private LongObjectHashMap<byte[]> map;

    public LoadCacheThread(Data[] datas, int threadNum, Semaphore loadSemaphore,
            CyclicBarrier loadBarrier, LongObjectHashMap<byte[]> map) {
        this.datas = datas;
        this.threadNum = threadNum;
        this.loadSemaphore = loadSemaphore;
        this.loadBarrier = loadBarrier;
        this.map = map;
    }

    @Override
    public void run() {
        int loadCount = this.threadNum;
        while (true) {
            try {
                loadSemaphore.acquire();
                map.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int startIndex = loadCount * Constant.CACHE_SIZE;
            for (int i = loadCount; i < loadCount + Constant.CACHE_SIZE; i++) {
                long key = SortIndex.instance.get(i);
                if (key == Long.MAX_VALUE) {
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

            loadCount += Constant.CACHE_SIZE;
            try {
                loadBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }
}

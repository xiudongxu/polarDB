package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class EngineRace extends AbstractEngine {

    private boolean noWrite = true; //为了看日志，这些天提交一直没有有价值的日志
    private boolean noRead = true;

    private Data[] datas;
    private boolean ranged;
    private int rangeCount = 1;
    private CachePool cachePool;

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            datas = EngineBoot.initDataFile(path);
            cachePool = EngineBoot.initCachePool(datas);
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "init data file IO exception!!!");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        if (noWrite) {
            noWrite = false;
            System.out.println("start write key value at " + LocalDateTime.now());
        }
        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        data.storeKV(key, value);
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        if (noRead) {
            noRead = false;
            System.out.println("start read key value at " + LocalDateTime.now());
        }
        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        int offset = data.get(keyL);
        if (offset == 0) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found the value");
        }
        return data.readValue(offset);
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {
        try {
            if (!ranged) {
                rangeBarrier.await();
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        long tmp = -1L; // key 为 -1 和 Long.MAX_VALUE 不可能吗？
        int[] range = SortIndex.instance.range(lower, upper);
        System.out.println("start range from:" + range[0] + " end:" + range[1]);
        for (int i = range[0]; i <= range[1]; i += Constant.ONE_CACHE_SIZE) {
            try {
                beginReadBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            int tmpEnd = i + Constant.ONE_CACHE_SIZE;
            int endIndex = tmpEnd > Constant.TOTAL_KV_COUNT ? Constant.TOTAL_KV_COUNT : tmpEnd;
            for (int j = i; j < endIndex; j++) {
                long key = SortIndex.instance.get(j);
                if (key == Long.MAX_VALUE) break;
                if (tmp == key) continue;
                tmp = key;

                int blockIndex = (j % Constant.ONE_CACHE_SIZE) / Constant.BLOCK_SIZE;
                int mapIndex = (j / Constant.ONE_CACHE_SIZE) & (Constant.MAPS_PER_BLOCK - 1);
                byte[] value = cachePool.getBlocks()[blockIndex].getMaps()[mapIndex].get(key);
                visitor.visit(ByteUtil.long2Bytes(key), value);
            }
            System.out.println("now read cursor:" + i);

            try {
                endReadBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        try {
            EngineBoot.closeDataFile(datas);
        } catch (IOException e) {
            System.out.println("close file resource error");
        }
    }

    public Data[] getDatas() {
        return datas;
    }

    private CountDownLatch loadDownLatch = new  CountDownLatch(Constant.THREAD_COUNT + 1);

    private CyclicBarrier rangeBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            ranged = true;
            EngineBoot.loadDataToCachePool(cachePool, beginLoadBarrier, endLoadBarrier, loadDownLatch);
        }
    });

    private CyclicBarrier beginLoadBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                if (cachePool.getLoadCursor() == 0 && rangeCount == 1) {
                    loadDownLatch.countDown();
                    cachePool.setLoadCursor(Constant.ONE_CACHE_SIZE * 2);
                }
                if (Constant.CACHE_CAP - (cachePool.getLoadCursor() - cachePool.getReadCursor()) <= 0) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            endLoadBarrier.reset();
        }
    });

    private CyclicBarrier endLoadBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                int newLoadCursor = cachePool.getLoadCursor() + Constant.ONE_CACHE_SIZE;
                if (newLoadCursor >= Constant.TOTAL_KV_COUNT && rangeCount == 1) {
                    newLoadCursor += Constant.CACHE_CAP;
                }
                cachePool.setLoadCursor(newLoadCursor);
                cachePool.notify();
            }
            beginLoadBarrier.reset();
        }
    });

    private CyclicBarrier beginReadBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                if (cachePool.getReadCursor() - cachePool.getLoadCursor() >= 0) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            endReadBarrier.reset();
        }
    });

    private CyclicBarrier endReadBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                int newReadCursor = cachePool.getReadCursor() + Constant.ONE_CACHE_SIZE;
                if (newReadCursor >= Constant.TOTAL_KV_COUNT && rangeCount == 1) {
                    cachePool.setReadCursor(0);
                    cachePool.setLoadCursor(0);
                    rangeCount++;
                } else {
                    cachePool.setReadCursor(newReadCursor);
                }
                if (Constant.CACHE_CAP - (cachePool.getLoadCursor() - cachePool.getReadCursor()) > 0) {
                    cachePool.notify();
                }
            }
            beginReadBarrier.reset();
        }
    });
}

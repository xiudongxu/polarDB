package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import javafx.util.Pair;

public class EngineRace extends AbstractEngine {

    private int writeCount = 0; //为了看日志，这些天提交一直没有有价值的日志
    private int readCount = 0;

    private Object lock = new Object();
    private Data[] datas;
    private boolean loaded;
    private int totalKvCount;
    private CachePool cachePool;

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            datas = EngineBoot.initDataFile(path);
            if (datas[0].getSubscript() > 0) {
                cachePool = EngineBoot.initCachePool(datas);
                EngineBoot.loadAndSortIndex(cachePool);
                totalKvCount = cachePool.getTotalKvCount().get();
            }
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "init data file IO exception!!!");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        if (writeCount < 10) {
            System.out.println("write key value count :" + writeCount);
            writeCount++;
        }

        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        data.storeKV(key, value);
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        if (readCount < 10) {
            System.out.println("read key value count :" + readCount);
            readCount++;
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
        System.out.println(Thread.currentThread().getName() + " start range from:"
                + Arrays.toString(lower) + " end:" + Arrays.toString(upper));

        if (!loaded) {
            synchronized (lock) {
                if (!loaded) {
                    EngineBoot.loadToCachePool(cachePool, beginLoadBarrier, endLoadBarrier);
                    loaded = true;
                }
            }
        }

        Pair<Integer, Integer> pair = SortIndex.instance.range(lower, upper);
        System.out.println("start range from:" + pair.getKey() + " end:" + pair.getValue());
        for (int i = pair.getKey(); i <= pair.getValue(); i += Constant.ONE_CACHE_SIZE) {
            try {
                beginReadBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            int tmpEnd = i + Constant.ONE_CACHE_SIZE;
            int endIndex = tmpEnd > totalKvCount ? totalKvCount : tmpEnd;
            for (int readIndex = i; readIndex < endIndex; readIndex++) {
                long key = SortIndex.instance.get(readIndex);

                int blockIndex = (readIndex % Constant.ONE_CACHE_SIZE) / Constant.BLOCK_SIZE;
                int mapIndex = (readIndex / Constant.ONE_CACHE_SIZE) & (Constant.MAPS_PER_BLOCK - 1);
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

    private CyclicBarrier beginLoadBarrier = new CyclicBarrier(Constant.RANGE_THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                if (Constant.CACHE_CAP - (cachePool.getLoadCursor() - cachePool.getReadCursor()) <= 0) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    });

    private CyclicBarrier endLoadBarrier = new CyclicBarrier(Constant.RANGE_THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                int newLoadCursor = cachePool.getLoadCursor() + Constant.ONE_CACHE_SIZE;
                newLoadCursor = newLoadCursor >= totalKvCount ? totalKvCount : newLoadCursor;
                cachePool.setLoadCursor(newLoadCursor);
                cachePool.notify();
            }
        }
    });

    private CyclicBarrier beginReadBarrier = new CyclicBarrier(Constant.RANGE_THREAD_COUNT, new Runnable() {
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
        }
    });

    private CyclicBarrier endReadBarrier = new CyclicBarrier(Constant.RANGE_THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            synchronized (cachePool) {
                int newReadCursor = cachePool.getReadCursor() + Constant.ONE_CACHE_SIZE;
                if (newReadCursor >= totalKvCount) {
                    loaded = false;
                    cachePool.setReadCursor(0);
                    cachePool.setLoadCursor(0);
                    cachePool.notify();
                } else {
                    cachePool.setReadCursor(newReadCursor);
                    cachePool.notify();
                }
            }
        }
    });

    public Data[] getDatas() {
        return datas;
    }
}

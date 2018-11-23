package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class EngineRace extends AbstractEngine {

    private Data[] datas;
    private CachePool cachePool;

    private CyclicBarrier cyclicBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            cachePool.setReadCursor(cachePool.getReadCursor() + Constant.CACHE_SIZE);
            synchronized (cachePool){
                cachePool.notify();
            }
            readBarrier.reset();
        }
    });

    private CyclicBarrier readBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            if (cachePool.getReadCursor() >= cachePool.getLoadCursor()) {
                synchronized (cachePool) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            cyclicBarrier.reset();
        }
    });

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            datas = EngineBoot.initDataFile(path);
            cachePool = EngineBoot.initCachePool(datas);
            EngineBoot.loadCache(cachePool);
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "init data file IO exception!!!");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        data.storeKV(key, value);
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
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
        long tmp = -1L; // key 为 -1 和 Long.MAX_VALUE 不可能吗？
        int[] range = SortIndex.instance.range(lower, upper);

        for (int i = range[0]; i <= range[1]; i += Constant.CACHE_SIZE) {
            try {
                readBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            int mapIndex = (i / Constant.CACHE_SIZE) & Constant.POOL_COUNT - 1;
            for (int j = i; j < i + Constant.CACHE_SIZE; j++) {
                long key = SortIndex.instance.get(j);
                if (key == Long.MAX_VALUE) break;
                if (tmp == key) continue;
                tmp = key;
                byte[] value = cachePool.getMaps()[mapIndex].get(key);
                visitor.visit(ByteUtil.long2Bytes(key), value);
            }

            try {
                cyclicBarrier.await();
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
}

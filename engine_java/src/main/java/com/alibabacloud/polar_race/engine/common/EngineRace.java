package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.cache.CachePool;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

public class EngineRace extends AbstractEngine {

    private Data[] datas;
    private CachePool cachePool;

    private LongObjectHashMap<byte[]>[] maps;
    private boolean firstRead = true;

    private Semaphore readSemaphore = new Semaphore(Constant.THREAD_COUNT);
    private Semaphore loadSemaphore = new Semaphore(Constant.THREAD_COUNT);

    private CyclicBarrier loadBarrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            readBarrier.reset();

            synchronized (cachePool) {
                cachePool.setLoadCursor(cachePool.getLoadCursor() + Constant.ONE_CACHE_SIZE);
            }
        }
    });

    private CyclicBarrier readBarrier = new CyclicBarrier(Constant.THREAD_COUNT, () -> {
        loadBarrier.reset();
        loadSemaphore.release(Constant.THREAD_COUNT);
    });

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            datas = EngineBoot.initDataFile(path);
            maps = EngineBoot.initCacheMap(datas, loadSemaphore, loadBarrier);
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
        for (int i = range[0]; i <= range[1]; i += Constant.TOTAL_CACHE_COUNT) {
            if (i >= Constant.TOTAL_KV_COUNT) return;
            try {
                readSemaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int tmpEnd = i + Constant.TOTAL_CACHE_COUNT;
            int endIndex = Constant.TOTAL_KV_COUNT > tmpEnd ? tmpEnd : Constant.TOTAL_KV_COUNT;

            for (int j = i; j < endIndex; j++) {
                long key = SortIndex.instance.get(j);
                if (key == Long.MAX_VALUE) {
                    break;
                }
                if (tmp == key) {
                    continue;
                }
                tmp = key;
                int cacheIndex = (j / Constant.CACHE_SIZE) & (Constant.THREAD_COUNT - 1);
                byte[] value = maps[cacheIndex].get(key);
                visitor.visit(ByteUtil.long2Bytes(key), value);
            }

            try {
                readBarrier.await();
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

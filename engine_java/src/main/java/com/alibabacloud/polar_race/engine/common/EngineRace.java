package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongObjectHashMap;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class EngineRace extends AbstractEngine {

    private Data[] datas;
    private LongObjectHashMap<byte[]>[] maps;
    private CyclicBarrier barrier = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            maps.notify();
        }
    });

    /*private TreeMap<Long,byte[]> cache = new TreeMap<>();
    private AtomicInteger numberInc = new AtomicInteger(0);
    private CacheData[] valueCache = new CacheData[Constant.THREAD_COUNT];
    private CyclicBarrier cyclicBarrier1 = new CyclicBarrier(Constant.THREAD_COUNT, new Runnable() {
        @Override
        public void run() {
            cyclicBarrier2.reset();
        }
    });

    private CyclicBarrier cyclicBarrier2 = new CyclicBarrier(Constant.THREAD_COUNT,
            () -> cyclicBarrier1.reset());*/

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            datas = EngineBoot.initDataFile(path);
            maps = new LongObjectHashMap[Constant.POOL_COUNT];
            for (int i = 0; i < Constant.POOL_COUNT; i++) {
                maps[i] = new LongObjectHashMap<>(Constant.CACHE_SIZE);
            }
            EngineBoot.loadCache(maps, datas);
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
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            int mapIndex = (i / Constant.CACHE_SIZE) & Constant.POOL_COUNT - 1;
            for (int j = i * Constant.CACHE_SIZE; j < (i + 1) * Constant.CACHE_SIZE; j++) {
                long key = SortIndex.instance.get(i);
                if (key == Long.MAX_VALUE) break;
                if (tmp == key) continue;
                tmp = key;
                byte[] value = maps[mapIndex].get(key);
                visitor.visit(ByteUtil.long2Bytes(key), value);
            }
        }
    }

    /*private void visit(AbstractVisitor visitor) {
        for (int i = 0; i < valueCache.length; i++) {
            visitor.visit(valueCache[i].getKey(),valueCache[i].getValue());
        }
    }

    public void readAndSet(long keyL,int number) throws EngineException {
        int modulus = (int) (keyL & (datas.length - 1));
        Data data = datas[modulus];
        int offset = data.get(keyL);
        byte[] bytes = data.readValue(offset);
        valueCache[number] = new CacheData(ByteUtil.long2Bytes(keyL),bytes);
    }*/

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

package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

public class EngineRace extends AbstractEngine {

    private Data[] datas;
    private TreeMap<Long,byte[]> cache = new TreeMap<>();

    /*private AtomicInteger numberInc = new AtomicInteger(0);
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
        for (int i = range[0]; i <= range[1]; i++) {
            long key = SortIndex.instance.get(i);
            if (key == Long.MAX_VALUE) break;
            if (tmp == key) continue;
            tmp = key;

            byte[] value = cache.get(key);
            if (value == null) {
                synchronized (this) {
                    value = cache.get(key);
                    if (value == null) {
                        int modulus = (int) (key & (datas.length - 1));
                        Data data = datas[modulus];
                        try {
                            value = data.readValue(data.get(key));
                        } catch (EngineException e) {
                            System.out.println("during range : read value IO exception!!!");
                        }
                        if (cache.size() == Constant.CACHE_SIZE) {
                            cache.remove(cache.firstKey());
                        }
                        cache.put(key, value);
                    }
                }
            }
            visitor.visit(ByteUtil.long2Bytes(key), value);
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

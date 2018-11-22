package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {

    private Data[] datas;
    private CacheData[] valueCache = new CacheData[64];
    private AtomicInteger numberInc = new AtomicInteger(0);

    private CyclicBarrier cyclicBarrier1 = new CyclicBarrier(64, new Runnable() {
        @Override
        public void run() {
            cyclicBarrier2.reset();
        }
    });
    private CyclicBarrier cyclicBarrier2 = new CyclicBarrier(64, new Runnable() {
        @Override
        public void run() {
            cyclicBarrier1.reset();
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
        int number = numberInc.getAndIncrement();
        CountDownLatch countDownLatch = new CountDownLatch(64);
        try {
            int[] range = SortIndex.instance.range(lower, upper);
            long tmp = -1L;
            for (int i = range[0] + number; i < range[1]; i+=64) {
                long key = SortIndex.instance.get(i);
                if(key == Long.MAX_VALUE){
                    break;
                }
                if (tmp == key) {
                    continue;
                }
                tmp = key;
                readAndSet(key,number);
                cyclicBarrier1.await();
                visit(visitor);
                cyclicBarrier2.await();
            }
        countDownLatch.countDown();
        numberInc.getAndDecrement();
        countDownLatch.await();
        } catch (EngineException | InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    private void visit(AbstractVisitor visitor) {
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

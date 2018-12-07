package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.bytebuf.DirectBufFactory;
import com.alibabacloud.polar_race.engine.common.bytebuf.DirectFileUtils;
import com.alibabacloud.polar_race.engine.common.cache.CacheSlot;
import com.alibabacloud.polar_race.engine.common.cache.RingCachePool;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EngineRace extends AbstractEngine {

    private Object lock = new Object();
    private Data[] datas;
    private boolean loaded;
    private boolean sorted;
    private int totalKvCount;
    private RingCachePool ringCachePool;
    private ExecutorService executorService;

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        try {
            DirectFileUtils.initBlockSize(path);
            executorService = Executors.newFixedThreadPool(Constant.THREAD_COUNT);
            datas = EngineBoot.initDataFile(path, executorService);
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
        if (!loaded) {
            synchronized (lock) {
                if (!sorted) {
                    EngineBoot.loadAndSortIndex(datas, executorService);
                    totalKvCount = SmartSortIndex.instance.getTotalKvCount();
                    sorted = true;
                }
                if (!loaded) {
                    ByteBuffer directBuf = DirectBufFactory.allocateAlign(Constant.BYTE_BUF_CAP);
                    ringCachePool = EngineBoot.initRingCache(datas, directBuf);
                    EngineBoot.loadToCachePool(ringCachePool, executorService);
                    loaded = true;
                }
            }
        }

        int readSlotCursor = ThreadContext.getReadCursor();
        for (int i = 0; i < totalKvCount; i += Constant.SLOT_SIZE) {
            int realCursor = readSlotCursor & (Constant.SLOT_COUNT - 1);
            CacheSlot cacheSlot = ringCachePool.getCacheSlots()[realCursor];
            doRange(cacheSlot, i, visitor, readSlotCursor);
            readSlotCursor++;
        }
    }

    private void doRange(CacheSlot cacheSlot, int startIndex, AbstractVisitor visitor, int readCursor) {
        int generation = (readCursor / Constant.SLOT_COUNT) + 1;
        for (;;) {
            if (generation != cacheSlot.getSlotStatus()) {
                rangeSleep(1);
                continue;
            }
            int tmpEnd = startIndex + Constant.SLOT_SIZE;
            int endIndex = tmpEnd > totalKvCount ? totalKvCount : tmpEnd;
            ByteBuffer slotValues = cacheSlot.getSlotValues().slice();
            for (int i = startIndex, j = 0; i < endIndex; i++, j++) {
                long keyL = SmartSortIndex.instance.get(i);
                slotValues.get(ThreadContext.getBytes());
                visitor.visit(ByteUtil.long2Bytes(keyL), ThreadContext.getBytes());
            }
            cacheSlot.addReadCount(endIndex, readCursor);
            break;
        }
    }

    @Override
    public void close() {
        try {
            EngineBoot.stopLoadCacheThread(executorService);
            EngineBoot.closeDataFile(datas);
        } catch (IOException e) {
            System.out.println("close file resource error");
        }
    }

    private void rangeSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Data[] getDatas() {
        return datas;
    }
}

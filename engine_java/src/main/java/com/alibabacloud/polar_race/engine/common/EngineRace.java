package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

public class EngineRace extends AbstractEngine {

    private Data data;
    private Index index;

    private ReentrantLock mutex = new ReentrantLock();

    @Override
    public void open(String path) throws EngineException {
        init(path);
    }

    private void init(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        data = new Data();
        index = new Index();
        data.setPath(path);
        index.setPath(path);
        try {
            data.makeOffsetMappedFile();
            data.initOffset();
            data.openAndMapFile();
            data.initReadBlockingQueue();
            index.openAndMapFile();
            index.loadIndexFileToIndexMap(data.getOffset());
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "open init IO exception!!!");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            mutex.lock();
            int offset = data.appendValueToDataFile(value);
            index.appendKeyToIndexFile(key);
            mutex.unlock();
            long keyL = ByteUtil.bytes2Long(key);
            index.put(keyL, offset);
        } catch (EngineException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write data IO exception!!!");
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        long keyL = ByteUtil.bytes2Long(key);
        int offset = index.get(keyL);
        if (offset == 0) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found the value");
        }
        return data.getValueFromDataFile(offset);
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {
        Arrays.equals(lower, upper); //比较两个byte的大小
    }

    @Override
    public void close() {
        try {
            index.close();
            data.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

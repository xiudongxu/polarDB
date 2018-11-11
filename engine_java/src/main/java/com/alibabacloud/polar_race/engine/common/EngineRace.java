package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class EngineRace extends AbstractEngine {

    private Index[] indices;
    private BlockingQueue<Data> dataQueue;
    private Map<Integer, BlockingQueue<RandomAccessFile>> readMap;

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }

        try {
            dataQueue = EngineBoot.initDataFile(path);
            indices = EngineBoot.initIndexFile(path);
            readMap = EngineBoot.initReadChannel(path);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "open init IO exception!!!");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        Data data = null;
        try {
            data = dataQueue.take();
            long pointer = data.appendValue(value);
            long keyL = ByteUtil.bytes2Long(key);
            int modulus = (int) (keyL & (indices.length - 1));
            indices[modulus].appendIndex(key, pointer);
            indices[modulus].put(keyL, pointer);
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write data IO exception!!!");
        } finally {
            try {
                dataQueue.put(data);
            } catch (InterruptedException e) {
                System.out.println("put data to queue error");
            }
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        long keyL = ByteUtil.bytes2Long(key);
        int modulus = (int) (keyL & (indices.length - 1));
        long pointer = indices[modulus].get(keyL);
        if (pointer == 0) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "not found the value");
        }

        int fileNo = (int) (pointer >> 32);
        int offset = (int) (pointer & (Long.MAX_VALUE >> 31));
        BlockingQueue<RandomAccessFile> accessFiles = readMap.get(fileNo);
        RandomAccessFile accessFile = null;
        try {
            accessFile = accessFiles.take();
            accessFile.seek((long) (offset - 1) << 12 );
            byte[] bytes = new byte[Constant.VALUE_SIZE];
            accessFile.read(bytes);
            return bytes;
        } catch (InterruptedException | IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "read value IO exception!!!");
        } finally {
            try {
                accessFiles.put(accessFile);
            } catch (InterruptedException e) {
                System.out.println("put access file to queue error");
            }
        }
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {
        Arrays.equals(lower, upper); //比较两个byte的大小
    }

    @Override
    public void close() {
        try {
            EngineBoot.closeDataFile(dataQueue);
            EngineBoot.closeIndexFile(indices);
            EngineBoot.closeReadMap(readMap);
        } catch (IOException | InterruptedException e) {
            System.out.println("close resource error");
        }
    }

}

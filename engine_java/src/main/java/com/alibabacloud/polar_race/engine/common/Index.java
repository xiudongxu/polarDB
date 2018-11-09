package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Index {

    private String path;
    private LongIntHashMap map;

    private FileChannel fileChannel;
    private MappedFile indexMappedFile;
    private ByteBuffer keyBuffer = ByteBuffer.allocate(Constant.keySize);

    public Index() {
        this.map = new LongIntHashMap(64000000,0.99);
    }

    public void openAndMapFile() throws IOException {
        indexMappedFile = new MappedFile(path + File.separator + "INDEX");
        fileChannel = indexMappedFile.getFileChannel();
    }

    public int get(long key) {
        return map.get(key);
    }

    public void put(long key, int offset) {
        map.put(key, offset);
    }

    public void appendKeyToIndexFile(byte[] key) throws EngineException {
        try {
            keyBuffer.put(key);
            keyBuffer.flip();
            fileChannel.write(keyBuffer);
            keyBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write key IO exception!!!");
        }
    }

    public void loadIndexFileToIndexMap(int offset) throws IOException {
        if (offset == 0) return;
        for (int i = 1; i <= offset; i++) {
            fileChannel.read(keyBuffer);
            put(ByteUtil.bytes2Long(keyBuffer.array()), i);
            keyBuffer.clear();
        }
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void close() throws IOException {
        indexMappedFile.close();
    }

}

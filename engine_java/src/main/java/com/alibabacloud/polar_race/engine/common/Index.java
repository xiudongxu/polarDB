package com.alibabacloud.polar_race.engine.common;

import com.carrotsearch.hppc.LongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

public class Index {

    private MappedByteBuffer indexMappedByteBuffer;
    private MappedFile indexMappedFile;

    private final static long IndexFileMaxSize = 512 * 1000 * 1000L;  //因为一个索引的value大小是14B 所以14*640000 =   896 * 100 * 10000L

    private LongIntHashMap map;
    private String path;

    public Index() {
        this.map = new LongIntHashMap(64000000,0.99);
    }

    public void openAndMapFile() throws IOException {
        indexMappedFile = new MappedFile(path + File.separator + "INDEX", 0, IndexFileMaxSize);
        indexMappedByteBuffer = indexMappedFile.getMappedByteBuffer();
    }

    public int get(long key) {
        return map.get(key);
    }

    public void put(long key, int offset) {
        map.put(key, offset);
    }

    public void close() throws IOException {
        indexMappedFile.close();
    }

    public void appendKeyToIndexFile(byte[] key) {
        for (byte aKey : key) {
            indexMappedByteBuffer.put(aKey);
        }
    }

    public void loadIndexFileToIndexMap(int offset)  {
        int OneKeySize = 8;
        if (offset == 0) {
            return;
        }
        byte[] key = new byte[OneKeySize];
        for (int i = 1; i <= offset; i++) {
            for (int j = 0; j < OneKeySize; j++) {
                key[j] = indexMappedByteBuffer.get();
            }
            put(ByteUtil.bytes2Long(key), i);
        }
    }

    public void setPath(String path) {
        this.path = path;
    }
}

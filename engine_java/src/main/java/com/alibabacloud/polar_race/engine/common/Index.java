package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongLongHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Index {

    private int fileNo; //文件编号
    private int offset; //当前文件偏移量
    private LongLongHashMap map;

    /** 索引映射到文件 */
    private FileChannel indexFileChannel;
    private MappedFile indexMappedFile;
    private ByteBuffer indexBuffer = ByteBuffer.allocateDirect(Constant.INDEX_SIZE);

    /** 文件标记映射到文件（用于标记每个文件当前偏移量） */
    private FileChannel markFileChannel;
    private MappedFile markMappedFile;
    private ByteBuffer markBuffer = ByteBuffer.allocateDirect(Constant.INDEX_MARK_SIZE);

    public Index(String path, int fileNo) throws IOException {
        this.fileNo = fileNo;
        //设置索引Map
        this.map = new LongLongHashMap(Constant.INIT_MAP_CAP, 0.99);

        //获取标记文件channel
        markMappedFile = new MappedFile(path + File.separator + "INDEX_MARK");
        markFileChannel = markMappedFile.getFileChannel();
        markFileChannel = markFileChannel.position(fileNo << 2);

        //获取索引文件的偏移量
        markFileChannel.read(markBuffer);
        markBuffer.flip();
        offset = markBuffer.remaining() > 0 ? markBuffer.getInt() : offset;
        markBuffer.clear();
        markFileChannel = markFileChannel.position(fileNo << 2);

        //获取索引文件channel
        indexMappedFile = new MappedFile(path + File.separator + "INDEX_" + fileNo);
        indexFileChannel = indexMappedFile.getFileChannel();
        //加载索引文件到Map
        loadIndexToMap();
    }

    private void loadIndexToMap() throws IOException {
        if (offset == 0) return;
        for (int i = 1; i <= offset; i++) {
            indexFileChannel.read(indexBuffer);
            indexBuffer.flip();
            put(indexBuffer.getLong(), indexBuffer.getLong());
            indexBuffer.clear();
        }
    }

    public long get(long key) {
        return map.get(key);
    }

    public synchronized void put(long key, long offset) {
        map.put(key, offset);
    }

    public synchronized void appendIndex(byte[] key, long pointer) throws EngineException {
        doAppendIndex(key, pointer);
        offset++;
        updateMark();
    }

    private void doAppendIndex(byte[] key, long pointer) throws EngineException {
        try {
            indexBuffer.put(key);
            indexBuffer.putLong(pointer);
            indexBuffer.flip();
            indexFileChannel.write(indexBuffer);
            indexBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write key IO exception!!!");
        }
    }

    private void updateMark() throws EngineException {
        try {
            markBuffer.put(ByteUtil.int2byte(offset));
            markBuffer.flip();
            markFileChannel.write(markBuffer);
            markBuffer.clear();
            markFileChannel = markFileChannel.position(fileNo << 2);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write offset IO exception!!!");
        }
    }

    public void close() throws IOException {
        markMappedFile.close();
        indexMappedFile.close();
    }

}

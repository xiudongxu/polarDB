package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 数据对存储相关
 * 每个 Data 实例包含存储 key 和 value 的文件对，以及一个索引 map :
 * key -> offset 其中 key 为实际的 key，offset 为存储在 value 文件的偏移量
 */
public class Data {
    private int offset; //文件内偏移量
    private LongIntHashMap map; //key -> offset map

    /** value 文件 */
    private MappedFile valueMappedFile;
    private FileChannel valueFileChannel;
    private ByteBuffer valueBuffer = ByteBuffer.allocateDirect(Constant.VALUE_SIZE);

    /** key 文件：首四字节存储偏移量，后面追加 key */
    private MappedFile keyMappedFile;
    private FileChannel keyFileChannel;
    private ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Constant.KEY_SIZE);
    private ByteBuffer offsetBuffer = ByteBuffer.allocateDirect(Constant.OFFSET_SIZE);

    public Data(String path, int fileNo) throws IOException {
        this.map = new LongIntHashMap(Constant.INIT_MAP_CAP, 0.99);

        //创建 key 的存储文件，并获取偏移量
        keyMappedFile = new MappedFile(path + File.separator + "KEY_" + fileNo);
        keyFileChannel = keyMappedFile.getFileChannel();
        keyFileChannel.read(offsetBuffer);
        offsetBuffer.flip();
        offset = offsetBuffer.remaining() > 0 ? offsetBuffer.getInt() : offset;
        offsetBuffer.clear();

        //创建 value 的存储文件，并设置其偏移量
        valueMappedFile = new MappedFile(path + File.separator + "VALUE_" + fileNo);
        valueFileChannel = valueMappedFile.getFileChannel();
        valueFileChannel = valueFileChannel.position((long) offset << 12);

        //加载 key
        loadKeyToMap();
    }

    private void loadKeyToMap() throws IOException {
        if (offset == 0) return;
        keyFileChannel.position(Constant.OFFSET_SIZE); //设置key的读取位置
        for (int i = 1; i <= offset; i++) {
            keyFileChannel.read(keyBuffer);
            keyBuffer.flip();
            map.put(keyBuffer.getLong(), i);
            keyBuffer.clear();
        }
    }

    public synchronized void storeKV(byte[] key, byte[] value) throws EngineException {
        appendValue(value);
        appendKey(key);
        offset++;
        updateOffset();
        put(ByteUtil.bytes2Long(key), offset);
    }

    public int get(long key) {
        return map.get(key);
    }

    private void appendValue(byte[] value) throws EngineException {
        try {
            valueBuffer.put(value);
            valueBuffer.flip();
            valueFileChannel.write(valueBuffer);
            valueBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write value IO exception!!!" + e.getMessage());
        }
    }

    private void appendKey(byte[] key) throws EngineException {
        try {
            keyFileChannel.position(Constant.OFFSET_SIZE + (offset << 3)); //设置key的追加位置
            keyBuffer.put(key);
            keyBuffer.flip();
            keyFileChannel.write(keyBuffer);
            keyBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write key IO exception!!!");
        }
    }

    private void updateOffset() throws EngineException {
        try {
            keyFileChannel.position(0); //设置偏移量的写入位置
            offsetBuffer.put(ByteUtil.int2byte(offset));
            offsetBuffer.flip();
            keyFileChannel.write(offsetBuffer);
            offsetBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write offset to mark file IO exception!!!");
        }
    }

    private void put(long key, int offset) {
        map.put(key, offset);
    }

    public void close() throws IOException {
        valueMappedFile.close();
        keyMappedFile.close();
    }
}

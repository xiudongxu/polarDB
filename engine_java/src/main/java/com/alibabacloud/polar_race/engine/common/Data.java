package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据对存储相关
 * 每个 Data 实例包含存储 key 和 value 的文件对，以及一个索引 map :
 * key -> offset 其中 key 为实际的 key，offset 为存储在 value 文件的偏移量
 */
public class Data {
    private AtomicInteger subscript; //key/value 下标
    private LongIntHashMap map; //key -> offset map

    /** value 文件 */
    private MappedFile valueMappedFile;
    private FileChannel valueFileChannel;
    private ByteBuffer valueBuffer = ByteBuffer.allocateDirect(Constant.VALUE_SIZE);

    /** key 文件：首四字节存储偏移量，后面追加 key */
    private MappedFile keyMappedFile;
    private FileChannel keyFileChannel;
    private ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Constant.KEY_SIZE);

    private RandomAccessFile accessFile;

    public Data(String path, int fileNo) throws IOException {
        this.map = new LongIntHashMap(Constant.INIT_MAP_CAP, 0.99);

        //创建 key 的存储文件，并获取偏移量
        keyMappedFile = new MappedFile(path + File.separator + "KEY_" + fileNo);
        keyFileChannel = keyMappedFile.getFileChannel();
        subscript = new AtomicInteger((int) (keyFileChannel.size() >> 2 - 1));

        //创建 value 的存储文件，并设置其偏移量
        valueMappedFile = new MappedFile(path + File.separator + "VALUE_" + fileNo);
        valueFileChannel = valueMappedFile.getFileChannel();

        //加载 key
        loadKeyToMap();

        //访问数据
        accessFile = new RandomAccessFile(path + File.separator + "VALUE_" + fileNo, "r");
    }

    private void loadKeyToMap() throws IOException {
        if (subscript.get() < 0) return;
        for (int i = 0; i <= subscript.get(); i++) {
            keyFileChannel.read(keyBuffer);
            keyBuffer.flip();
            map.put(keyBuffer.getLong(), i);
            keyBuffer.clear();
        }
    }

    public void storeKV(byte[] key, byte[] value) throws EngineException {
        int newSubscript = subscript.addAndGet(1);
        appendValue(value, newSubscript << 12);
        appendKey(key, newSubscript << 2);
        put(ByteUtil.bytes2Long(key), newSubscript);
    }

    public synchronized byte[] readValue(int offset) throws EngineException {
        try {
            accessFile.seek(offset);
            byte[] bytes = new byte[Constant.VALUE_SIZE];
            accessFile.read(bytes);
            return bytes;
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "read value IO exception!!!");
        }
    }

    public int get(long key) {
        return map.get(key);
    }

    private void appendValue(byte[] value, long pos) throws EngineException {
        try {
            valueBuffer.put(value);
            valueBuffer.flip();
            valueFileChannel.write(valueBuffer, pos);
            valueBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write value IO exception!!!" + e.getMessage());
        }
    }

    private void appendKey(byte[] key, long pos) throws EngineException {
        try {
            keyBuffer.put(key);
            keyBuffer.flip();
            keyFileChannel.write(keyBuffer, pos);
            keyBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write key IO exception!!!");
        }
    }

    private void put(long key, int offset) {
        map.put(key, offset);
    }

    public void close() throws IOException {
        valueMappedFile.close();
        keyMappedFile.close();
        accessFile.close();
    }
}

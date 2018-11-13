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

    /**
     * value 文件
     */
    private MappedFile valueMappedFile;
    private FileChannel valueFileChannel;
    private ByteBuffer valueBuffer = ByteBuffer.allocateDirect(Constant.VALUE_SIZE);
    private ByteBuffer readBuffer = ByteBuffer.allocate(Constant.VALUE_SIZE);

    /**
     * key 文件：首四字节存储偏移量，后面追加 key
     */
    private MappedFile keyMappedFile;
    private FileChannel keyFileChannel;
    private ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Constant.KEY_SIZE);

    private FileChannel accessFileChannel;

    public Data(String path, int fileNo) throws IOException {
        this.map = new LongIntHashMap(Constant.INIT_MAP_CAP, 0.99);

        //创建 key 的存储文件，并获取偏移量
        keyMappedFile = new MappedFile(path + File.separator + "KEY_" + fileNo);
        keyFileChannel = keyMappedFile.getFileChannel();
        //加载 key
        int offset = 0;
        ByteBuffer keysBuffer = ByteBuffer.allocateDirect(Constant.ONE_LOAD_SIZE);
        while (keyFileChannel.read(keysBuffer) != -1) {
            keysBuffer.flip();
            while (keysBuffer.hasRemaining()) {
                offset++;
                map.put(keysBuffer.getLong(), offset);
            }
            keysBuffer.clear();
        }
        subscript = new AtomicInteger(offset);

        //创建 value 的存储文件，并设置其偏移量
        valueMappedFile = new MappedFile(path + File.separator + "VALUE_" + fileNo);
        valueFileChannel = valueMappedFile.getFileChannel();

        //访问数据
        accessFileChannel = new RandomAccessFile(path + File.separator + "VALUE_" + fileNo, "r")
                .getChannel();
    }

    public void storeKV(byte[] key, byte[] value) throws EngineException {
        int newSubscript = subscript.addAndGet(1);
        appendValueAndKey(value, key, newSubscript);
        put(ByteUtil.bytes2Long(key), newSubscript);
    }

    public synchronized byte[] readValue(int offset) throws EngineException {
        try {
            readBuffer.clear();
            accessFileChannel.read(readBuffer, (long) (offset - 1) << 12);
            return readBuffer.array();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "read value IO exception!!!");
        }
    }

    private synchronized void appendValueAndKey(byte[] value, byte[] key, int offset) throws EngineException {
        try {
            valueBuffer.put(value);
            valueBuffer.flip();
            valueFileChannel.write(valueBuffer, (long) (offset - 1) << 12);
            valueBuffer.clear();

            keyBuffer.put(key);
            keyBuffer.flip();
            keyFileChannel.write(keyBuffer, (offset - 1) << 3);
            keyBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,
                    "write value IO exception!!!" + e.getMessage());
        }
    }

    private void put(long key, int offset) {
        map.put(key, offset);
    }

    public int get(long key) {
        return map.get(key);
    }

    public LongIntHashMap getMap() {
        return map;
    }

    public void close() throws IOException {
        valueMappedFile.close();
        keyMappedFile.close();
        accessFileChannel.close();
    }
}

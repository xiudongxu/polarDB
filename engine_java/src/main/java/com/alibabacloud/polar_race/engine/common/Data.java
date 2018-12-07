package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.bytebuf.DirectFileUtils;
import com.alibabacloud.polar_race.engine.common.cache.CacheSlot;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import sun.misc.Contended;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * 数据对存储相关
 * 每个 Data 实例包含存储 key 和 value 的文件对，以及一个索引 map :
 * key -> offset 其中 key 为实际的 key，offset 为存储在 value 文件的偏移量
 */
@Contended
public class Data {

    @Contended("1")
    private int subscript; //key/value 下标
    @Contended("1")
    private MappedByteBuffer keyMapperByteBuffer;
    @Contended("1")
    private ByteBuffer wirteBuffer = ByteBuffer.allocateDirect(Constant.VALUE_SIZE);

    private long address;
    private LongIntHashMap map; //key -> offset map
    private Unsafe unsafe = UnsafeUtil.getUnsafe();
    /**
     * value 文件
     */
    private MappedFile valueMappedFile;
    private FileChannel valueFileChannel;
    /**
     * key 文件
     */
    private MappedFile keyMappedFile;
    private FileChannel keyFileChannel;

    private RandomAccessFile accessFileForRange;
    //private DirectRandomAccessFile accessFileForRange;

    private int fd;

    public Data(String path, int fileNo) throws IOException {
        this.map = new LongIntHashMap(Constant.INIT_MAP_CAP, 0.99);

        //创建 key 的存储文件，并获取偏移量
        keyMappedFile = new MappedFile(path + File.separator + "KEY_" + fileNo);
        keyFileChannel = keyMappedFile.getFileChannel();

        //创建 value 的存储文件，并设置其偏移量
        valueMappedFile = new MappedFile(path + File.separator + "VALUE_" + fileNo);
        valueFileChannel = valueMappedFile.getFileChannel();

        //创建映射，根据value文件的size 计算共有多少个key
        keyMapperByteBuffer = keyFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.KEY_MAPPED_SIZE);
        subscript = (int) (valueFileChannel.size() >> 12);

        byte[] bytes = new byte[Constant.KEY_SIZE];
        for (int i = 1; i <= subscript; i++) {
            for (int j = 0; j < 8; j++) {
                bytes[j] = keyMapperByteBuffer.get();
            }
            long aLong = ByteUtil.bytes2Long(bytes);
            map.put(aLong, i);
        }
        valueFileChannel.position((long) subscript << 12);

        //accessFileForRange = new DirectRandomAccessFile(path + File.separator + "VALUE_" + fileNo, "r");
        accessFileForRange = new RandomAccessFile(path + File.separator + "VALUE_" + fileNo, "r");
        fd = DirectFileUtils.openReadOnly(path + File.separator + "VALUE_" + fileNo);

        address = ((DirectBuffer) wirteBuffer).address();
    }

    public synchronized void storeKV(byte[] key, byte[] value) throws EngineException {
        appendValue(value);
        appendKey(key);
    }

    private int appendValue(byte[] value) throws EngineException {
        try {
            wirteBuffer.clear();
            unsafe.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address, Constant.VALUE_SIZE);
            long position = valueFileChannel.position();
            valueFileChannel.write(wirteBuffer);
            return (int) ((position >> 12) + 1);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,
                    "write value IO exception!!!" + e.getMessage());
        }
    }

    private void appendKey(byte[] key) {
        keyMapperByteBuffer.put(key);
    }

    public byte[] readValue(int offset) throws EngineException {
        try {
            byte[] bytes = ThreadContext.getBytes();
            synchronized (this) {
                accessFileForRange.seek((long) (offset - 1) << 12);
                accessFileForRange.read(bytes);
            }
            return bytes;
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "read value IO exception!!!");
        }
    }

    public void readForRange(int offset, byte[] rangeValue) throws EngineException {
        try {
            synchronized (this) {
                accessFileForRange.seek((long) (offset - 1) << 12);
                accessFileForRange.read(rangeValue);
            }
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "range value IO exception!!!");
        }
    }

    public void readForRange(int offset, CacheSlot cacheSlot) throws EngineException {
        try {
            ByteBuffer slotValues = cacheSlot.getSlotValues();
            DirectFileUtils.pread(fd, slotValues, (long) (offset - 1) << 12);
            slotValues.get(cacheSlot.getSlotBytes());
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "dio range value IO exception!!!");
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
    }

    public int getSubscript() {
        return subscript;
    }

    public MappedByteBuffer getKeyMapperByteBuffer() {
        return keyMapperByteBuffer;
    }
}

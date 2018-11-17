package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import net.smacke.jaydio.DirectRandomAccessFile;
import sun.misc.Contended;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 数据对存储相关
 * 每个 Data 实例包含存储 key 和 value 的文件对，以及一个索引 map :
 * key -> offset 其中 key 为实际的 key，offset 为存储在 value 文件的偏移量
 */
public class Data {

    @Contended
    private int subscript; //key/value 下标

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
    @Contended
    private MappedByteBuffer keyMapperByteBuffer;

    private DirectRandomAccessFile accessFileChannel;

    @Contended
    private ByteBuffer wirteBuffer = ByteBuffer.allocateDirect(Constant.VALUE_SIZE);

    public Data(String path, int fileNo) throws IOException {
        this.map = new LongIntHashMap(Constant.INIT_MAP_CAP, 0.99);

        //创建 key 的存储文件，并获取偏移量
        keyMappedFile = new MappedFile(path + File.separator + "KEY_" + fileNo);
        keyFileChannel = keyMappedFile.getFileChannel();

        //创建 value 的存储文件，并设置其偏移量
        valueMappedFile = new MappedFile(path + File.separator + "VALUE_" + fileNo);
        valueFileChannel = valueMappedFile.getFileChannel();
        //计算写入了多少个key

//        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Constant.ONE_LOAD_SIZE);
//        while (keyFileChannel.read(keyBuffer) != -1) {
//            keyBuffer.flip();
//            while (keyBuffer.hasRemaining()) {
//                offset++;
//                map.put(keyBuffer.getLong(), offset);
//            }
//            keyBuffer.clear();
//        }
        //务必要后面进行映射 否则上面offet会不准
        keyMapperByteBuffer = keyFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.KEY_MAPPED_SIZE);
        subscript = (int) (valueFileChannel.size() >> 12);
        byte[] bytes = new byte[Constant.KEY_SIZE];
        for (int i = 1; i <= subscript; i++) {
            for (int j = 0; j < 8; j++) {
                bytes[j] = keyMapperByteBuffer.get();
            }
            map.put(ByteUtil.bytes2Long(bytes), i);
        }
        //accessFileChannel = new RandomAccessFile(path + File.separator + "VALUE_" + fileNo, "r").getChannel();
        accessFileChannel = new DirectRandomAccessFile(path + File.separator + "VALUE_" + fileNo, "r");

        address = ((DirectBuffer) wirteBuffer).address();
    }

    public void storeKV(byte[] key, byte[] value) throws EngineException {
        int newSubscript;
        synchronized (this) {
            newSubscript = ++subscript;
            appendValue(value, (long) (newSubscript - 1) << 12);
            appendKey(key);
        }
        put(ByteUtil.bytes2Long(key), subscript);
    }

    public byte[] readValue(int offset) throws EngineException {
        try {
            byte[] bytes = ThreadContext.getBytes();
            synchronized (this) {
                accessFileChannel.seek((long) (offset - 1) << 12);
                accessFileChannel.read(bytes);
            }
            return bytes;
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, "read value IO exception!!!");
        }
    }

    private void appendValue(byte[] value, long pos) throws EngineException {
        try {
            wirteBuffer.clear();
            unsafe.copyMemory(value, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address, Constant.VALUE_SIZE);
            valueFileChannel.write(wirteBuffer,pos);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,
                    "write value IO exception!!!" + e.getMessage());
        }
    }

    private void appendKey(byte[] key) {
        keyMapperByteBuffer.put(key);
    }

    private void put(long key, int offset) {
        map.put(key, offset);
    }

    public int get(long key) {
        return map.get(key);
    }

    public void close() throws IOException {
        valueMappedFile.close();
        keyMappedFile.close();
        accessFileChannel.close();
    }
}

package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author dongxu.xiu
 * @since 2018-10-15 下午6:12
 */
public class Data {
    /**
     * 内存映射管理
     */

    private MappedFile dataMappedFile;
    private MappedByteBuffer dataMappedByteBuffer;

    //管理offset记录
    private MappedFile offsetMappedFile;
    private MappedByteBuffer offsetMappedByteBuffer;


    /**
     * offset：偏移量
     * path：  路径
     * DataFileSubfixNow：当前所写入的数据文件的后缀
     */
    private int offset = 0;
    private int dataOffsetNow = 0;
    private String path;
    /**
     * **********常******量******值************
     * DataFileMaxOffset：文件最大offset 250000
     * OneValueSize：     一个value的size 4096
     * DataFileMaxSize：  一个数据文件的最大size
     */
    private final int OneMapMaxOffset = 125000;  //12 5000
    private final int OneValueSize = 4096;  //4*1024
    private final long DataFileMaxSize = OneMapMaxOffset << 12; // 500 * 1000 * 1024

    private ArrayBlockingQueue fileChannels = new ArrayBlockingQueue<RandomAccessFile>(64);

    public RandomAccessFile getFileChannels() throws InterruptedException {
        return (RandomAccessFile) fileChannels.take();
    }
    public void putFileChannel(RandomAccessFile randomAccessFile) throws EngineException {
        try {
            fileChannels.put(randomAccessFile);
        } catch (InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,"fileChannel put exception");
        }
    }

    public int getOffset() {
        return offset;
    }

    public void close() throws IOException {
        offsetMappedFile.close();
        dataMappedFile.close();
    }

    public void openAndMapFile() throws IOException {
        dataMappedFile = new MappedFile(path + File.separator + "DATA", (long) (offset) << 12, DataFileMaxSize);
        dataMappedByteBuffer = dataMappedFile.getMappedByteBuffer();
    }

    public  byte[] getValueFromDataFile(int offset) throws EngineException {
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = getFileChannels();
            byte[] bytes = new byte[4096];
            randomAccessFile.seek((long)(offset-1) << 12);
            randomAccessFile.read(bytes);
            //ByteBuffer byteBuffer = ByteBuffer.allocate(OneValueSize);
            //channel.position((long)(offset-1) << 12);
            //channel.read(byteBuffer);
            return bytes;
        } catch (IOException | InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,"read value IO exception!!!");
        } finally {
            putFileChannel(randomAccessFile);
        }
    }

    public int appendValueToDataFile(byte[] value) throws EngineException {
        dataOffsetNow++;
        offset++;
        writeOffset(offset);
        wirteData(value);
        if (dataOffsetNow == OneMapMaxOffset) {
            reMapDataFile();
        }
        return offset;
    }

    private void reMapDataFile() throws EngineException {
        dataMappedFile.openNewMap((long)(offset) << 12, DataFileMaxSize);
        dataMappedByteBuffer = dataMappedFile.getMappedByteBuffer();
        dataOffsetNow = 0;
    }

    private void wirteData(byte[] value) {
        for (byte aValue : value) {
            dataMappedByteBuffer.put(aValue);
        }
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void makeOffsetMappedFile() throws IOException {
        offsetMappedFile = new MappedFile(path + File.separator + "OFFSET", 0, 8);
        offsetMappedByteBuffer = offsetMappedFile.getMappedByteBuffer();
    }

    public void initOffset() {
        byte[] bytes1 = new byte[4];
        for (int i = 0; i < 4; i++) {
            bytes1[i] = offsetMappedByteBuffer.get();
        }
        byte[] bytes2 = new byte[4];
        for (int i = 0; i < 4; i++) {
            bytes2[i] = offsetMappedByteBuffer.get();
        }
        offsetMappedByteBuffer.position(0);
        offset = Math.max(ByteUtil.byte2int(bytes1), ByteUtil.byte2int(bytes2));
    }

    private void writeOffset(int offset) {
        byte[] bytes = ByteUtil.int2byte(offset);
        for (byte aByte : bytes) {
            offsetMappedByteBuffer.put(aByte);
        }
        if (offsetMappedByteBuffer.position() == 8) {
            offsetMappedByteBuffer.position(0);
        }
    }

    public void initReadBlockingQueue() throws FileNotFoundException{
        for (int i = 0; i < 64; i++) {
            RandomAccessFile randomAccessFile = new RandomAccessFile(path + File.separator + "DATA", "r");
            fileChannels.offer(randomAccessFile);
        }
    }
}

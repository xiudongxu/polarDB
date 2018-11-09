package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author dongxu.xiu
 * @since 2018-10-15 下午6:12
 */
public class Data {

    /**
     * offset：偏移量
     * path  ：路径
     */
    private int offset = 0;
    private String path;

    /**
     * 内存映射管理
     */
    private FileChannel dataFileChannel;
    private MappedFile dataMappedFile;
    private ByteBuffer valueBuffer = ByteBuffer.allocate(Constant.valueSize);

    /**
     * 管理offset记录
     */
    private FileChannel offsetFileChannel;
    private MappedFile offsetMappedFile;
    private ByteBuffer offsetBuffer = ByteBuffer.allocate(Constant.offsetBytes);

    private ArrayBlockingQueue fileChannels = new ArrayBlockingQueue<RandomAccessFile>(64);

    public  byte[] getValueFromDataFile(int offset) throws EngineException {
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = getFileChannels();
            byte[] bytes = new byte[Constant.valueSize];
            randomAccessFile.seek((long) (offset - 1) << 12);
            randomAccessFile.read(bytes);
            return bytes;
        } catch (IOException | InterruptedException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR,"read value IO exception!!!");
        } finally {
            putFileChannel(randomAccessFile);
        }
    }

    public int appendValueToDataFile(byte[] value) throws EngineException {
        offset++;
        writeOffset(offset);
        writeData(value);
        return offset;
    }

    private void writeData(byte[] value) throws EngineException {
        try {
            valueBuffer.put(value);
            valueBuffer.flip();
            dataFileChannel.write(valueBuffer);
            valueBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write data IO exception!!!");
        }
    }

    public void makeOffsetMappedFile() throws IOException {
        offsetMappedFile = new MappedFile(path + File.separator + "OFFSET");
        offsetFileChannel = offsetMappedFile.getFileChannel();
        offsetFileChannel.read(offsetBuffer);
        offsetBuffer.flip();
    }

    public void initOffset() {
        offset = ByteUtil.byte2int(offsetBuffer.array());
        offsetBuffer.clear();
    }

    public void openAndMapFile() throws IOException {
        dataMappedFile = new MappedFile(path + File.separator + "DATA");
        dataFileChannel = dataMappedFile.getFileChannel();
        dataFileChannel = dataFileChannel.position(offset << 12);
    }

    private void writeOffset(int offset) throws EngineException {
        try {
            offsetBuffer.put(ByteUtil.int2byte(offset));
            offsetBuffer.flip();
            offsetFileChannel.write(offsetBuffer);
            offsetBuffer.clear();
            offsetFileChannel = offsetFileChannel.position(0);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write offset IO exception!!!");
        }
    }

    public void initReadBlockingQueue() throws FileNotFoundException{
        for (int i = 0; i < 64; i++) {
            RandomAccessFile randomAccessFile = new RandomAccessFile(path + File.separator + "DATA", "r");
            fileChannels.offer(randomAccessFile);
        }
    }

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

    public void setPath(String path) {
        this.path = path;
    }

    public void close() throws IOException {
        offsetMappedFile.close();
        dataMappedFile.close();
    }
}

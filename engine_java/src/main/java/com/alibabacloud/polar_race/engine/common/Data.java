package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author dongxu.xiu
 * @since 2018-10-15 下午6:12
 */
public class Data {
    private int fileNo; //文件编号
    private int offset; //当前文件内偏移量

    /** 数据映射到文件 */
    private FileChannel dataFileChannel;
    private MappedFile dataMappedFile;
    private ByteBuffer valueBuffer = ByteBuffer.allocate(Constant.VALUE_SIZE);

    /** 文件标记映射到文件 */
    private FileChannel markFileChannel;
    private MappedFile markMappedFile;
    private ByteBuffer markBuffer = ByteBuffer.allocate(Constant.VALUE_MARK_SIZE);

    public Data(String path, int fileNo) throws IOException {
        this.fileNo = fileNo;

        //获取标记文件channel
        markMappedFile = new MappedFile(path + File.separator + "DATA_MARK");
        markFileChannel = markMappedFile.getFileChannel();
        markFileChannel = markFileChannel.position(fileNo << 2);

        //获取数据文件的偏移量
        markFileChannel.read(markBuffer);
        markBuffer.flip();
        offset = ByteUtil.byte2int(markBuffer.array());
        markBuffer.clear();
        markFileChannel = markFileChannel.position(fileNo << 2);

        //获取数据文件channel并设置数据文件的偏移量
        dataMappedFile = new MappedFile(path + File.separator + "DATA_" + fileNo);
        dataFileChannel = dataMappedFile.getFileChannel();
        dataFileChannel = dataFileChannel.position(offset << 12);
    }

    /**
     * 文件末尾追加 value
     * @return 返回指向 value 的"指针"
     * ps: 高四个字节表示数据文件编号，低四字节表示 value 在文件中的偏移量
     */
    public long appendValue(byte[] value) throws EngineException {
        doAppendValue(value);
        offset++;
        updateMark();
        long pointer = (long) fileNo << 32;
        return pointer | offset;
    }

    private void doAppendValue(byte[] value) throws EngineException {
        try {
            valueBuffer.put(value);
            valueBuffer.flip();
            dataFileChannel.write(valueBuffer);
            valueBuffer.clear();
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write value IO exception!!!" + e.getMessage());
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
            throw new EngineException(RetCodeEnum.IO_ERROR, "write offset to mark file IO exception!!!");
        }
    }

    public void close() throws IOException {
        markMappedFile.close();
        dataMappedFile.close();
    }
}

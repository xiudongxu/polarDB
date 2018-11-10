package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author dongxu.xiu
 * @since 2018-10-17 下午4:24
 */
public class MappedFile {

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    public MappedFile(String fileName) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void close() throws IOException {
        randomAccessFile.close();
        randomAccessFile = null;
        fileChannel.close();
        fileChannel = null;
    }
}

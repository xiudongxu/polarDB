package com.alibabacloud.polar_race.engine.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author wangshuo
 * @version 2018-11-10
 */
public class EngineBoot {

    public static Data[] initDataFile(String path) throws IOException {
        Data[] datas = new Data[Constant.DATA_FILE_COUNT];
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            datas[i] = new Data(path, i);
        }
        return datas;
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }

    /**
     * 为每个 value 文件开启若干个访问通道
     */
    public static Map<Integer, BlockingQueue<RandomAccessFile>> initReadChannel(String path)
            throws FileNotFoundException {
        Map<Integer, BlockingQueue<RandomAccessFile>> map = new HashMap<>();
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            BlockingQueue<RandomAccessFile> queue = new ArrayBlockingQueue<>(Constant.ACCESS_FILE_COUNT);
            map.put(i, queue);
            for (int j = 0; j < Constant.ACCESS_FILE_COUNT; j++) {
                queue.offer(new RandomAccessFile(path + File.separator + "VALUE_" + i, "r"));
            }
        }
        return map;
    }

    public static void closeReadMap(Map<Integer, BlockingQueue<RandomAccessFile>> readMap)
            throws IOException {
        for (BlockingQueue<RandomAccessFile> files : readMap.values()) {
            for (RandomAccessFile file : files) {
                file.close();
            }
        }
    }
}

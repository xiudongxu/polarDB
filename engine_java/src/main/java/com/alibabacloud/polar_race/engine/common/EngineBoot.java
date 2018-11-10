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

    public static BlockingQueue<Data> initDataFile(String path) throws IOException {
        BlockingQueue<Data> dataBlockingQueue = new ArrayBlockingQueue<>(Constant.DATA_FILE_COUNT);
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            dataBlockingQueue.offer(new Data(path, i));
        }
        return dataBlockingQueue; 
    }

    public static Index[] initIndexFile(String path) throws IOException {
        Index[] indices = new Index[Constant.DATA_FILE_COUNT];
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            indices[i] = new Index(path, i);
        }
        return indices;
    }

    /**
     * 为每一个数据文件绑定两个随机读channel
     */
    public static Map<Integer, BlockingQueue<RandomAccessFile>> initReadChannel(String path)
            throws FileNotFoundException {
        Map<Integer, BlockingQueue<RandomAccessFile>> map = new HashMap<>();
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            BlockingQueue<RandomAccessFile> queue = new ArrayBlockingQueue<>(Constant.ACCESS_FILE_COUNT);
            map.put(i, queue);
            for (int j = 0; j < Constant.ACCESS_FILE_COUNT; j++) {
                queue.offer(new RandomAccessFile(path + File.separator + "DATA_" + i, "r"));
            }
        }
        return map;
    }

    public static void closeDataFile(BlockingQueue<Data> queue)
            throws IOException, InterruptedException {
        for (Data next : queue) {
            next.close();
        }
    }

    public static void closeIndexFile(Index[] indices) throws IOException {
        for (Index index : indices) {
            index.close();
        }
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

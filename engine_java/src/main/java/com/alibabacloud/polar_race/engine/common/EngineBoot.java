package com.alibabacloud.polar_race.engine.common;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-10
 */
public class EngineBoot {

    public static Data[] initDataFile(String path) throws InterruptedException {
        Data[] datas = new Data[Constant.DATA_FILE_COUNT];
        CountDownLatch downLatch = new CountDownLatch(Constant.DATA_FILE_COUNT);
        for (int i = 0; i < Constant.DATA_FILE_COUNT; i++) {
            new InitDataThread(i, path, datas, downLatch).start();
        }
        downLatch.await();
        SortIndex.instance.sort();
        for (int i = 0; i < 100; i++) {
            System.out.println(SortIndex.instance.get(i));
        }
        return datas;
    }

    public static void closeDataFile(Data[] datas) throws IOException {
        for (Data data : datas) {
            data.close();
        }
    }
}

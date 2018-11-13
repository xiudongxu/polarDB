package com.alibabacloud.polar_race.engine.common;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-12
 */
public class InitDataThread extends Thread {

    private int fileNo;
    private String path;
    private Data[] datas;
    private CountDownLatch downLatch;

    public InitDataThread(int fileNo, String path, Data[] datas, CountDownLatch downLatch) {
        this.fileNo = fileNo;
        this.path = path;
        this.datas = datas;
        this.downLatch = downLatch;
    }

    @Override
    public void run() {
        try {
            datas[fileNo] = new Data(path, fileNo);
        } catch (IOException e) {
            System.out.println("init data file IO exception!!!");
        } finally {
            downLatch.countDown();
        }
    }
}

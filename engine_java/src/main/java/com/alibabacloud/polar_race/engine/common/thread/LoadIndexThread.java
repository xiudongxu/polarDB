package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SortIndex;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-25
 */
public class LoadIndexThread extends Thread {

    private Data data;
    private CountDownLatch downLatch;

    public LoadIndexThread(Data data, CountDownLatch downLatch) {
        this.data = data;
        this.downLatch = downLatch;
    }

    @Override
    public void run() {
        try {
            long[] keys = data.getMap().keys;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == 0) continue;
                SortIndex.instance.set(keys[i]);
            }
        } finally {
            downLatch.countDown();
        }
    }
}

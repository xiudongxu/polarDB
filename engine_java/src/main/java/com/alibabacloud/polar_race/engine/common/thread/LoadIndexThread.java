package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Data;
import com.alibabacloud.polar_race.engine.common.SmartSortIndex;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-25
 */
public class LoadIndexThread implements Runnable {

    private Data data;
    private CountDownLatch downLatch;

    public LoadIndexThread(Data data, CountDownLatch downLatch) {
        this.data = data;
        this.downLatch = downLatch;
    }

    @Override
    public void run() {
        try {
            int negativeCount = 0;
            long[] keys = data.getMap().keys;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == 0) continue;
                if (keys[i] < 0) negativeCount++;
                SmartSortIndex.instance.set(keys[i]);
            }
            SmartSortIndex.instance.negativeAdd(negativeCount);
        } finally {
            downLatch.countDown();
        }
    }
}

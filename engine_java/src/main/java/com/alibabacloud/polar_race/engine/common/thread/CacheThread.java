package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.cache.CachePool;

/**
 * @author wangshuo
 * @version 2018-11-24
 */
public class CacheThread extends Thread {

    private int threadNum;
    private CachePool cachePool;

    @Override
    public void run() {
        while (true) {
            //判断能不能继续缓存
            synchronized (cachePool) {
                if (Constant.CACHE_CAP - (cachePool.getLoadCursor() - cachePool.getReadCursor()) < 0) {
                    try {
                        cachePool.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }



        }
    }
}

package com.alibabacloud.polar_race.engine.common.thread;

import com.alibabacloud.polar_race.engine.common.Constant;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author dongxu.xiu
 * @since 2018-12-03 下午2:39
 */
public class UnifyFixedThreadPool {

    private ExecutorService executorService;

    private CountDownLatch countDownLatch;

    private UnifyFixedThreadPool() {
        executorService = Executors.newFixedThreadPool(Constant.THREAD_COUNT);
    }

    private static class UnifyThreadPoolHolder {
        private static UnifyFixedThreadPool INSTANCE = new UnifyFixedThreadPool();
    }

    public void submit(Runnable runnable) {
        executorService.submit(runnable);
    }

    public void initCountDownLatch(int count){
        countDownLatch = new CountDownLatch(count);
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void countDownAwait() {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void shutDown(){
        executorService.shutdown();
    }
    public static UnifyFixedThreadPool getInstance() {
        return UnifyThreadPoolHolder.INSTANCE;
    }
}

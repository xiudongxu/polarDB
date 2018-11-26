package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.ByteUtil;
import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * @author dongxu.xiu
 * @since 2018-10-17 下午2:24
 */
public class Test {

    private static EngineRace engineRace;

    public static void main(String[] args) throws IOException, EngineException {
        engineRace = new EngineRace();
        AbstractVisitor visitor = new VisitorImpl();
        engineRace.open("/Users/wangshuo/polarDb/store");

        CountDownLatch downLatch = new CountDownLatch(Constant.THREAD_COUNT);
        for (int i = 1; i <= Constant.THREAD_COUNT; i++) {
            new WriteData(i, downLatch).start();
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        downLatch = new CountDownLatch(Constant.THREAD_COUNT);
        for (int i = 1; i <= Constant.THREAD_COUNT; i++) {
            new ReadData(i, downLatch).start();
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        long begin = System.currentTimeMillis();
        downLatch = new CountDownLatch(Constant.THREAD_COUNT + 1);
        new RangeData(visitor, downLatch).start();

        /*try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        System.out.println("range data cost time:" + (System.currentTimeMillis() - begin));
    }

    static class RangeData extends Thread {

        private AbstractVisitor visitor;
        private CountDownLatch downLatch;

        public RangeData(AbstractVisitor visitor, CountDownLatch downLatch) {
            this.visitor = visitor;
            this.downLatch = downLatch;
        }

        @Override
        public void run() {
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                new RangeThread(engineRace, visitor, downLatch).start();
            }
            downLatch.countDown();
        }
    }

    static class WriteData extends Thread {

        private int threadNum;
        private CountDownLatch downLatch;

        public WriteData(int threadNum, CountDownLatch downLatch) {
            this.threadNum = threadNum;
            this.downLatch = downLatch;
        }

        @Override
        public void run() {
            try {
                long tmp = 1 + (this.threadNum - 1) * 100;
                for (int i = 0; i < 100; i++) {
                    byte[] key = ByteUtil.long2Bytes(tmp);
                    engineRace.write(key, makeValue(key));
                    tmp += 1;
                }
            } catch (EngineException e) {}
            finally {
                downLatch.countDown();
            }
        }
    }

    static class ReadData extends Thread {

        private int threadNum;
        private CountDownLatch downLatch;

        public ReadData(int threadNum, CountDownLatch downLatch) {
            this.threadNum = threadNum;
            this.downLatch = downLatch;
        }

        @Override
        public void run() {
            try {
                long tmp = 1 + (this.threadNum - 1) * 100;
                for (int i = 0; i < 100; i++) {
                    byte[] key = ByteUtil.long2Bytes(tmp);
                    byte[] value = engineRace.read(key);
                    byte[] prefixValue = new byte[8];
                    System.arraycopy(value, 0, prefixValue, 0, 8);
                    if (!Arrays.equals(key, prefixValue)) {
                        System.out.println("key:" + ByteUtil.bytes2Long(key) + " not match value");
                    }
                }
            } catch (EngineException e) {}
            finally {
                downLatch.countDown();
            }
        }
    }

    public static byte[] makeValue(byte[] key) {
        byte[] bytes = new byte[4096];
        for (int i = 0; i < bytes.length; i += key.length) {
            System.arraycopy(key, 0, bytes, i, key.length);
        }
        return bytes;
    }
}

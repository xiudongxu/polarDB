package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.ByteUtil;
import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.util.concurrent.CountDownLatch;

/**
 * @author dongxu.xiu
 * @since 2018-10-17 下午2:24
 */
public class Test {

    private static EngineRace engineRace;

    /*public static void main(String[] args) throws IOException, EngineException {
        engineRace = new EngineRace();
        AbstractVisitor visitor = new VisitorImpl();
        engineRace.open("/Users/wangshuo/polarDb/store");

        write(engineRace);

        long begin = System.currentTimeMillis();
        CountDownLatch downLatch = new CountDownLatch(Constant.THREAD_COUNT + 1);
        new RangeData(visitor, downLatch).start();
        try {
            downLatch.await();
            System.out.println("range count one finish");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("range count two start");
        downLatch = new CountDownLatch(Constant.THREAD_COUNT + 1);
        new RangeData(visitor, downLatch).start();
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("range data cost time:" + (System.currentTimeMillis() - begin));
    }*/

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

    public static void write(EngineRace engineRace) throws EngineException {
        long tmp = 1;
        for (int i = 0; i < Constant.TOTAL_KV_COUNT; i++) {
            byte[] key = ByteUtil.long2Bytes(tmp);
            engineRace.write(key, makeValue(key));
            tmp += 1;
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

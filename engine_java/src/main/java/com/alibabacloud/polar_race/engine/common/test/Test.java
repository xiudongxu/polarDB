package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.ByteUtil;
import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.io.IOException;

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

        //write(engineRace);

        long begin = System.currentTimeMillis();
        RangeData rangeData = new RangeData(visitor);
        rangeData.start();

        try {
            rangeData.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("cost time:" + (System.currentTimeMillis() - begin));
    }

    static class RangeData extends Thread {

        private AbstractVisitor visitor;

        public RangeData(AbstractVisitor visitor) {
            this.visitor = visitor;
        }

        @Override
        public void run() {
            for (int i = 0; i < Constant.THREAD_COUNT; i++) {
                new RangeThread(engineRace, visitor).start();
            }
        }
    }

    public static void write(EngineRace engineRace) throws EngineException {
        long tmp = 0;
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

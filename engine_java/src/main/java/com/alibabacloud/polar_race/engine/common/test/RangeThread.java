package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.ByteUtil;
import com.alibabacloud.polar_race.engine.common.Constant;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangshuo
 * @version 2018-11-22
 */
public class RangeThread extends Thread {

    private EngineRace engineRace;
    private AbstractVisitor visitor;
    private CountDownLatch downLatch;

    public RangeThread(EngineRace engineRace, AbstractVisitor visitor, CountDownLatch downLatch) {
        this.visitor = visitor;
        this.engineRace = engineRace;
        this.downLatch = downLatch;
    }

    @Override
    public void run() {
        try {
            long tmp = 1;
            byte[] lower = ByteUtil.long2Bytes(tmp);
            tmp += Constant.TOTAL_KV_COUNT - 1;
            byte[] upper = ByteUtil.long2Bytes(tmp);
            engineRace.range(lower, upper, visitor);
        } finally {
            downLatch.countDown();
        }
    }
}

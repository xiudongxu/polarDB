package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.EngineRace;

/**
 * @author wangshuo
 * @version 2018-11-22
 */
public class RangeThread extends Thread {

    private EngineRace engineRace;
    private AbstractVisitor visitor;

    public RangeThread(EngineRace engineRace, AbstractVisitor visitor) {
        this.visitor = visitor;
        this.engineRace = engineRace;
    }

    @Override
    public void run() {
        byte[] lower = Test.makeKey((byte) 'a');
        byte[] upper = Test.makeKey((byte) 'h');
        engineRace.range(lower, upper, visitor);
    }
}

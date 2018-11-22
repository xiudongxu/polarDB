package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
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

        for (int i = 0; i < Constant.THREAD_COUNT; i++) {
            new RangeThread(engineRace, visitor).start();
        }
    }

    public static void write(EngineRace engineRace) throws EngineException {
        engineRace.write(makeKey((byte) 'a'), makeValue((byte) 'a'));
        engineRace.write(makeKey((byte) 'b'), makeValue((byte) 'b'));
        engineRace.write(makeKey((byte) 'c'), makeValue((byte) 'c'));
        engineRace.write(makeKey((byte) 'd'), makeValue((byte) 'd'));

        engineRace.write(makeKey((byte) 'e'), makeValue((byte) 'e'));
        engineRace.write(makeKey((byte) 'f'), makeValue((byte) 'f'));
        engineRace.write(makeKey((byte) 'g'), makeValue((byte) 'g'));
        engineRace.write(makeKey((byte) 'h'), makeValue((byte) 'h'));
        /*byte[] read = engineRace.read(makeKey((byte) 'a'));
        System.out.println(Arrays.equals(read, makeValue((byte)'a')));
        read = engineRace.read(makeKey((byte) 'b'));
        System.out.println(Arrays.equals(read, makeValue((byte)'b')));
        read = engineRace.read(makeKey((byte) 'c'));
        System.out.println(Arrays.equals(read, makeValue((byte)'c')));
        read = engineRace.read(makeKey((byte) 'd'));
        System.out.println(Arrays.equals(read, makeValue((byte)'d')));*/
    }

    public static byte[] makeValue(byte b) {
        byte[] bytes = new byte[4096];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = b;
        }
        return bytes;
    }

    public static byte[] makeKey(byte b) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = b;
        }
        return bytes;
    }
}

package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author dongxu.xiu
 * @since 2018-10-17 下午2:24
 */
public class Test {

    private static EngineRace engineRace;

    public static void main(String[] args) throws IOException, EngineException {
        engineRace = new EngineRace();
        engineRace.open("/Users/wangshuo/polarDb/store/");
        //engineRace.write(ByteUtil.long2Bytes(298792), makeValue((byte) 'q'));
        byte[] read = engineRace.read(ByteUtil.long2Bytes(298792));
        System.out.println(Arrays.equals(read, makeValue((byte)'q')));
        engineRace.read(makeKey((byte) 'a'));

        /*for (Data data : engineRace.getDatas()) {
            System.out.println(data.getMap());
        }*/
        /*engineRace.write(makeKey((byte) '!'), makeValue((byte) '!'));
        engineRace.write(makeKey((byte) 'a'), makeValue((byte) 'a'));
        engineRace.write(makeKey((byte) 'b'), makeValue((byte) 'b'));
        engineRace.write(makeKey((byte) 'c'), makeValue((byte) 'c'));
        engineRace.write(makeKey((byte) 'd'), makeValue((byte) 'd'));
        byte[] read = engineRace.read(makeKey((byte) 'a'));
        System.out.println(Arrays.equals(read, makeValue((byte)'a')));
        read = engineRace.read(makeKey((byte) 'b'));
        System.out.println(Arrays.equals(read, makeValue((byte)'b')));
        read = engineRace.read(makeKey((byte) 'c'));
        System.out.println(Arrays.equals(read, makeValue((byte)'c')));
        read = engineRace.read(makeKey((byte) 'd'));
        System.out.println(Arrays.equals(read, makeValue((byte)'d')));*/

        /*byte[] read = engineRace.read(makeKey((byte) '!'));
        System.out.println(Arrays.equals(read, makeValue((byte)'!')));
        read = engineRace.read(makeKey((byte) 'x'));
        System.out.println(Arrays.equals(read, makeValue((byte)'x')));*/
    }

    public void testUnsafe() {
        //Unsafe.getUnsafe()
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

package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.io.IOException;
import java.nio.ByteBuffer;
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
        System.out.println(Arrays.equals(read, makeValue((byte)'d')));

        //testByte();

    }

    public static void testByte() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(ByteUtil.int2byte(4));

        /*buffer.put(new byte[]{1, 1, 1, 1});
        System.out.println(ByteUtil.byte2int(buffer.array()));
        buffer.flip();
        buffer.put((byte) 2);
        System.out.println(ByteUtil.byte2int(buffer.array()));*/
    }


    //测试步骤
    // 1.从0开始创建文件，读写正常
    // 2.继续写入文件

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

package com.alibabacloud.polar_race.engine.common;

import java.io.IOException;

/**
 * @author dongxu.xiu
 * @since 2018-10-17 下午2:24
 */
public class Test {

    private static EngineRace engineRace;

    public static void main(String[] args) throws IOException {


//        File file = new File("store" + "//"+"DOFFSET");
//        System.out.println(file.isFile());
//        System.out.println(file.getPath());
//        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
//        MappedFile mappedFile = new MappedFile(file,10);
//        MappedByteBuffer mappedByteBuffer = mappedFile.getMappedByteBuffer();
//        engineRace = new EngineRace(new Index(), new Data());
//        engineRace.open("//Users//xiudongxu//polarDb//store");
//        engineRace.write(makeKey((byte) 'a'), makeValue((byte) 'a'));
//        engineRace.write(makeKey((byte) 'b'), makeValue((byte) 'b'));
//        engineRace.write(makeKey((byte) 'c'), makeValue((byte) 'c'));
//        engineRace.write(makeKey((byte) 'd'), makeValue((byte) 'd'));
//        byte[] read = engineRace.read(makeKey((byte) 'a'));
//        System.out.println(Arrays.equals(read,makeValue((byte)'a')));
//        read = engineRace.read(makeKey((byte) 'b'));
//        System.out.println(Arrays.equals(read,makeValue((byte)'b')));
//        read = engineRace.read(makeKey((byte) 'c'));
//        System.out.println(Arrays.equals(read,makeValue((byte)'c')));
//        read = engineRace.read(makeKey((byte) 'd'));
//        System.out.println(Arrays.equals(read,makeValue((byte)'d')));

//        File file = new File("//Users//xiudongxu//polarDb//store//test.txt");



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

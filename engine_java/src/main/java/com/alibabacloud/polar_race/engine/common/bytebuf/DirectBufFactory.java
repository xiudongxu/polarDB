package com.alibabacloud.polar_race.engine.common.bytebuf;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

/**
 * @author wangshuo
 * @version 2018-12-06
 */
public class DirectBufFactory {

    public static ByteBuffer allocateAlign(int cap) {
        ByteBuffer byteBuffer = null;
        try {
            Class clazz = Class.forName("java.nio.DirectByteBuffer");
            Constructor constructor = clazz.getDeclaredConstructor(long.class, int.class, Object.class);
            constructor.setAccessible(true);
            byteBuffer = (ByteBuffer) constructor
                    .newInstance(DirectFileUtils.consAdrress(cap), cap, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return byteBuffer;
    }
}

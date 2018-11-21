package com.alibabacloud.polar_race.engine.common;

/**
 * @author wangshuo
 * @version 2018-11-15
 */
public class ThreadContext {

    private static ThreadLocal<byte[]> context = ThreadLocal
            .withInitial(() -> new byte[Constant.VALUE_SIZE]);

    public static byte[] getBytes(){
        return context.get();
    }
}

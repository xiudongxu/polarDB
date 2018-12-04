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

    private static ThreadLocal<Integer> readCursor = ThreadLocal.withInitial(() -> 0);

    public static int getReadCursor() {
        return readCursor.get();
    }

    public static void setReadCursor(int cursor) {
        readCursor.set(cursor);
    }
}

package com.alibabacloud.polar_race.engine.common;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

/**
 * @author wangshuo
 * @version 2018-11-15
 */
public class UnsafeUtil {
    private static Unsafe unsafe;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (Exception ex) { throw new Error(ex); }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }
    public static long getAddr(Object obj) {
        Object[] array = new Object[]{obj};
        long baseOffset = unsafe.arrayBaseOffset(Object[].class);
        return unsafe.getLong(array, baseOffset);
    }
}

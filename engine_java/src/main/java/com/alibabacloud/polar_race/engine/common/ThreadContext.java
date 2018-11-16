package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.nio.ByteBuffer;

/**
 * @author wangshuo
 * @version 2018-11-15
 */
public class ThreadContext {

    private static ThreadLocal<byte[]> context = ThreadLocal
            .withInitial(() -> new byte[Constant.VALUE_SIZE]);

    public static byte[] getBytes() throws EngineException {
        return context.get();
    }
}

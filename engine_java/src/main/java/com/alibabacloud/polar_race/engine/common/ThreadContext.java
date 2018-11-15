package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import java.nio.ByteBuffer;

/**
 * @author wangshuo
 * @version 2018-11-15
 */
public class ThreadContext {


    private static ThreadLocal<ByteBuffer> context = new ThreadLocal<>();

    public static void setBuffer() {
        ByteBuffer buffer = context.get();
        if (buffer == null) {
            context.set(ByteBuffer.allocateDirect(Constant.VALUE_SIZE));
        }
    }

    public static ByteBuffer getBuffer(String type) throws EngineException {
        return context.get();
    }
}

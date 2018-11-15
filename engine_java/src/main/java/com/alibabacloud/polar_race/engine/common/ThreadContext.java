package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangshuo
 * @version 2018-11-15
 */
public class ThreadContext {


    private static ThreadLocal<Map<String, ByteBuffer>> context = new ThreadLocal<>();

    public static void setBuffer() {
        Map<String, ByteBuffer> map = ThreadContext.context.get();
        if (map == null) {
            map = new HashMap<>();
            context.set(map);
            map.put(Constant.KEY, ByteBuffer.allocateDirect(Constant.KEY_SIZE));
            map.put(Constant.VALUE, ByteBuffer.allocateDirect(Constant.VALUE_SIZE));
        }
    }

    public static ByteBuffer getBuffer(String type) throws EngineException {
        Map<String, ByteBuffer> map = context.get();
        ByteBuffer buffer = map.get(type);
        if (buffer == null) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "not exist direct buffer");
        }
        return buffer;
    }
}

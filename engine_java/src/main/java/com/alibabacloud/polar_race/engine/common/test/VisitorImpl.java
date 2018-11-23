package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.ByteUtil;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongxu.xiu
 * @since 2018-11-21 下午8:10
 */
public class VisitorImpl extends AbstractVisitor {

    private AtomicInteger integer = new AtomicInteger(0);

    @Override
    public void visit(byte[] key, byte[] value) {
        byte[] prefixValue = new byte[8];
        System.arraycopy(value, 0, prefixValue, 0, 8);

        System.out.println("thread name:" + Thread.currentThread().getName() + " key:" + ByteUtil
                .bytes2Long(key) + " " + Arrays.equals(key, prefixValue) + " " + integer.getAndIncrement());
    }
}

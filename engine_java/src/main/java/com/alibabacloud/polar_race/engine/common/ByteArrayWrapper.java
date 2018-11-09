package com.alibabacloud.polar_race.engine.common;

import java.util.Arrays;

/**
 * @author dongxu.xiu
 * @since 2018-10-18 下午4:10
 */
public class ByteArrayWrapper {

    private final byte[] data;

    public ByteArrayWrapper(byte[] data)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.data = data;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayWrapper))
        {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper)other).data);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }

    public byte[] getData() {
        return data;
    }
}

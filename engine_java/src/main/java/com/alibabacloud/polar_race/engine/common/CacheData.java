package com.alibabacloud.polar_race.engine.common;

/**
 * @author dongxu.xiu
 * @since 2018-11-22 下午5:28
 */
public class CacheData {
    byte[] key;
    byte[] value;

    public CacheData(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
